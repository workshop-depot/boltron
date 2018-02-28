package boltron

import (
	"bytes"
	"encoding/binary"
	"os"
	"sync"

	bolt "github.com/coreos/bbolt"
	"github.com/pkg/errors"
)

//-----------------------------------------------------------------------------

// Options represents the options that can be set when opening a database.
type Options = bolt.Options

// Stats represents statistics about the database.
type Stats = bolt.Stats

const (
	// MaxKeySize is the maximum length of a key, in bytes.
	MaxKeySize = bolt.MaxKeySize

	// MaxValueSize is the maximum length of a value, in bytes.
	MaxValueSize = bolt.MaxValueSize
)

//-----------------------------------------------------------------------------

// DB .
type DB struct {
	*bolt.DB
	indexes map[string]*Index
	m       sync.RWMutex
}

// Open creates and opens a database at the given path.
// If the file does not exist then it will be created automatically.
// Passing in nil options will cause Bolt to open the database with the default options.
func Open(path string, mode os.FileMode, options *Options) (*DB, error) {
	var opt *bolt.Options
	if options != nil {
		opt = options
	}
	_db, err := bolt.Open(path, mode, opt)
	if err != nil {
		return nil, err
	}
	db := &DB{
		DB:      _db,
		indexes: make(map[string]*Index),
	}
	return db, nil
}

// Batch .
func (db *DB) Batch(fn func(*Tx) error) error {
	return db.DB.Batch(func(tx *bolt.Tx) error {
		return fn(newTx(tx, db))
	})
}

// Begin .
func (db *DB) Begin(writable bool) (*Tx, error) {
	btx, err := db.DB.Begin(writable)
	if err != nil {
		return nil, err
	}
	return newTx(btx, db), err
}

// Update .
func (db *DB) Update(fn func(*Tx) error) error {
	return db.DB.Update(func(tx *bolt.Tx) error {
		return fn(newTx(tx, db))
	})
}

// View .
func (db *DB) View(fn func(*Tx) error) error {
	return db.DB.View(func(tx *bolt.Tx) error {
		return fn(newTx(tx, db))
	})
}

// AddIndex adds the indexs
func (db *DB) AddIndex(indexes ...*Index) error {
	func() {
		db.m.Lock()
		defer db.m.Unlock()
		for _, v := range indexes {
			db.indexes[v.name] = v
		}
	}()
	db.m.RLock()
	defer db.m.RUnlock()
	return db.Update(func(tx *Tx) error {
		for _, v := range db.indexes {
			if _, err := tx.CreateBucketIfNotExists([]byte(v.name)); err != nil {
				return err
			}
			if _, err := tx.CreateBucketIfNotExists([]byte(v.name + bkkeyssuffix)); err != nil {
				return err
			}
		}
		return nil
	})
}

// RebuildIndex .
func (db *DB) RebuildIndex(name string) error {
	var allBuckets []string
	err := db.DB.View(func(tx *bolt.Tx) error {
		return tx.ForEach(func(name []byte, b *bolt.Bucket) error {
			allBuckets = append(allBuckets, string(append([]byte{}, name...)))
			return nil
		})
	})
	if err != nil {
		return err
	}
ALL_BUCKETS:
	for _, vn := range allBuckets {
		for _, v := range db.indexes {
			if vn == v.name {
				continue ALL_BUCKETS
			}
		}
		// TODO: must get partitioned to some 100 records on each transaction
		err := db.Update(func(tx *Tx) error {
			orig := tx.Bucket([]byte(vn))
			if orig == nil {
				return nil
			}
			c := orig.Cursor()
			for k, v := c.First(); k != nil; k, v = c.Next() {
				k := append([]byte{}, k...)
				v := append([]byte{}, v...)
				if err := orig.upsertIndex(k, v); err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			return err
		}
	}
	return nil
}

//-----------------------------------------------------------------------------

// Tx .
type Tx struct {
	*bolt.Tx
	db *DB
}

func newTx(tx *bolt.Tx, db *DB) *Tx {
	return &Tx{Tx: tx, db: db}
}

// Bucket .
func (tx *Tx) Bucket(name []byte) *Bucket {
	bk := tx.Tx.Bucket(name)
	if bk == nil {
		return nil
	}
	return newBucket(bk, tx, string(name))
}

// CreateBucket .
func (tx *Tx) CreateBucket(name []byte) (*Bucket, error) {
	_bk, err := tx.Tx.CreateBucket(name)
	if err != nil {
		return nil, err
	}
	return newBucket(_bk, tx, string(name)), nil
}

// CreateBucketIfNotExists .
func (tx *Tx) CreateBucketIfNotExists(name []byte) (*Bucket, error) {
	_bk, err := tx.Tx.CreateBucketIfNotExists(name)
	if err != nil {
		return nil, err
	}
	return newBucket(_bk, tx, string(name)), nil
}

// DeleteBucket .
func (tx *Tx) DeleteBucket(name []byte) error {
	return tx.Tx.DeleteBucket(name)
}

// ForEach .
func (tx *Tx) ForEach(fn func(name []byte, b *Bucket) error) error {
	return tx.Tx.ForEach(func(name []byte, b *bolt.Bucket) error {
		return fn(name, newBucket(b, tx, ""))
	})
}

// DB .
func (tx *Tx) DB() *DB {
	return tx.db
}

//-----------------------------------------------------------------------------

// OrigBucket .
type OrigBucket = *bolt.Bucket

// Bucket .
type Bucket struct {
	OrigBucket
	tx   *Tx
	name string
}

func newBucket(bk *bolt.Bucket, tx *Tx, name string) *Bucket {
	res := &Bucket{OrigBucket: bk, tx: tx}
	if name != "" {
		res.name = name
	}
	return res
}

// Bucket .
func (bk *Bucket) Bucket(name []byte) *Bucket {
	_bk := bk.OrigBucket.Bucket(name)
	if _bk == nil {
		return nil
	}
	return newBucket(_bk, bk.tx, string(name))
}

// CreateBucket .
func (bk *Bucket) CreateBucket(key []byte) (*Bucket, error) {
	_bk, err := bk.OrigBucket.CreateBucket(key)
	if err != nil {
		return nil, err
	}
	return newBucket(_bk, bk.tx, string(key)), nil
}

// CreateBucketIfNotExists .
func (bk *Bucket) CreateBucketIfNotExists(key []byte) (*Bucket, error) {
	_bk, err := bk.OrigBucket.CreateBucketIfNotExists(key)
	if err != nil {
		return nil, err
	}
	return newBucket(_bk, bk.tx, string(key)), nil
}

// DeleteBucket .
func (bk *Bucket) DeleteBucket(key []byte) error {
	return bk.OrigBucket.DeleteBucket(key)
}

// Cursor .
func (bk *Bucket) Cursor() *Cursor {
	c := bk.OrigBucket.Cursor()
	if c == nil {
		return nil
	}
	return newCursor(bk, c)
}

// Delete .
func (bk *Bucket) Delete(key []byte) error {
	bk.tx.db.m.RLock()
	defer bk.tx.db.m.RUnlock()
	if bk.isIndex() {
		return ErrModifyIndex
	}
	for _, v := range bk.tx.db.indexes {
		indexBucket := bk.tx.Bucket([]byte(v.name))
		if indexBucket == nil {
			return errors.WithMessage(ErrIndexNotFound, v.name)
		}
		keysBucket := bk.tx.Bucket([]byte(v.name + bkkeyssuffix))
		if keysBucket == nil {
			return errors.WithMessage(ErrIndexNotFound, v.name)
		}

		// TODO: (ERR-A) error ignored (should it be checked? because of the possibility of double delete)

		// delete prev
		c := keysBucket.OrigBucket.Cursor()
		var (
			toDeleteKeys    [][]byte
			toDeleteIndexes [][]byte
		)
		for k, v := c.Seek(key); k != nil && bytes.HasPrefix(k, key); k, v = c.Next() {
			toDeleteKeys = append(toDeleteKeys, k)
			toDeleteIndexes = append(toDeleteIndexes, v)
		}
		for _, v := range toDeleteIndexes {
			indexBucket.OrigBucket.Delete(v) // TODO: (ERR-A)
		}
		for _, v := range toDeleteKeys {
			keysBucket.OrigBucket.Delete(v) // TODO: (ERR-A)
		}
	}
	return bk.OrigBucket.Delete(key)
}

// Put .
func (bk *Bucket) Put(key []byte, value []byte) error {
	bk.tx.db.m.RLock()
	defer bk.tx.db.m.RUnlock()
	if bk.isIndex() {
		return ErrModifyIndex
	}
	if err := bk.upsertIndex(key, value); err != nil {
		return err
	}
	return bk.OrigBucket.Put(key, value)
}

func (bk *Bucket) upsertIndex(key []byte, value []byte) error {
	for _, v := range bk.tx.db.indexes {
		emittedIndexes := v.selector(key, value)
		if len(emittedIndexes) == 0 {
			continue
		}
		var filtered [][]byte
		for _, vp := range emittedIndexes {
			if len(vp) == 0 {
				continue
			}
			filtered = append(filtered, vp)
		}
		emittedIndexes = filtered
		if len(emittedIndexes) == 0 {
			continue
		}
		indexBucket := bk.tx.Bucket([]byte(v.name))
		if indexBucket == nil {
			return errors.WithMessage(ErrIndexNotFound, v.name)
		}
		keysBucket := bk.tx.Bucket([]byte(v.name + bkkeyssuffix))
		if keysBucket == nil {
			return errors.WithMessage(ErrIndexNotFound, v.name)
		}
		for _, vix := range emittedIndexes {
			key2seg := bytes.Join([][]byte{key, vix}, []byte(Sep))
			seg2key := bytes.Join([][]byte{vix, key}, []byte(Sep))

			// delete prev
			c := keysBucket.OrigBucket.Cursor()
			var (
				toDeleteKeys    [][]byte
				toDeleteIndexes [][]byte
			)
			for k, v := c.Seek(key); k != nil && bytes.HasPrefix(k, key); k, v = c.Next() {
				toDeleteKeys = append(toDeleteKeys, k)
				toDeleteIndexes = append(toDeleteIndexes, v)
			}
			for _, v := range toDeleteIndexes {
				indexBucket.OrigBucket.Delete(v) // TODO: (ERR-A)
			}
			for _, v := range toDeleteKeys {
				keysBucket.OrigBucket.Delete(v) // TODO: (ERR-A)
			}

			if err := keysBucket.OrigBucket.Put(key2seg, seg2key); err != nil {
				return err
			}
			if err := indexBucket.OrigBucket.Put(seg2key, key); err != nil {
				return err
			}
		}
	}
	return nil
}

// constants
const (
	Sep = ":"

	bkkeyssuffix = "_keys"
)

// Tx .
func (bk *Bucket) Tx() *Tx { return bk.tx }

func (bk *Bucket) isIndex() bool {
	_, ok := bk.tx.db.indexes[bk.name]
	return ok
}

//-----------------------------------------------------------------------------

// OrigCursor .
type OrigCursor = *bolt.Cursor

// Cursor .
type Cursor struct {
	OrigCursor
	bucket *Bucket
}

func newCursor(bucket *Bucket, c *bolt.Cursor) *Cursor { return &Cursor{bucket: bucket, OrigCursor: c} }

// Bucket .
func (c *Cursor) Bucket() *Bucket {
	return c.bucket
}

//-----------------------------------------------------------------------------

// ToSlice v must be []byte, string, Name, Byter or a fixed width struct.
func ToSlice(v interface{}) ([]byte, error) {
	switch b := v.(type) {
	case []byte:
		return b, nil
	case string:
		return []byte(b), nil
	case Name:
		return []byte(b), nil
	case Byter:
		return b.Bytes(), nil
	}
	var buf bytes.Buffer
	if err := binary.Write(&buf, binary.BigEndian, v); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Byter .
type Byter interface {
	Bytes() []byte
}

//-----------------------------------------------------------------------------

// Name uses an string and provides []byte when needed.
// Storing []byte in a variable is problematic.
type Name string

// Bytes .
func (n Name) Bytes() []byte { return []byte(n) }

//-----------------------------------------------------------------------------

// KeyMaker .
type KeyMaker struct {
	key [][]byte
}

func (k *KeyMaker) Write(v interface{}) error {
	b, err := ToSlice(v)
	if err != nil {
		return err
	}
	k.key = append(k.key, b)
	return nil
}

// Bytes .
func (k KeyMaker) Bytes(sep []byte) []byte {
	return bytes.Join(k.key, sep)
}

//-----------------------------------------------------------------------------

// Index provide either gjson pattern(s) or selector (not both)
type Index struct {
	name     string
	selector func(k, v []byte) [][]byte
}

// NewIndex .
func NewIndex(name string, selector func(k, v []byte) [][]byte) *Index {
	if name == "" {
		panic("name must be provided")
	}
	return &Index{name: name, selector: selector}
}

//-----------------------------------------------------------------------------

// errors
var (
	ErrModifyIndex   error = sentinelErr("modifying indexes are not allowed")
	ErrIndexNotFound error = sentinelErr("index not found")
)

type sentinelErr string

func (v sentinelErr) Error() string { return string(v) }

//-----------------------------------------------------------------------------
