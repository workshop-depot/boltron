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
		opt = &options.Options
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

// Options represents the options that can be set when opening a database.
type Options struct {
	bolt.Options
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

// AddIndex TODO: build the index
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
	return newBucket(tx.Tx.Bucket(name), tx, string(name))
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

// ForEach .
func (tx *Tx) ForEach(fn func(name []byte, b *Bucket) error) error {
	return tx.Tx.ForEach(func(name []byte, b *bolt.Bucket) error {
		return fn(name, newBucket(b, tx, ""))
	})
}

//-----------------------------------------------------------------------------

// Bucket .
type Bucket struct {
	*bolt.Bucket
	tx   *Tx
	name string
}

func newBucket(bk *bolt.Bucket, tx *Tx, name string) *Bucket {
	res := &Bucket{Bucket: bk, tx: tx}
	if name != "" {
		res.name = name
	}
	return res
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
		c := indexBucket.Bucket.Cursor()
		var toDelete [][]byte
		for k, v := c.Seek(key); k != nil && bytes.HasPrefix(k, key); k, v = c.Next() {
			toDelete = append(toDelete, k, v)
		}
		for _, v := range toDelete {
			indexBucket.Bucket.Delete(v) // TODO: (ERR-A) error ignored (should it be checked? because of the possibility of double delete)
		}
	}
	return bk.Bucket.Delete(key)
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
	return bk.Bucket.Put(key, value)
}

func (bk *Bucket) upsertIndex(key []byte, value []byte) error {
	for _, v := range bk.tx.db.indexes {
		selectedSegments := v.selector(key, value)
		if len(selectedSegments) == 0 {
			continue
		}
		var filteredSegments [][]byte
		for _, vp := range selectedSegments {
			if len(vp) == 0 {
				continue
			}
			filteredSegments = append(filteredSegments, vp)
		}
		indexSegments := bytes.Join(filteredSegments, sep())
		key2seg := bytes.Join([][]byte{key, indexSegments}, sep())
		seg2key := bytes.Join([][]byte{indexSegments, key}, sep())
		indexBucket := bk.tx.Bucket([]byte(v.name))
		if indexBucket == nil {
			return errors.WithMessage(ErrIndexNotFound, v.name)
		}

		// delete prev
		c := indexBucket.Bucket.Cursor()
		var toDelete [][]byte
		for k, v := c.Seek(key); k != nil && bytes.HasPrefix(k, key); k, v = c.Next() {
			toDelete = append(toDelete, k, v)
		}
		for _, v := range toDelete {
			indexBucket.Bucket.Delete(v) // TODO: (ERR-A)
		}

		if err := indexBucket.Bucket.Put(key2seg, seg2key); err != nil {
			return err
		}
		if err := indexBucket.Bucket.Put(seg2key, key); err != nil {
			return err
		}
	}
	return nil
}

func sep() []byte {
	sep := ":"
	return []byte(sep)
}

// Tx .
func (bk *Bucket) Tx() *Tx { return bk.tx }

func (bk *Bucket) isIndex() bool {
	_, ok := bk.tx.db.indexes[bk.name]
	return ok
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
