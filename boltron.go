package boltron

import (
	"bytes"
	"encoding/binary"
	"os"
	"sync"

	"github.com/boltdb/bolt"
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
	panic("N/A")
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
		ixbk := bk.tx.Bucket([]byte(v.name))
		if ixbk == nil {
			return errors.WithMessage(ErrIndexNotFound, v.name)
		}
		c := ixbk.Bucket.Cursor()
		var toDelete [][]byte
		for k, v := c.Seek(key); k != nil && bytes.HasPrefix(k, key); k, v = c.Next() {
			toDelete = append(toDelete, k, v)
		}
		for _, v := range toDelete {
			ixbk.Bucket.Delete(v)
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
	for _, v := range bk.tx.db.indexes {
		parts := v.selector(key, value)
		if len(parts) == 0 {
			continue
		}
		var km [][]byte
		for _, vp := range parts {
			if len(vp) == 0 {
				continue
			}
			km = append(km, vp)
		}
		ixpart := bytes.Join(km, []byte(":"))
		key2part := bytes.Join([][]byte{key, ixpart}, []byte(":"))
		part2key := bytes.Join([][]byte{ixpart, key}, []byte(":"))
		ixbk := bk.tx.Bucket([]byte(v.name))
		if ixbk == nil {
			return errors.WithMessage(ErrIndexNotFound, v.name)
		}

		// delete prev
		c := ixbk.Bucket.Cursor()
		var toDelete [][]byte
		for k, v := c.Seek(key); k != nil && bytes.HasPrefix(k, key); k, v = c.Next() {
			toDelete = append(toDelete, k, v)
		}
		for _, v := range toDelete {
			ixbk.Bucket.Delete(v)
		}

		if err := ixbk.Bucket.Put(key2part, part2key); err != nil {
			return err
		}
		if err := ixbk.Bucket.Put(part2key, key); err != nil {
			return err
		}
	}
	return bk.Bucket.Put(key, value)
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
