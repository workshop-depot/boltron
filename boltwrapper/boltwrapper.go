package boltwrapper

import (
	"errors"
	"io"
	"os"
	"time"

	"github.com/boltdb/bolt"
)

//-----------------------------------------------------------------------------

// Errors
var (
	ErrNilBucket    = errors.New("ErrNilBucket")
	ErrNilTx        = errors.New("ErrNilTx")
	ErrNilDB        = errors.New("ErrNilDB")
	ErrNilMarshaler = errors.New("ErrNilMarshaler")
)

//-----------------------------------------------------------------------------

type Bucket struct {
	buk *bolt.Bucket
	tx  *Tx
}

func NewBucket(tx *Tx, b *bolt.Bucket) *Bucket {
	if b == nil {
		panic(ErrNilBucket)
	}
	return &Bucket{
		tx:  tx,
		buk: b,
	}
}

func (b *Bucket) Bucket(name []byte) *Bucket {
	return NewBucket(b.tx, b.buk.Bucket(name))
}

func (b *Bucket) CreateBucket(key []byte) (*Bucket, error) {
	_b, err := b.buk.CreateBucket(key)
	if err != nil {
		return nil, err
	}
	return NewBucket(b.tx, _b), nil
}

func (b *Bucket) CreateBucketIfNotExists(key []byte) (*Bucket, error) {
	_b, err := b.buk.CreateBucketIfNotExists(key)
	if err != nil {
		return nil, err
	}
	return NewBucket(b.tx, _b), nil
}

func (b *Bucket) DeleteBucket(key []byte) error {
	return b.buk.DeleteBucket(key)
}

func (b *Bucket) ForEach(fn func(k, v []byte) error) error {
	return b.buk.ForEach(fn)
}

func (b *Bucket) Get(key []byte) []byte {
	return b.buk.Get(key)
}

func (b *Bucket) NextSequence() (uint64, error) {
	return b.buk.NextSequence()
}

func (b *Bucket) Root() uint64 {
	return uint64(b.buk.Root())
}

func (b *Bucket) Sequence() uint64 {
	return b.buk.Sequence()
}

func (b *Bucket) SetSequence(v uint64) error {
	return b.buk.SetSequence(v)
}

func (b *Bucket) Writable() bool {
	return b.buk.Writable()
}

func (b *Bucket) Tx() *Tx {
	return b.tx
}

func (b *Bucket) Cursor() *bolt.Cursor {
	return b.buk.Cursor()
}

func (b *Bucket) Stats() bolt.BucketStats {
	return b.buk.Stats()
}

func (b *Bucket) FillPercent(v ...float64) float64 {
	if len(v) > 0 {
		b.buk.FillPercent = v[0]
	}
	return b.buk.FillPercent
}

func (b *Bucket) Put(key []byte, doc interface{}) error {
	bdata, err := b.tx.db.marshaler(doc)
	if err != nil {
		return err
	}
	err = b.buk.Put(key, bdata)
	if err != nil {
		return err
	}
	if b.tx.db.OnPut != nil {
		err = b.tx.db.OnPut(b.tx, key, doc)
		if err != nil {
			return err
		}
	}
	return nil
}

func (b *Bucket) Delete(key []byte) error {
	err := b.buk.Delete(key)
	if err != nil {
		return err
	}
	if b.tx.db.OnDelete != nil {
		err = b.tx.db.OnDelete(b.tx, key)
		if err != nil {
			return err
		}
	}
	return nil
}

func (b *Bucket) BoltBucket() *bolt.Bucket {
	return b.buk
}

//-----------------------------------------------------------------------------

type Tx struct {
	tx *bolt.Tx
	db *DB
}

func NewTx(db *DB, tx *bolt.Tx) *Tx {
	if db == nil {
		panic(ErrNilDB)
	}
	if tx == nil {
		panic(ErrNilTx)
	}
	return &Tx{
		tx: tx,
		db: db,
	}
}

func (tx *Tx) Bucket(name []byte) *Bucket {
	return NewBucket(tx, tx.tx.Bucket(name))
}

func (tx *Tx) Check() <-chan error {
	return tx.tx.Check()
}

func (tx *Tx) Commit() error {
	return tx.tx.Commit()
}

func (tx *Tx) Copy(w io.Writer) error {
	return tx.tx.Copy(w)
}

func (tx *Tx) CopyFile(path string, mode os.FileMode) error {
	return tx.tx.CopyFile(path, mode)
}

func (tx *Tx) CreateBucket(name []byte) (*Bucket, error) {
	_b, err := tx.tx.CreateBucket(name)
	if err != nil {
		return nil, err
	}
	return NewBucket(tx, _b), nil
}

func (tx *Tx) CreateBucketIfNotExists(name []byte) (*Bucket, error) {
	_b, err := tx.tx.CreateBucketIfNotExists(name)
	if err != nil {
		return nil, err
	}
	return NewBucket(tx, _b), nil
}

func (tx *Tx) DB() *DB {
	return tx.db
}

func (tx *Tx) DeleteBucket(name []byte) error {
	return tx.tx.DeleteBucket(name)
}

func (tx *Tx) ForEach(fn func(name []byte, b *Bucket) error) error {
	return tx.tx.ForEach(func(name []byte, b *bolt.Bucket) error {
		return fn(name, NewBucket(tx, b))
	})
}

func (tx *Tx) ID() int {
	return tx.tx.ID()
}

func (tx *Tx) OnCommit(fn func()) {
	tx.tx.OnCommit(fn)
}

func (tx *Tx) Page(id int) (*bolt.PageInfo, error) {
	return tx.tx.Page(id)
}

func (tx *Tx) Rollback() error {
	return tx.tx.Rollback()
}

func (tx *Tx) Size() int64 {
	return tx.tx.Size()
}

func (tx *Tx) Writable() bool {
	return tx.tx.Writable()
}

func (tx *Tx) WriteTo(w io.Writer) (n int64, err error) {
	return tx.tx.WriteTo(w)
}

func (tx *Tx) Cursor() *bolt.Cursor {
	return tx.tx.Cursor()
}

func (tx *Tx) Stats() bolt.TxStats {
	return tx.tx.Stats()
}

func (tx *Tx) WriteFlag(v ...int) int {
	if len(v) > 0 {
		tx.tx.WriteFlag = v[0]
	}
	return tx.tx.WriteFlag
}

//-----------------------------------------------------------------------------

type DB struct {
	db        *bolt.DB
	marshaler func(v interface{}) (data []byte, err error)
	OnPut     func(*Tx, []byte, interface{}) error
	OnDelete  func(*Tx, []byte) error
}

func NewDB(db *bolt.DB, marshaler func(v interface{}) (data []byte, err error)) *DB {
	if db == nil {
		panic(ErrNilDB)
	}
	if marshaler == nil {
		panic(ErrNilMarshaler)
	}
	return &DB{
		db:        db,
		marshaler: marshaler,
	}
}

func Open(marshaler func(v interface{}) (data []byte, err error), path string, mode os.FileMode, options *bolt.Options) (*DB, error) {
	_db, err := bolt.Open(path, mode, options)
	if err != nil {
		return nil, err
	}
	return NewDB(_db, marshaler), nil
}

func (db *DB) Batch(fn func(*Tx) error) error {
	return db.db.Batch(func(tx *bolt.Tx) error {
		return fn(NewTx(db, tx))
	})
}

func (db *DB) Begin(writable bool) (*Tx, error) {
	_tx, err := db.db.Begin(writable)
	if err != nil {
		return nil, err
	}
	return NewTx(db, _tx), nil
}
func (db *DB) Close() error { return db.db.Close() }

func (db *DB) GoString() string {
	return db.db.GoString()
}

func (db *DB) Info() *bolt.Info {
	return db.db.Info()
}

func (db *DB) IsReadOnly() bool {
	return db.db.IsReadOnly()
}

func (db *DB) Path() string {
	return db.db.Path()
}

func (db *DB) Stats() bolt.Stats {
	return db.db.Stats()
}

func (db *DB) String() string {
	return db.db.String()
}

func (db *DB) Sync() error {
	return db.db.Sync()
}

func (db *DB) Update(fn func(*Tx) error) error {
	return db.db.Update(func(tx *bolt.Tx) error {
		return fn(NewTx(db, tx))
	})
}

func (db *DB) View(fn func(*Tx) error) error {
	return db.db.View(func(tx *bolt.Tx) error {
		return fn(NewTx(db, tx))
	})
}

func (db *DB) StrictMode(v ...bool) bool {
	if len(v) > 0 {
		db.db.StrictMode = v[0]
	}
	return db.db.StrictMode
}

func (db *DB) NoSync(v ...bool) bool {
	if len(v) > 0 {
		db.db.NoSync = v[0]
	}
	return db.db.NoSync
}

func (db *DB) NoGrowSync(v ...bool) bool {
	if len(v) > 0 {
		db.db.NoGrowSync = v[0]
	}
	return db.db.NoGrowSync
}

func (db *DB) MmapFlags(v ...int) int {
	if len(v) > 0 {
		db.db.MmapFlags = v[0]
	}
	return db.db.MmapFlags
}

func (db *DB) MaxBatchSize(v ...int) int {
	if len(v) > 0 {
		db.db.MaxBatchSize = v[0]
	}
	return db.db.MaxBatchSize
}

func (db *DB) AllocSize(v ...int) int {
	if len(v) > 0 {
		db.db.AllocSize = v[0]
	}
	return db.db.AllocSize
}

func (db *DB) MaxBatchDelay(v ...time.Duration) time.Duration {
	if len(v) > 0 {
		db.db.MaxBatchDelay = v[0]
	}
	return db.db.MaxBatchDelay
}

//-----------------------------------------------------------------------------
