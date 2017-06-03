package boltron

import (
	"bytes"
	"errors"
	"sync"

	"github.com/dc0d/boltron/boltwrapper"
)

//-----------------------------------------------------------------------------

// Errors
var (
	ErrIgnored = errors.New("ErrIgnored")
)

// KV == (key, value) tuple
type KV struct{ Key, Value []byte }

// Index if a document does not belong in an index, must return ErrIgnored
//
// is map part as in (CouchDB) map/reduce (sense)
type Index func(key []byte, value interface{}) ([]KV, error)

//-----------------------------------------------------------------------------

type Engine struct {
	db   *boltwrapper.DB
	repo map[string]Index
	m    sync.RWMutex
}

func NewEngine(db *boltwrapper.DB) *Engine {
	if db == nil {
		panic(boltwrapper.ErrNilDB)
	}
	res := &Engine{
		db:   db,
		repo: make(map[string]Index),
	}
	db.OnPut = res.onPut
	db.OnDelete = res.onDelete
	return res
}

func (eg *Engine) PutIndex(name string, m Index) {
	if m == nil {
		return
	}
	eg.m.Lock()
	defer eg.m.Unlock()
	eg.repo[name] = m
}

func (eg *Engine) DropIndex(tx *boltwrapper.Tx, name string) error {
	eg.m.Lock()
	defer eg.m.Unlock()
	err := tx.DeleteBucket([]byte(name))
	if err != nil {
		return err
	}
	delete(eg.repo, name)
	return nil
}

func (eg *Engine) onPut(tx *boltwrapper.Tx, key []byte, doc interface{}) error {
	eg.m.RLock()
	defer eg.m.RUnlock()
	var resErr Errors
	for mapKey, mapVal := range eg.repo {
		mapKey, mapVal := mapKey, mapVal
		bmap, err := tx.CreateBucketIfNotExists([]byte(mapKey))
		if err != nil {
			resErr = append(resErr, err)
			continue
		}
		kvList, err := mapVal(key, doc)
		if err == ErrIgnored {
			continue
		}
		if err != nil {
			resErr = append(resErr, err)
			continue
		}
		for _, kv := range kvList {
			kv := kv
			if len(kv.Key) == 0 {
				continue
			}
			gkey := append(IndexPrefix(), []byte(kv.Key)...)
			err = bmap.BoltBucket().Put(append(key, gkey...), gkey)
			if err != nil {
				resErr = append(resErr, err)
				continue
			}
			err = bmap.BoltBucket().Put(gkey, kv.Value)
			if err != nil {
				resErr = append(resErr, err)
				continue
			}
		}
	}
	return resErr
}

func (eg *Engine) onDelete(tx *boltwrapper.Tx, key []byte) error {
	eg.m.RLock()
	defer eg.m.RUnlock()
	var resErr Errors
	for mapKey := range eg.repo {
		mapKey := mapKey
		bmap, err := tx.CreateBucketIfNotExists([]byte(mapKey))
		if err != nil {
			resErr = append(resErr, err)
			continue
		}
		prefix := key
		c := bmap.Cursor()
		for k, v := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, v = c.Next() {
			err = bmap.BoltBucket().Delete(k)
			if err != nil {
				resErr = append(resErr, err)
			}
			err = bmap.BoltBucket().Delete(v)
			if err != nil {
				resErr = append(resErr, err)
			}
		}
	}
	return resErr
}
