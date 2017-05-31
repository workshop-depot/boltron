package boltron

import (
	"bytes"
	"fmt"
	"strings"
	"sync"

	"github.com/boltdb/bolt"
)

//-----------------------------------------------------------------------------

var bufferPool = sync.Pool{
	New: func() interface{} {
		return &bytes.Buffer{}
	},
}

func getBuffer() *bytes.Buffer {
	buff := bufferPool.Get().(*bytes.Buffer)
	buff.Reset()
	return buff
}

func putBuffer(buff *bytes.Buffer) {
	bufferPool.Put(buff)
}

// Errors .
type Errors []error

func (x Errors) String() string {
	return x.Error()
}

func (x Errors) Error() string {
	if x == nil {
		return ``
	}

	buff := getBuffer()

	for _, ve := range x {
		if ve == nil {
			continue
		}
		buff.WriteString(` [` + ve.Error() + `]`)
	}
	res := strings.TrimSpace(buff.String())

	putBuffer(buff)

	return res
}

// Append .
func (x Errors) Append(err error) Errors {
	if err == nil {
		return x
	}
	return append(x, err)
}

//-----------------------------------------------------------------------------

// MakeName converts mapname to __mapname__
func MakeName(name string) string {
	return fmt.Sprintf("__%s__", strings.Trim(name, "_"))
}

// KV == (key, value) tuple
type KV struct{ Key, Value []byte }

// Map part as in (CouchDB) map/reduce (sense)
type Map func(key []byte, value interface{}) []KV

// Engine .
type Engine struct {
	repo map[string]Map
	m    sync.RWMutex
}

// NewEngine .
func NewEngine() *Engine {
	return &Engine{repo: make(map[string]Map)}
}

// GetMap get a Map func
func (e *Engine) GetMap(name string) Map {
	e.m.RLock()
	defer e.m.RUnlock()
	res, ok := e.repo[MakeName(name)]
	if ok {
		return res
	}
	return nil
}

// PutMap .
func (e *Engine) PutMap(name string, m Map) {
	if m == nil {
		return
	}
	e.m.Lock()
	defer e.m.Unlock()
	e.repo[MakeName(name)] = m
}

// DropMap .
func (e *Engine) DropMap(tx *bolt.Tx, name string) error {
	e.m.Lock()
	defer e.m.Unlock()
	k := MakeName(name)
	err := tx.DeleteBucket([]byte(k))
	if err != nil {
		return err
	}
	delete(e.repo, k)
	return nil
}

// MapPrefix prefix for secondary indexes in a map(ped view)
func MapPrefix() []byte { return []byte("ツ") }

// Put (key, value) tuple in maps
func (e *Engine) Put(tx *bolt.Tx, key []byte, value interface{}) error {
	e.m.RLock()
	defer e.m.RUnlock()
	var resErr Errors
	for mapKey, mapVal := range e.repo {
		mapKey, mapVal := mapKey, mapVal
		b, err := tx.CreateBucketIfNotExists([]byte(mapKey))
		if err != nil {
			resErr = append(resErr, err)
			continue
		}
		for _, kv := range mapVal(key, value) {
			kv := kv
			if len(kv.Key) == 0 {
				continue
			}
			gkey := append(MapPrefix(), []byte(kv.Key)...)
			err = b.Put(append(key, gkey...), gkey)
			if err != nil {
				resErr = append(resErr, err)
				continue
			}
			err = b.Put(gkey, kv.Value)
			if err != nil {
				resErr = append(resErr, err)
				continue
			}

			// NEXT_SEQ: (a seq prefix is needed too)
			// 	seq, err := b.NextSequence()
			// 	if err != nil {
			// 		resErr = append(resErr, err)
			// 		continue
			// 	}
			// 	if seq == 0 {
			// 		goto NEXT_SEQ
			// 	}
			// 	buf := new(bytes.Buffer)
			// 	err = binary.Write(buf, binary.BigEndian, seq)
			// 	if err != nil {
			// 		resErr = append(resErr, err)
			// 		continue
			// 	}
			// 	var seqKey = append(key, []byte("SEQPFX")...)
			// 	err = b.Put(append(seqKey, buf.Bytes()...), gkey)
			// 	if err != nil {
			// 		resErr = append(resErr, err)
			// 		continue
			// 	}
		}
	}
	return resErr
}

// Delete (key, value) tuple from maps
func (e *Engine) Delete(tx *bolt.Tx, key []byte, value interface{}) error {
	e.m.RLock()
	defer e.m.RUnlock()
	var resErr Errors
	for mapKey := range e.repo {
		mapKey := mapKey
		b, err := tx.CreateBucketIfNotExists([]byte(mapKey))
		if err != nil {
			resErr = append(resErr, err)
			continue
		}
		prefix := key
		c := b.Cursor()
		for k, v := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, v = c.Next() {
			err = b.Delete(k)
			if err != nil {
				resErr = append(resErr, err)
			}
			err = b.Delete(v)
			if err != nil {
				resErr = append(resErr, err)
			}
		}
	}
	return resErr
}
