package boltron

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/boltdb/bolt"
	"github.com/dc0d/goroutines"
	"github.com/rs/xid"
	"github.com/stretchr/testify/assert"
)

var (
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
	db     *bolt.DB
)

func TestMain(m *testing.M) {
	defer goroutines.New().
		WaitGo(time.Second).
		Go(func() { wg.Wait() })
	ctx, cancel = context.WithCancel(context.Background())
	defer cancel()

	fdb := filepath.Join(os.TempDir(), "boltrondb-"+xid.New().String())
	var err error
	db, err = bolt.Open(fdb, 0774, nil)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer db.Close()

	m.Run()
}

func TestErrors(t *testing.T) {
	var r Errors
	r = r.Append(nil)
	assert.Nil(t, r)
	err := fmt.Errorf("ERR")
	r = r.Append(err)
	assert.Contains(t, r, err)
	var _ error = Errors([]error{})
}

func TestMakeName(t *testing.T) {
	assert.Equal(t, "__books__", MakeName("books"))
	assert.Equal(t, "__books__", MakeName("______books__"))
}

type data struct {
	Name string `json:"name"`
	Age  int    `json:"age,string"`
}

func TestEngineDummy(t *testing.T) {
	vewName := "wrapped"
	e := NewEngine()
	e.PutMap(vewName, func(key []byte, value interface{}) []KV {
		var res []KV

		var val struct {
			Tag xid.ID
			Doc interface{}
		}
		val.Tag = xid.New()
		val.Doc = value

		newValue, _ := json.Marshal(val)
		newKey := append([]byte("JSONED:"), key...)
		kv := KV{
			Value: newValue,
			Key:   newKey,
		}
		res = append(res, kv)
		newKey = append(newKey, ':')
		newKey = append(newKey, []byte(fmt.Sprintf("%d", time.Now().Unix()))...)
		kv = KV{
			Key:   newKey,
			Value: newValue,
		}
		res = append(res, kv)

		return res
	})

	for i := 0; i < 6; i++ {
		i := i
		db.Update(func(tx *bolt.Tx) error {
			d := &data{
				Name: fmt.Sprintf("N%03d", i),
				Age:  i,
			}

			b, err := tx.CreateBucketIfNotExists([]byte("buk"))
			if err != nil {
				return err
			}

			// call engine in each Update
			e.Put(tx, []byte(d.Name), d)

			js, _ := json.Marshal(d)
			b.Put([]byte(d.Name), js)

			return nil
		})
	}

	for i := 0; i < 3; i++ {
		i := i
		db.Update(func(tx *bolt.Tx) error {
			d := &data{
				Name: fmt.Sprintf("N%03d", i),
				Age:  i,
			}

			b, err := tx.CreateBucketIfNotExists([]byte("buk"))
			if err != nil {
				return err
			}

			e.Delete(tx, []byte(d.Name), d)

			b.Delete([]byte(d.Name))

			return nil
		})
	}

	db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(MakeName(vewName)))
		b.ForEach(func(key []byte, value []byte) error {
			assert.True(t,
				bytes.HasPrefix(key, []byte("N003ツJSONED")) ||
					bytes.HasPrefix(key, []byte("N004ツJSONED")) ||
					bytes.HasPrefix(key, []byte("N005ツJSONED")) ||
					bytes.HasPrefix(key, []byte("ツJSONED:N003")) ||
					bytes.HasPrefix(key, []byte("ツJSONED:N004")) ||
					bytes.HasPrefix(key, []byte("ツJSONED:N005")))
			return nil
		})
		return nil
	})
}
