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

	"github.com/dc0d/boltron/boltwrapper"
	"github.com/dc0d/goroutines"
	"github.com/rs/xid"
	"github.com/stretchr/testify/assert"
)

var (
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
	db     *boltwrapper.DB
)

func TestMain(m *testing.M) {
	defer goroutines.New().
		WaitGo(time.Second).
		Go(func() { wg.Wait() })
	ctx, cancel = context.WithCancel(context.Background())
	defer cancel()

	fdb := filepath.Join(os.TempDir(), "boltrondb-"+xid.New().String())
	var err error
	db, err = boltwrapper.Open(marshaler, fdb, 0774, nil)
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

func marshaler(v interface{}) (data []byte, err error) {
	switch x := v.(type) {
	case []byte:
		return x, nil
	}
	return json.Marshal(v)
}

type data struct {
	Name string `json:"name"`
	Age  int    `json:"age,string"`
}

func TestEngineDummy(t *testing.T) {
	viewName := "view1"
	eg := NewEngine(db)
	eg.PutIndex(viewName, func(key []byte, value interface{}) ([]KV, error) {
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

		return res, nil
	})

	for i := 0; i < 6; i++ {
		i := i
		err := db.Update(func(tx *boltwrapper.Tx) error {
			d := &data{
				Name: fmt.Sprintf("N%03d", i),
				Age:  i,
			}

			b, err := tx.CreateBucketIfNotExists([]byte("buk"))
			if err != nil {
				return err
			}

			b.Put([]byte(d.Name), d)

			return nil
		})
		assert.Nil(t, err)
	}

	for i := 0; i < 3; i++ {
		i := i
		err := db.Update(func(tx *boltwrapper.Tx) error {
			d := &data{
				Name: fmt.Sprintf("N%03d", i),
				Age:  i,
			}

			b, err := tx.CreateBucketIfNotExists([]byte("buk"))
			if err != nil {
				return err
			}

			// b.DeleteDoc([]byte(d.Name))
			b.Delete([]byte(d.Name))

			return nil
		})
		assert.Nil(t, err)
	}

	verr := db.View(func(tx *boltwrapper.Tx) error {
		b := tx.Bucket([]byte(viewName))
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
	assert.Nil(t, verr)
}

func TestEngineDummy1(t *testing.T) {
	viewName := "view1"
	eg := NewEngine(db)
	eg.PutIndex(viewName, func(key []byte, value interface{}) ([]KV, error) {
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

		return res, nil
	})

	defer goroutines.New().
		WaitGo(time.Second * 90).
		Go(func() { wg.Wait() })

	q := make(chan int, 66)
	for i := 0; i < 600; i++ {
		i := i
		q <- i
		goroutines.New().
			WaitGroup(&wg).
			Go(func() {
				defer func() {
					<-q
				}()
				err := db.Update(func(tx *boltwrapper.Tx) error {
					d := &data{
						Name: fmt.Sprintf("N%03d", i),
						Age:  i,
					}

					b, err := tx.CreateBucketIfNotExists([]byte("buk"))
					if err != nil {
						return err
					}

					b.Put([]byte(d.Name), d)

					return nil
				})
				assert.Nil(t, err)
			})
	}
}
