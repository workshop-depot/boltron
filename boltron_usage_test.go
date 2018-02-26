package boltron_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/boltdb/bolt"
	"github.com/dc0d/boltron"
	"github.com/stretchr/testify/assert"
)

type data struct {
	ID    string    `json:"id,omitempty"`
	Name  string    `json:"name,omitempty"`
	Score float64   `json:"score,omitempty"`
	At    time.Time `json:"at,omitempty"`
}

var (
	db *boltron.DB
	at = time.Date(2018, 1, 1, 1, 1, 1, 1, time.Local)
)

func TestMain(m *testing.M) {
	fp := filepath.Join(os.TempDir(), "boltron_test.db")
	defer os.Remove(fp)

	_db, err := bolt.Open(fp, 0777, &bolt.Options{Timeout: time.Second, InitialMmapSize: 1024 * 1024})
	if err != nil {
		panic(err)
	}
	db = boltron.New(_db)
	defer db.Close()

	code := m.Run()
	os.Exit(code)
}

func TestUsage01(t *testing.T) {
	assert := assert.New(t)

	ix := boltron.NewIndex("names", func(k, v []byte) [][]byte {
		if !bytes.HasPrefix(k, []byte("data")) {
			return nil
		}
		var d data
		if err := json.Unmarshal(v, &d); err != nil {
			return nil
		}
		if d.At.IsZero() {
			return nil
		}
		return [][]byte{[]byte(d.At.Format("200601021504050700"))}
	})
	assert.NoError(db.AddIndex(ix))

	err := db.Update(func(tx *boltron.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte("scores"))
		return err
	})
	assert.NoError(err)

	err = db.Update(func(tx *boltron.Tx) error {
		bk := tx.Bucket([]byte("scores"))
		for i := 0; i < 3; i++ {
			i := i
			k := fmt.Sprintf("data:%020d", i)
			var d data
			d.ID = k
			d.Name = fmt.Sprintf("name:%d", i)
			d.Score = float64(i)
			d.At = at
			js, err := json.Marshal(&d)
			if err != nil {
				return err
			}
			if err := bk.Put([]byte(k), []byte(js)); err != nil {
				return err
			}
		}
		return nil
	})
	assert.NoError(err)

	err = db.View(func(tx *boltron.Tx) error {
		bk := tx.Bucket([]byte("scores"))
		c := bk.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			var d data
			assert.NoError(json.Unmarshal(v, &d))
			assert.Equal(at, d.At)
			assert.Condition(func() bool {
				return 0 <= d.Score && d.Score <= 2 && len(d.Name) > 0
			})
		}
		return nil
	})
	assert.NoError(err)

	err = db.View(func(tx *boltron.Tx) error {
		bk := tx.Bucket([]byte("names"))
		c := bk.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			switch string(k) {
			case "201801010101010700:data:00000000000000000000":
			case "201801010101010700:data:00000000000000000001":
			case "201801010101010700:data:00000000000000000002":
			case "data:00000000000000000000:201801010101010700":
			case "data:00000000000000000001:201801010101010700":
			case "data:00000000000000000002:201801010101010700":
			default:
				assert.Fail("wrong key")
			}
			switch string(v) {
			case "201801010101010700:data:00000000000000000000":
			case "201801010101010700:data:00000000000000000001":
			case "201801010101010700:data:00000000000000000002":
			case "data:00000000000000000000":
			case "data:00000000000000000001":
			case "data:00000000000000000002":
			default:
				assert.Fail("wrong key")
			}
		}
		return nil
	})
	assert.NoError(err)

	err = db.Update(func(tx *boltron.Tx) error {
		bk := tx.Bucket([]byte("scores"))
		for i := 0; i < 3; i++ {
			i := i
			k := fmt.Sprintf("data:%020d", i)
			if err := bk.Delete([]byte(k)); err != nil {
				return err
			}
		}
		return nil
	})
	assert.NoError(err)

	count := 0
	err = db.View(func(tx *boltron.Tx) error {
		bk := tx.Bucket([]byte("scores"))
		c := bk.Cursor()
		for k, _ := c.First(); k != nil; k, _ = c.Next() {
			count++
		}
		return nil
	})
	assert.NoError(err)
	assert.Equal(0, count)

	count = 0

	err = db.View(func(tx *boltron.Tx) error {
		bk := tx.Bucket([]byte("names"))
		c := bk.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			t.Log(string(k), string(v))
			count++
		}
		return nil
	})
	assert.NoError(err)
	assert.Equal(0, count)
}
