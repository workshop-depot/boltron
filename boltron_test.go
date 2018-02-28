package boltron

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestDropIndex(t *testing.T) {
	assert := assert.New(t)

	db, err := Open(tempfile(), 0666, nil)
	assert.NoError(err)
	defer os.Remove(db.Path())
	defer db.Close()

	ix := NewIndex("names", func(k, v []byte) [][]byte {
		return nil
	})
	assert.NoError(db.AddIndex(ix))

	err = db.View(func(tx *Tx) error {
		bk := tx.Bucket([]byte("names"))
		assert.NotNil(bk)
		return nil
	})
	assert.NoError(err)

	assert.NoError(db.DropIndex("names"))

	err = db.View(func(tx *Tx) error {
		bk := tx.Bucket([]byte("names"))
		assert.Nil(bk)
		return nil
	})
	assert.NoError(err)

	assert.Len(db.indexes, 0)
}

// tempfile returns a temporary file path.
func tempfile() string {
	f, err := ioutil.TempFile("", "bolt-")
	if err != nil {
		panic(err)
	}
	if err := f.Close(); err != nil {
		panic(err)
	}
	if err := os.Remove(f.Name()); err != nil {
		panic(err)
	}
	return f.Name()
}

type data struct {
	ID    string    `json:"id,omitempty"`
	Name  string    `json:"name,omitempty"`
	Score float64   `json:"score,omitempty"`
	At    time.Time `json:"at,omitempty"`
}

var (
	at = time.Date(2018, 1, 1, 1, 1, 1, 1, time.Local)
)

func Test01(t *testing.T) {
	assert := assert.New(t)

	fp := filepath.Join(os.TempDir(), "boltron_test.db")
	defer os.Remove(fp)

	opt := &Options{}
	opt.Timeout = time.Second
	opt.InitialMmapSize = 1024 * 1024
	_db, err := Open(fp, 0777, opt)
	if err != nil {
		panic(err)
	}
	db := _db
	defer db.Close()

	ix := NewIndex("names", func(k, v []byte) [][]byte {
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

	err = db.Update(func(tx *Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte("scores"))
		return err
	})
	assert.NoError(err)

	err = db.Update(func(tx *Tx) error {
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

	err = db.View(func(tx *Tx) error {
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

	err = db.View(func(tx *Tx) error {
		bk := tx.Bucket([]byte("names"))
		c := bk.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			switch string(k) {
			case "201801010101010700:data:00000000000000000000":
			case "201801010101010700:data:00000000000000000001":
			case "201801010101010700:data:00000000000000000002":
			// case "data:00000000000000000000:201801010101010700":
			// case "data:00000000000000000001:201801010101010700":
			// case "data:00000000000000000002:201801010101010700":
			default:
				assert.Fail("wrong k " + string(k))
			}
			switch string(v) {
			// case "201801010101010700:data:00000000000000000000":
			// case "201801010101010700:data:00000000000000000001":
			// case "201801010101010700:data:00000000000000000002":
			case "data:00000000000000000000":
			case "data:00000000000000000001":
			case "data:00000000000000000002":
			default:
				assert.Fail("wrong v " + string(v))
			}
		}
		return nil
	})
	assert.NoError(err)

	err = db.View(func(tx *Tx) error {
		bk := tx.Bucket([]byte("names" + bkkeyssuffix))
		c := bk.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			switch string(k) {
			// case "201801010101010700:data:00000000000000000000":
			// case "201801010101010700:data:00000000000000000001":
			// case "201801010101010700:data:00000000000000000002":
			case "data:00000000000000000000:201801010101010700":
			case "data:00000000000000000001:201801010101010700":
			case "data:00000000000000000002:201801010101010700":
			default:
				assert.Fail("wrong k " + string(k))
			}
			switch string(v) {
			case "201801010101010700:data:00000000000000000000":
			case "201801010101010700:data:00000000000000000001":
			case "201801010101010700:data:00000000000000000002":
			// case "data:00000000000000000000":
			// case "data:00000000000000000001":
			// case "data:00000000000000000002":
			default:
				assert.Fail("wrong v " + string(v))
			}
		}
		return nil
	})
	assert.NoError(err)

	err = db.Update(func(tx *Tx) error {
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
	err = db.View(func(tx *Tx) error {
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

	err = db.View(func(tx *Tx) error {
		bk := tx.Bucket([]byte("names"))
		c := bk.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			_, _ = k, v
			// t.Log(string(k), string(v))
			count++
		}
		return nil
	})
	assert.NoError(err)
	assert.Equal(0, count)

	count = 0

	err = db.View(func(tx *Tx) error {
		bk := tx.Bucket([]byte("names" + bkkeyssuffix))
		c := bk.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			_, _ = k, v
			// t.Log(string(k), string(v))
			count++
		}
		return nil
	})
	assert.NoError(err)
	assert.Equal(0, count)
}

func Test02(t *testing.T) {
	assert := assert.New(t)

	fp := filepath.Join(os.TempDir(), "boltron_test.db")
	defer os.Remove(fp)

	opt := &Options{}
	opt.Timeout = time.Second
	opt.InitialMmapSize = 1024 * 1024
	_db, err := Open(fp, 0777, opt)
	if err != nil {
		panic(err)
	}
	db := _db
	defer db.Close()

	ix := NewIndex("names", func(k, v []byte) [][]byte {
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

	err = db.Update(func(tx *Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte("scores"))
		return err
	})
	assert.NoError(err)

	err = db.Update(func(tx *Tx) error {
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

	assert.NoError(db.RebuildIndex("names"))

	err = db.View(func(tx *Tx) error {
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

	err = db.View(func(tx *Tx) error {
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

	err = db.Update(func(tx *Tx) error {
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
	err = db.View(func(tx *Tx) error {
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

	err = db.View(func(tx *Tx) error {
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
