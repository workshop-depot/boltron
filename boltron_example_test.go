package boltron_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	bolt "github.com/dc0d/boltron"
	"github.com/tidwall/gjson"
)

func ExampleDB_boltronIndexTimeJSON() {
	// Open the database.
	db, err := bolt.Open(tempfile(), 0666, nil)
	if err != nil {
		log.Fatal(err)
	}
	defer os.Remove(db.Path())
	defer db.Close()

	// sample struct that holds the data
	type data struct {
		ID    string    `json:"id,omitempty"`
		Name  string    `json:"name,omitempty"`
		Score float64   `json:"score,omitempty"`
		At    time.Time `json:"at,omitempty"`
	}

	err = db.AddIndex(bolt.NewIndex("times", func(k, v []byte) [][]byte {
		// this is a sample index that indexes the data if
		// the key starts with "data:" and
		// the value is a valid json and
		// it has a field named "at" containing a valid time.Time representation.
		// otherwise it will get ignored and not be indexed.

		// ignoring any key that has not a 'data:' prefix
		// by returning nil. this way, other kinds of data
		// would not be indexed by this index.
		if !bytes.HasPrefix(k, []byte("data:")) {
			return nil
		}

		// check if v is valid json
		js := string(v)
		if !gjson.Valid(js) {
			return nil
		}

		// getting the event time
		res := gjson.Get(string(v), "at")
		at := res.Time()
		if at.IsZero() {
			return nil
		}

		return [][]byte{[]byte(at.Format("20060102150405"))}
	}))
	if err != nil {
		log.Fatal(err)
	}

	err = db.Update(func(tx *bolt.Tx) error {
		// inserting a sample record
		d := data{
			ID:    "data:000010",
			Name:  "Kaveh",
			Score: 71,
			At:    time.Now(),
		}
		js, err := json.Marshal(&d)
		if err != nil {
			return err
		}
		bk, err := tx.CreateBucketIfNotExists([]byte("data"))
		if err != nil {
			return err
		}
		return bk.Put([]byte(d.ID), js)
	})
	if err != nil {
		log.Fatal(err)
	}

	var found *data
	err = db.View(func(tx *bolt.Tx) error {
		// searching for first records between yesterday (in current hour) and tomorrow
		indexBucket := tx.Bucket([]byte("times"))
		now := time.Now()
		yesterday := []byte(now.Add(time.Hour * 24 * -1).Format("20060102150405"))
		tomorrow := []byte(now.Add(time.Hour * 24).Format("20060102150405"))
		c := indexBucket.Cursor()
		prefix := yesterday
		var id []byte
		for k, v := c.Seek(prefix); k != nil && bytes.Compare(k, tomorrow) <= 0; k, v = c.Next() {
			id = v
			break
		}
		dataBucket := tx.Bucket([]byte("data"))
		v := dataBucket.Get(id)
		if len(v) == 0 {
			log.Fatal("not found")
		}
		found = new(data)
		return json.Unmarshal(v, found)
	})
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println(found.ID)

	// Output:
	// data:000010
}
