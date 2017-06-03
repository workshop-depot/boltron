package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/comail/colog"
	"github.com/dc0d/boltron"
	"github.com/dc0d/boltron/boltwrapper"
	"github.com/dc0d/goroutines"
	"github.com/rs/xid"
	"gitlab.com/dc0d/tune"
)

var (
	ctx    context.Context
	cancel context.CancelFunc
	db     *boltwrapper.DB
	eg     *boltron.Engine

	wg = &sync.WaitGroup{}
)

func init() {
	colog.Register()
	colog.SetFlags(log.Lshortfile)
}

func main() {
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

	go func() {
		log.Println(http.ListenAndServe("localhost:8080", nil))
	}()

	go func() {
		showStat := false
		var delay = time.Second
		if showStat {
			delay = time.Second * 3
		}

		ticks := time.NewTicker(delay)
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticks.C:
				if showStat {
					st := db.Stats()
					fmt.Println(fmt.Sprintf("%+v", st))
				} else {
					fmt.Print(".")
				}
			}
		}
	}()

	tune.CallOnSignal(func() {
		cancel()
	})
	done := make(chan struct{})
	go lab(done)
	select {
	case <-ctx.Done():
		// case <-done:
	}
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

func lab(done chan struct{}) {
	defer close(done)
	recCount := 100000
	defer tune.TimerScope("", recCount)()

	viewName := "view1"
	eg := boltron.NewEngine(db)
	eg.PutIndex(viewName, func(key []byte, value interface{}) ([]boltron.KV, error) {
		var res []boltron.KV

		var val struct {
			Tag xid.ID
			Doc interface{}
		}
		val.Tag = xid.New()
		val.Doc = value

		newValue, _ := json.Marshal(val)
		newKey := append([]byte("JSONED:"), key...)
		kv := boltron.KV{
			Value: newValue,
			Key:   newKey,
		}
		res = append(res, kv)
		newKey = append(newKey, ':')
		newKey = append(newKey, []byte(fmt.Sprintf("%d", time.Now().Unix()))...)
		kv = boltron.KV{
			Key:   newKey,
			Value: newValue,
		}
		res = append(res, kv)

		return res, nil
	})

	dataStream := make(chan *data, 100)
	goroutines.New().
		WaitGroup(wg).
		Go(func() {
			defer close(dataStream)
			for i := 0; i < recCount; i++ {
				d := &data{
					Name: fmt.Sprintf("N%012d", i),
					Age:  i,
				}
				dataStream <- d
			}
		})

	batchSize := 3000

	var b []*data
	for nextData := range dataStream {
		b = append(b, nextData)
		if len(b) >= batchSize {
			buf := b
			b = nil
			err := db.Update(func(tx *boltwrapper.Tx) error {
				b, err := tx.CreateBucketIfNotExists([]byte("buk"))
				if err != nil {
					return err
				}

				for _, v := range buf {
					b.Put([]byte(v.Name), v)
				}

				return nil
			})
			if err != nil {
				fmt.Println(err)
			}
		}
	}
	if len(b) > 0 {
		buf := b
		b = nil
		err := db.Update(func(tx *boltwrapper.Tx) error {
			b, err := tx.CreateBucketIfNotExists([]byte("buk"))
			if err != nil {
				return err
			}

			for _, v := range buf {
				b.Put([]byte(v.Name), v)
			}

			return nil
		})
		if err != nil {
			fmt.Println(err)
		}
	}

	// q := make(chan int, 100)
	// for i := 0; i < recCount; i++ {
	// 	i := i
	// 	goroutines.New().
	// 		WaitGroup(wg).
	// 		Go(func() {
	// 			defer func() {
	// 				<-q
	// 			}()
	// 			q <- i
	// 			err := db.Update(func(tx *boltwrapper.Tx) error {
	// 				d := &data{
	// 					Name: fmt.Sprintf("N%012d", i),
	// 					Age:  i,
	// 				}

	// 				b, err := tx.CreateBucketIfNotExists([]byte("buk"))
	// 				if err != nil {
	// 					return err
	// 				}

	// 				b.Put([]byte(d.Name), d)

	// 				return nil
	// 			})
	// 			if err != nil {
	// 				fmt.Println(err)
	// 			}
	// 		})
	// }
}
