package boltron_example

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/dc0d/boltron"
	"github.com/dc0d/goroutines"
	"github.com/rs/xid"
)

var (
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
	db     *boltron.DB
)

func TestMain(m *testing.M) {
	defer goroutines.New().
		WaitGo(time.Second).
		Go(func() { wg.Wait() })
	ctx, cancel = context.WithCancel(context.Background())
	defer cancel()

	fdb := filepath.Join(os.TempDir(), "boltrondb-"+xid.New().String())
	var err error
	db, err = boltron.Open(fdb, 0774, nil)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer db.Close()

	m.Run()
}

func TestExample01(t *testing.T) {}
