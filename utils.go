package boltron

import (
	"bytes"
	"strings"
	"sync"
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

// IndexPrefix prefix for secondary indexes in a view
func IndexPrefix() []byte { return []byte("ツ") }

//-----------------------------------------------------------------------------
