package pgsnap

import (
	"encoding/json"
	"os"
	"sync"
	"testing"

	"github.com/jackc/pgproto3/v2"
)

type Packet struct {
	ConnID int              `json:"connId"`
	Packet pgproto3.Message `json:"packet"`
}

type Disk struct {
	conns int
	f     *os.File
	enc   *json.Encoder
	mu    sync.Mutex
}

func openWriter(t *testing.T) *Disk {
	file := getFilename(t)
	f, err := os.Create(file)
	if err != nil {
		t.Fatal(err)
	}
	return &Disk{
		f:   f,
		enc: json.NewEncoder(f),
	}
}

func (d *Disk) Close() {
	d.f.Close()
}

// new connection gets a new channel. Close the returned channel when done.
func (d *Disk) newWriter() chan<- pgproto3.Message {
	d.mu.Lock()
	d.conns++
	connID := d.conns
	d.mu.Unlock()

	msgs := make(chan pgproto3.Message)
	go func() {
		for msg := range msgs {
			d.mu.Lock()
			d.enc.Encode(Packet{ConnID: connID, Packet: msg})
			d.mu.Unlock()
		}
	}()
	return msgs
}

//
func (d *Disk) newReader() <-chan pgproto3.Message {
}
