package pgsnap

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"os"
	"sync"

	"github.com/jackc/pgproto3/v2"

	"github.com/alicebob/pgsnap/pgmock"
)

type RWriter struct {
	buf *bytes.Buffer
	enc *json.Encoder
	mu  sync.Mutex
}

// A ReplayWriter writes packets to a buffer.
func NewReplayWriter() *RWriter {
	b := &bytes.Buffer{}
	enc := json.NewEncoder(b)
	return &RWriter{
		buf: b,
		enc: enc,
	}
}

func (rw *RWriter) Add(p pgproto3.Message) {
	rw.mu.Lock()
	defer rw.mu.Unlock()

	// pgx has a habit of reusing structs, so we have to encode now.
	rw.enc.Encode(p)
}

func (rw *RWriter) WriteFile(name string) error {
	rw.mu.Lock()
	defer rw.mu.Unlock()

	return os.WriteFile(name, rw.buf.Bytes(), 0600)
}

// given two replays, say if these are "functional" the same thing.
// Used to avoid rewriting a file if only "local" things changed (OIDs)
func (rw *RWriter) Equivalent(r2 *Replay) bool {
	// TODO
	return false
}

type Replay struct {
	packets []pgproto3.Message
	mu      sync.Mutex
}

// Read stored replay. Will return <some error> if the file doesn't exists.
func ReadReplay(name string) (*Replay, error) {
	f, err := os.Open(name)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	r := &Replay{}
	dec := json.NewDecoder(f)
	for {
		var raw json.RawMessage
		if err := dec.Decode(&raw); err != nil {
			if errors.Is(err, io.EOF) {
				return r, nil
			}
			return r, err
		}
		p, err := unmarshal(raw)
		if err != nil {
			return r, err
		}
		r.packets = append(r.packets, p)
	}
}

func (r *Replay) AsMock() *pgmock.Script {
	r.mu.Lock()
	defer r.mu.Unlock()

	s := pgmock.NewScript()
	for _, p := range r.packets {
		s.Append(p)
	}
	return s
}

func unmarshal(src []byte) (pgproto3.Message, error) {
	t := struct {
		Type string
	}{}
	if err := json.Unmarshal(src, &t); err != nil {
		return nil, err
	}

	var o pgproto3.Message

	switch t.Type {
	// case "AuthenticationOK":
	// o = &pgproto3.AuthenticationOk{}
	// case "BackendKeyData":
	// o = &pgproto3.BackendKeyData{}
	case "ParseComplete":
		o = &pgproto3.ParseComplete{}
	case "ParameterDescription":
		o = &pgproto3.ParameterDescription{}
	case "RowDescription":
		o = &pgproto3.RowDescription{}
	case "ReadyForQuery":
		o = &pgproto3.ReadyForQuery{}
	case "BindComplete":
		o = &pgproto3.BindComplete{}
	case "DataRow":
		o = &pgproto3.DataRow{}
	case "CommandComplete":
		o = &pgproto3.CommandComplete{}
	case "EmptyQueryResponse":
		o = &pgproto3.EmptyQueryResponse{}
	case "NoData":
		o = &pgproto3.NoData{}
	// case "StartupMessage":
	// o = &pgproto3.StartupMessage{}
	case "Parse":
		o = &pgproto3.Parse{}
	case "Query":
		o = &pgproto3.Query{}
	case "Describe":
		o = &pgproto3.Describe{}
	case "Sync":
		o = &pgproto3.Sync{}
	case "Bind":
		o = &pgproto3.Bind{}
	case "Execute":
		o = &pgproto3.Execute{}
	case "Terminate":
		o = &pgproto3.Terminate{}
	default:
		panic("unknown type: " + t.Type)
	}

	err := json.Unmarshal(src, o)
	return o, err
}
