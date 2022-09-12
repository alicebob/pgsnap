package pgsnap

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"strings"
	"testing"
	"time"
	"unicode"

	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
)

type Snap struct {
	t       *testing.T
	addr    string
	errchan chan error
	cancel  func()
	done    chan struct{} // FIXME: drop
	l       net.Listener
}

// run and stop-on-cleanup.
// If "replay" is true this will use the local replay files (and fail if there are none), otherwise it'll connect to the postgres URL.
func Run(ctx context.Context, t *testing.T, postgresURL string, replay bool) string {
	t.Helper()
	s := NewSnap(ctx, t, postgresURL, replay)
	t.Cleanup(s.Finish)
	return s.Addr()
}

// Same as Run(), but connects to postgres (and writes the .txt files) if, and only if, PGPROXY is set in ENV (anything non-empty)
// This is the recommended function to use.
// Connects to PGURL.
// .txt file is "pgsnap_[based on test name].txt"
func RunEnv(t *testing.T) string {
	t.Helper()
	ctx := context.Background()

	proxy := os.Getenv("PGPROXY") != ""
	return Run(ctx, t, os.Getenv("PGURL"), !proxy)
}

// Wrapper for RunEnv which returns a pgx.Conn directly.
func RunEnvPGX(t *testing.T) *pgx.Conn {
	t.Helper()
	ctx := context.Background()

	addr := RunEnv(t)

	db, err := pgx.Connect(ctx, addr)
	if err != nil {
		t.Fatalf("pg connect: %s", err)
	}
	return db
}

// Wrapper for RunEnv which returns a pgxpool.Pool.
func RunEnvPGXPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	ctx := context.Background()

	addr := RunEnv(t)

	p, err := pgxpool.Connect(ctx, addr)
	if err != nil {
		t.Fatalf("pg connect: %s", err)
	}
	// FIXME: set max 1 connection
	return p
}

// Manual invocation of snap. Usually you would use RunEnv or Run.
// See .Addr() and .Finish().
func NewSnap(ctx context.Context, t *testing.T, postgresURL string, replay bool) *Snap {
	ctx, cancel := context.WithCancel(ctx)

	s := &Snap{
		t:       t,
		errchan: make(chan error, 2),
		done:    make(chan struct{}),
		cancel:  cancel,
	}

	s.listen()
	if replay {
		script, err := s.getScript()
		if err != nil {
			t.Fatal(err)
		}
		s.runFakePostgres(ctx, script)
		close(s.done)
		return s
	}

	p, err := s.startProxy(ctx, postgresURL)
	if err != nil {
		t.Fatalf("pgsnap: %s", err)
	}
	go func() {
		rw := NewReplayWriter()
		if err := p.run(ctx, rw, s.l); err != nil {
			// can't Fatal() in a go routine
			t.Errorf("pgsnap: %s", err)
			s.errchan <- err
			return
		}
		if err := rw.WriteFile(s.getFilename()); err != nil {
			s.errchan <- err
			return
		}
		close(s.done) // fixme: simply close errchan
	}()
	return s
}

func (s *Snap) Addr() string {
	return s.addr
}

func (s *Snap) Finish() {
	s.l.Close()
	s.cancel()

	if err := s.waitFor(5 * time.Second); err != nil {
		s.t.Helper()
		s.t.Errorf("pgsnap finish: %s", err)
	}
}

func (s *Snap) waitFor(d time.Duration) error {
	select {
	case <-time.After(d):
		return errors.New("pgsnap timeout")
	case e := <-s.errchan:
		return e
	case <-s.done:
		return nil
	}
}

func (s *Snap) getFile() (*os.File, error) {
	return os.Open(s.getFilename())
}

func (s *Snap) getFilename() string {
	return getFilename(s.t)
}

func getFilename(t *testing.T) string {
	n := t.Name()
	n = strings.TrimPrefix(n, "Test")
	n = strings.ReplaceAll(n, "/", "__")
	n = strings.Map(func(r rune) rune {
		switch {
		case unicode.IsLetter(r) || unicode.IsNumber(r):
			return r
		default:
			return '_'
		}
	}, n)
	n = strings.ToLower(n)
	return "pgsnap_" + n + ".txt"
}

func (s *Snap) listen() {
	var err error

	s.l, err = net.Listen("tcp", "127.0.0.1:")
	if err != nil {
		s.t.Fatal("can't open port: " + err.Error())
	}

	s.addr = fmt.Sprintf("postgres://user@%s/?sslmode=disable", s.l.Addr())
}
