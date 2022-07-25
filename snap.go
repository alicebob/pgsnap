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
	s := NewSnap(ctx, t, postgresURL, replay)
	t.Helper()
	t.Cleanup(s.Finish)
	return s.Addr()
}

// Same as Run(), but does replay if, and only if, PGREPLAY is set in ENV (anything non-empty)
// This is the recommended function to use.
// TODO: get url from ENV as well
func RunEnv(t *testing.T, postgresURL string) string {
	ctx := context.Background()
	replay := os.Getenv("PGREPLAY") != ""
	t.Helper()
	return Run(ctx, t, postgresURL, replay)
}

// Manual invocation of snap. Usually you would use RunEnv or Run.
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
			fmt.Printf("runProxy res: %s\n", err)
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
	fmt.Printf("start snap Finish\n")
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
