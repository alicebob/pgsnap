package pgsnap

import (
	"errors"
	"fmt"
	"net"
	"os"
	"strings"
	"testing"
	"time"
)

type Snap struct {
	t         *testing.T
	addr      string
	errchan   chan error
	msgchan   chan string
	done      chan struct{}
	writeMode bool
	l         net.Listener
}

// run and stop-on-cleanup.
// If "replay" is true this will use the local replay files (and fail if there are none), otherwise it'll connect to the postgres URL.
func Run(t *testing.T, postgreURL string, replay bool) string {
	s := run(t, postgreURL, replay)
	t.Cleanup(s.Finish)
	return s.Addr()
}

// Same as Run(), but does replay if, and only if, PGREPLAY is set in ENV (anything non-empty)
func RunEnv(t *testing.T, postgreURL string) string {
	replay := os.Getenv("PGREPLAY") != ""
	return Run(t, postgreURL, replay)
}

func run(t *testing.T, postgreURL string, replay bool) *Snap {
	s := &Snap{
		t:       t,
		errchan: make(chan error, 1),
		msgchan: make(chan string, 1),
		done:    make(chan struct{}),
	}
	s.listen()
	if replay {
		script, err := s.getScript()
		if err != nil {
			t.Fatal(err)
		}
		s.runFakePostgres(script)
	} else {
		s.runProxy(postgreURL)
	}
	return s
}

// NewSnap will create snap
func NewSnap(t *testing.T, postgreURL string) *Snap {
	return NewSnapWithForceWrite(t, postgreURL, false)
}

// NewSnap
func NewSnapWithForceWrite(t *testing.T, url string, forceWrite bool) *Snap {
	s := &Snap{
		t:       t,
		errchan: make(chan error, 100),
		msgchan: make(chan string, 100),
		done:    make(chan struct{}, 1),
	}

	s.listen()

	script, err := s.getScript()
	if s.shouldRunProxy(forceWrite, err) {
		s.runProxy(url)
		return s
	}

	if err != nil {
		s.t.Fatalf("can't open file %q: %s", s.getFilename(), err)
	}

	s.runFakePostgres(script)
	return s
}

func (s *Snap) Finish() {
	err := s.Wait()
	if err != nil {
		s.t.Helper()
		s.t.Error(err)
	}
}

func (s *Snap) Addr() string {
	return s.addr
}

func (s *Snap) Wait() error {
	return s.WaitFor(5 * time.Second)
}

func (s *Snap) WaitFor(d time.Duration) error {
	if s.writeMode {
		close(s.done)
	}

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
	n := s.t.Name() + ".txt"
	n = strings.ReplaceAll(n, "/", "__")
	return n
}

func (s *Snap) listen() net.Listener {
	var err error

	s.l, err = net.Listen("tcp", "127.0.0.1:")
	if err != nil {
		s.t.Fatal("can't open port: " + err.Error())
	}

	s.addr = fmt.Sprintf("postgres://user@%s/?sslmode=disable", s.l.Addr())

	return s.l
}

func (s *Snap) shouldRunProxy(forceWrite bool, err error) bool {
	if forceWrite == true {
		return true
	}

	if os.IsNotExist(err) {
		return true
	}

	if errors.Is(EmptyScript, err) {
		return true
	}

	return false
}
