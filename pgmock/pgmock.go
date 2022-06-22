// Package pgmock provides the ability to mock a PostgreSQL server.
package pgmock

import (
	"fmt"
	"reflect"

	"github.com/jackc/pgproto3/v2"
)

type Script struct {
	Steps    []pgproto3.Message
	Prepared map[string]string // live preprared name -> replay name
}

func NewScript() *Script {
	return &Script{
		Prepared: map[string]string{},
	}
}

func (s *Script) Append(msg pgproto3.Message) {
	s.Steps = append(s.Steps, msg)
}

func (s *Script) Run(b *pgproto3.Backend) error {
	if err := handleStartup(b); err != nil {
		return err
	}

	for _, step := range s.Steps {
		if cmd, ok := step.(pgproto3.BackendMessage); ok {
			if err := s.SendReply(b, cmd); err != nil {
				return err
			}
			continue
		}
		if cmd, ok := step.(pgproto3.FrontendMessage); ok {
			if err := s.ReadMessage(b, cmd); err != nil {
				return err
			}
			continue
		}
		fmt.Printf("unhandled message type...\n")
	}

	return nil
}

func (s *Script) SendReply(b *pgproto3.Backend, msg pgproto3.BackendMessage) error {
	return b.Send(msg)
}

func (s *Script) ReadMessage(b *pgproto3.Backend, want pgproto3.FrontendMessage) error {
	msg, err := b.Receive()
	if err != nil {
		return err
	}

	if _, ok := msg.(*pgproto3.Terminate); ok {
		return nil
	}

	if q, ok := msg.(*pgproto3.Parse); ok {
		live := q.Name
		if live != "" {
			if wq, ok := want.(*pgproto3.Parse); ok {
				stored := wq.Name
				s.Prepared[live] = stored
				q.Name = stored
				msg = q
			}
		}
	}
	if q, ok := msg.(*pgproto3.Describe); ok {
		live := q.Name
		if live != "" {
			// fmt.Printf("describe with name: %q -> %q\n", live, s.Prepared[live])
			q.Name = s.Prepared[live]
			msg = q
		}
	}
	if q, ok := msg.(*pgproto3.Bind); ok {
		live := q.PreparedStatement
		if live != "" {
			// fmt.Printf("bind with name: %q -> %q\n", live, s.Prepared[live])
			q.PreparedStatement = s.Prepared[live]
			msg = q
		}
	}

	if q, ok := want.(*pgproto3.Bind); ok {
		if len(q.Parameters) == 0 {
			// json decoding gives a zero-length slice, not nil.
			q.Parameters = nil
		}
		want = q
	}

	// if e.any && reflect.TypeOf(msg) == reflect.TypeOf(e.want) {
	// return nil
	// }

	if !reflect.DeepEqual(msg, want) {
		return fmt.Errorf("msg => %#v, want => %#v", msg, want)
	}

	return nil
}

// taken from github.com/jackc/pgproto3/example/pgfortune/server.go
func handleStartup(backend *pgproto3.Backend) error {
	startupMessage, err := backend.ReceiveStartupMessage()
	if err != nil {
		return fmt.Errorf("error receiving startup message: %w", err)
	}

	switch startupMessage.(type) {
	case *pgproto3.StartupMessage:
		backend.Send(&pgproto3.AuthenticationOk{})
		backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
		// if _, err = conn.Write(buf); err != nil {
		// return fmt.Errorf("error sending ready for query: %w", err)
		// }
		return nil
	case *pgproto3.SSLRequest:
		return fmt.Errorf("SSL not yet supported")
		// _, err = conn.Write([]byte("N"))
		// if err != nil {
		// return fmt.Errorf("error sending deny SSL request: %w", err)
		// }
		// return handleStartup(backend, conn)
	default:
		return fmt.Errorf("unknown startup message: %#v", startupMessage)
	}
}
