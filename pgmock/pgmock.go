// Package pgmock provides the ability to mock a PostgreSQL server.
package pgmock

import (
	"fmt"
	"reflect"

	"github.com/jackc/pgproto3/v2"
)

type Script struct {
	Steps []pgproto3.Message
}

func (s *Script) Append(msg pgproto3.Message) {
	s.Steps = append(s.Steps, msg)
}

func (s *Script) Run(b *pgproto3.Backend) error {
	if err := handleStartup(b); err != nil {
		return err
	}

	for i, step := range s.Steps {
		fmt.Printf("run step %d: %T\n", i, step)
		if cmd, ok := step.(pgproto3.BackendMessage); ok {
			// todo: add name mapping
			if err := s.SendReply(b, cmd); err != nil {
				return err
			}
			continue
		}
		if cmd, ok := step.(pgproto3.FrontendMessage); ok {
			// todo: add name mapping
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
	// fmt.Printf("Read from client: %T (%s)\n", msg, err)
	if err != nil {
		return err
	}

	if _, ok := msg.(*pgproto3.Terminate); ok {
		return nil
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
