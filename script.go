package pgsnap

import (
	"bufio"
	"encoding/json"
	"errors"
	"net"
	"os"
	"time"

	"github.com/jackc/pgproto3/v2"

	"github.com/alicebob/pgsnap/pgmock"
)

var (
	EmptyScript = errors.New("script is empty")
)

func (s *Snap) getScript() (*pgmock.Script, error) {
	f, err := s.getFile()
	if err != nil {
		return nil, err
	}

	script := s.readScript(f)
	// if len(script.Steps) < len(pgmock.AcceptUnauthenticatedConnRequestSteps())+1 {
	// return script, EmptyScript
	// }

	return script, nil
}

func (s *Snap) runFakePostgres(script *pgmock.Script) {
	go s.acceptConnForScript(script)
}

func (s *Snap) acceptConnForScript(script *pgmock.Script) {
	conn, err := s.l.Accept()
	if err != nil {
		s.errchan <- err
		return
	}
	defer conn.Close()

	if err = conn.SetDeadline(time.Now().Add(time.Second)); err != nil {
		s.errchan <- err
		return
	}

	be := pgproto3.NewBackend(pgproto3.NewChunkReader(conn), conn)

	if err := script.Run(be); err != nil {
		s.waitTilSync(be)

		s.sendError(be, err)

		/*
			be.Send(&pgproto3.ErrorResponse{
				Severity:            "ERROR",
				SeverityUnlocalized: "ERROR",
				Message:             err.Error(),
			})
			be.Send(&pgproto3.ReadyForQuery{'I'})
		*/

		conn.(*net.TCPConn).SetLinger(0)
		s.errchan <- err
		return
	}

	s.done <- struct{}{}
}

func (s *Snap) waitTilSync(be *pgproto3.Backend) {
	for i := 0; i < 10; i++ {
		msg, err := be.Receive()
		if err != nil {
			continue
		}

		_, ok := msg.(*pgproto3.Sync)
		if ok {
			break
		}
	}
}

func (s *Snap) sendError(be *pgproto3.Backend, err error) {
	be.Send(&pgproto3.ErrorResponse{
		Severity:            "ERROR",
		SeverityUnlocalized: "ERROR",
		Code:                "99999",
		Message:             "pgsnap:\n" + err.Error(),
	})
	be.Send(&pgproto3.ReadyForQuery{'I'})
}

func (s *Snap) readScript(f *os.File) *pgmock.Script {
	// script := &pgmock.Script{
	// Steps: pgmock.AcceptUnauthenticatedConnRequestSteps(),
	// }
	script := pgmock.NewScript()

	scanner := bufio.NewScanner(f)

	for scanner.Scan() {
		b := scanner.Bytes()
		msg, err := s.unmarshal(b)
		if err != nil {
			s.t.Fatalf("script read error: %s", err)
		}
		script.Append(msg)
	}

	return script
}

func (s *Snap) unmarshal(src []byte) (pgproto3.Message, error) {
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
	default:
		panic("unknown type: " + t.Type)
	}

	err := json.Unmarshal(src, o)
	return o, err
}
