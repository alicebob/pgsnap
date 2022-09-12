package pgsnap

import (
	"context"
	"net"
	"time"

	"github.com/jackc/pgproto3/v2"

	"github.com/alicebob/pgsnap/pgmock"
)

func (s *Snap) getScript() (*pgmock.Script, error) {
	f, err := ReadReplay(s.getFilename())
	if err != nil {
		return nil, err
	}
	return f.AsMock(), nil
}

func (s *Snap) runFakePostgres(ctx context.Context, script *pgmock.Script) {
	go func() {
		if err := s.acceptConnForScript(script); err != nil {
			s.t.Error(err)
			// fmt.Printf("runFakePostgres: %s\n", err)
			// s.errchan <- err
			// close(s.errchan)
			s.l.Close() // don't accept more connections (for now?)
		}
	}()
}

func (s *Snap) acceptConnForScript(script *pgmock.Script) error {
	conn, err := s.l.Accept()
	if err != nil {
		return err
	}
	defer conn.Close()

	if err := conn.SetDeadline(time.Now().Add(time.Second)); err != nil {
		return err
	}

	be := pgproto3.NewBackend(pgproto3.NewChunkReader(conn), conn)

	if err := script.Run(be); err != nil {
		// fmt.Printf("script run: %s\n", err)
		// s.waitTilSync(be)

		s.sendError(be, err)

		conn.(*net.TCPConn).SetLinger(0)
		return err
	}
	return nil
}

func (s *Snap) waitTilSync(be *pgproto3.Backend) {
	for i := 0; i < 10; i++ {
		msg, err := be.Receive()
		if err != nil {
			continue
		}

		if _, ok := msg.(*pgproto3.Sync); ok {
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
