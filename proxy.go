package pgsnap

import (
	"context"
	"net"

	"github.com/jackc/pgproto3/v2"
	"github.com/jackc/pgx/v4"
)

func (s *Snap) runProxy(url string) {
	s.writeMode = true

	disk := newDisk(s.t)
	defer disk.Close()

	db, err := pgx.Connect(context.Background(), url)
	if err != nil {
		s.t.Fatalf("can't connect to db %s: %v", url, err)
	}

	for {
		conn, err := s.l.Accept()
		if err != nil {
			s.errchan <- err
			break
		}
		f := disk.newWriter()
		defer close(f) // fixme: do this nicer
		go s.acceptConnForProxy(db, f, conn)
	}
}

func (s *Snap) acceptConnForProxy(db *pgx.Conn, out chan<- pgproto3.Message, conn net.Conn) {
	be := s.prepareBackend(conn)
	fe := s.prepareFrontend(db)
	s.runConversation(fe, be, out)
}

func (s *Snap) runConversation(fe *pgproto3.Frontend, be *pgproto3.Backend, out chan<- pgproto3.Message) {
	// TODO: deal with error
	go s.streamBEtoFE(fe, be, out)
	go s.streamFEtoBE(fe, be, out)
}

func (s *Snap) streamBEtoFE(fe *pgproto3.Frontend, be *pgproto3.Backend, out chan<- pgproto3.Message) error {
	for {
		msg, err := be.Receive()
		if err != nil {
			return err
		}
		out <- msg
		fe.Send(msg)
	}
}

func (s *Snap) streamFEtoBE(fe *pgproto3.Frontend, be *pgproto3.Backend, out chan<- pgproto3.Message) error {
	for {
		msg, err := fe.Receive()
		if err != nil {
			return err
		}
		out <- msg
		be.Send(msg)
	}
}

func (s *Snap) prepareBackend(conn net.Conn) *pgproto3.Backend {
	be := pgproto3.NewBackend(pgproto3.NewChunkReader(conn), conn)

	// expect startup message
	_, _ = be.ReceiveStartupMessage()
	be.Send(&pgproto3.AuthenticationOk{})
	be.Send(&pgproto3.BackendKeyData{ProcessID: 0, SecretKey: 0})
	be.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})

	return be
}

func (s *Snap) prepareFrontend(db *pgx.Conn) *pgproto3.Frontend {
	conn := db.PgConn().Conn()
	return pgproto3.NewFrontend(pgproto3.NewChunkReader(conn), conn)
}
