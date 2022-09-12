package pgsnap

import (
	"context"
	"fmt"
	"net"
	"sync"

	"github.com/jackc/pgproto3/v2"
	"github.com/jackc/pgx/v4"
)

type Proxy struct {
	db *pgx.Conn
}

func (s *Snap) startProxy(ctx context.Context, url string) (*Proxy, error) {
	db, err := pgx.Connect(ctx, url)
	if err != nil {
		return nil, fmt.Errorf("can't connect to db %s: %w", url, err)
	}

	return &Proxy{
		db: db,
	}, nil
}

func (p *Proxy) run(ctx context.Context, rw *RWriter, l net.Listener) error {
	// we accept a single connection only
	conn, err := l.Accept()
	if err != nil {
		return err
	}

	beconn := p.db.PgConn().Conn()
	defer beconn.Close()

	be := p.prepareBackend(conn)
	fe := p.prepareFrontend(beconn)

	go func() {
		<-ctx.Done()
		conn.Close()
	}()

	// TODO: deal with errors
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		if err := p.readClient(fe, be, rw); err != nil {
			fmt.Printf("readClient: %s\n", err)
		}
		conn.Close()
		beconn.Close()
		wg.Done()
	}()
	go func() {
		if err := p.readServer(fe, be, rw); err != nil {
			fmt.Printf("readServer: %s\n", err)
		}
		conn.Close()
		beconn.Close()
		wg.Done()
	}()
	wg.Wait()
	return nil
}

func (p *Proxy) readClient(fe *pgproto3.Frontend, be *pgproto3.Backend, rw *RWriter) error {
	for {
		msg, err := be.Receive()
		if err != nil {
			return err
		}
		if false {
			fmt.Printf("client->: %T\n", msg)
			if m, ok := msg.(*pgproto3.Parse); ok {
				fmt.Printf("          %s\n", m.Query)
			}
		}
		rw.Add(msg)
		fe.Send(msg)
	}
}

func (p *Proxy) readServer(fe *pgproto3.Frontend, be *pgproto3.Backend, rw *RWriter) error {
	for {
		msg, err := fe.Receive()
		if err != nil {
			return err
		}
		// fmt.Printf("<-server: %T\n", msg)
		rw.Add(msg)
		be.Send(msg)
	}
}

func (p *Proxy) prepareBackend(conn net.Conn) *pgproto3.Backend {
	be := pgproto3.NewBackend(pgproto3.NewChunkReader(conn), conn)

	// expect startup message
	_, _ = be.ReceiveStartupMessage()
	be.Send(&pgproto3.AuthenticationOk{})
	be.Send(&pgproto3.BackendKeyData{ProcessID: 0, SecretKey: 0})
	be.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})

	return be
}

func (p *Proxy) prepareFrontend(conn net.Conn) *pgproto3.Frontend {
	return pgproto3.NewFrontend(pgproto3.NewChunkReader(conn), conn)
}
