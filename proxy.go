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
	fmt.Printf("runProxy!\n")
	defer fmt.Printf("runProxy done!\n")

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
		fmt.Printf("start readPostgres\n")
		defer fmt.Printf("done readPostgres\n")
		if err := p.readPostgres(fe, be, rw); err != nil {
			fmt.Printf("readPostgres: %s\n", err)
		}
		conn.Close()
		beconn.Close()
		wg.Done()
	}()
	go func() {
		fmt.Printf("start readClient\n")
		defer fmt.Printf("done readClient\n")
		if err := p.readClient(fe, be, rw); err != nil {
			fmt.Printf("readClient: %s\n", err)
		}
		conn.Close()
		beconn.Close()
		wg.Done()
	}()
	wg.Wait()
	return nil
}

func (p *Proxy) readPostgres(fe *pgproto3.Frontend, be *pgproto3.Backend, rw *RWriter) error {
	for {
		msg, err := be.Receive()
		if err != nil {
			return err
		}
		rw.Add(msg)
		fe.Send(msg)

		// if _, ok := msg.(*pgproto3.Terminate); ok {
		// return nil
		// }
	}
}

func (p *Proxy) readClient(fe *pgproto3.Frontend, be *pgproto3.Backend, rw *RWriter) error {
	for {
		msg, err := fe.Receive()
		if err != nil {
			return err
		}
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
