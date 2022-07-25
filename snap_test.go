package pgsnap

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"

	"github.com/jackc/pgx/v4"
	_ "github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var addr = "postgres://postgres@127.0.0.1:15432/?sslmode=disable"

func init() {
	// set PGSNAPURL to override the postgres test address
	if a := os.Getenv("PGSNAPURL"); a != "" {
		addr = a
	}
}

func TestPQ(t *testing.T) {
	ctx := context.Background()

	a := NewSnap(ctx, t, addr, false)
	runPQ(t, a.Addr())
	a.Finish()

	b := NewSnap(ctx, t, addr, true)
	runPQ(t, b.Addr())
	b.Finish()
}

func TestPGX(t *testing.T) {
	ctx := context.Background()

	a := NewSnap(ctx, t, addr, false)
	runPGX(t, a.Addr())
	a.Finish()

	b := NewSnap(ctx, t, addr, true)
	runPGX(t, b.Addr())
	b.Finish()
}

func runPQ(t *testing.T, addr string) {
	t.Helper()

	db, err := sql.Open("postgres", addr)
	require.NoError(t, err)

	err = db.Ping()
	require.NoError(t, err)

	rows, err := db.Query("select id from mytable limit $1", 7)
	require.NoError(t, err)

	rows.Close()
}

func runPGX(t *testing.T, addr string) {
	t.Helper()
	ctx := context.Background()

	fmt.Printf("pgx connect\n")
	db, err := pgx.Connect(ctx, addr)
	require.NoError(t, err)

	fmt.Printf("pgx ping\n")
	require.NoError(t, db.Ping(ctx))

	fmt.Printf("pgx query\n")
	_, err = db.Query(ctx, "select id from mytable limit $1", 7)
	require.NoError(t, err)

	fmt.Printf("pgx close\n")
	require.NoError(t, db.Close(ctx))
}

func Test_getFilename(t *testing.T) {
	s := &Snap{t: t}
	assert.Equal(t, "pgsnap__getfilename.txt", s.getFilename())

	t.Run("another test name", func(t *testing.T) {
		s = &Snap{t: t}
		assert.Equal(t, "pgsnap__getfilename__another_test_name.txt", s.getFilename())
	})

	t.Run("what about this one?", func(t *testing.T) {
		s = &Snap{t: t}
		assert.Equal(t, "pgsnap__getfilename__what_about_this_one_.txt", s.getFilename())
	})
}
