package pgsnap

import (
	"context"
	"reflect"
	"testing"

	"github.com/jackc/pgconn"
	"github.com/jackc/pgx/v4"
	"github.com/stretchr/testify/require"
)

type Queryer interface {
	Query(context.Context, string, ...interface{}) (pgx.Rows, error)
	Exec(context.Context, string, ...interface{}) (pgconn.CommandTag, error)
}

// cb() is called once with real pg, and then once with the pgsnap replay.
// During the callback both call t.Compare() (and CompareExec(), &c) with
// values from the database, and that should run the same for both.
// Callback is first run against the real PG.
func runCmpT(cb func(t *T, c *pgx.Conn)) func(t *testing.T) {
	return func(t *testing.T) {
		t.Helper()
		tt := &T{T: t}

		// tt.Log("running against real")
		real, s := connect(t, false)
		cb(tt, real)
		real.Close(context.Background())
		s.Finish()

		// tt.Log("running against snap")
		tt.replayMode = true
		snap, s := connect(t, true)
		cb(tt, snap)
		snap.Close(context.Background())
		s.Finish()
	}
}

type T struct {
	replayMode bool // true the 2nd time when we replay the snap log.
	// result builds up the cases used in Compare() when running against
	// realpg, and then unshifts them it runs the 2nd time. This way we get
	// nice error results with the correct line number.
	expected []interface{}
	*testing.T
}

// returns "real" or "snap"
func (t *T) sys() string {
	if t.replayMode {
		return "snap"
	}
	return "real"
}

// Compare is the main function to compare realpg with snap. We'll run a test
// first against realpg, and then build up a list of expected values by calling
// Compare(). When we then run against snap we compare against the values
// when running the same test in realpg.
func (t *T) Compare(v interface{}) {
	t.Helper()
	if !t.replayMode {
		t.expected = append(t.expected, v)
		return
	}

	if len(t.expected) == 0 {
		t.Fatalf("nothing to Compare against")
	}
	want := t.expected[0]
	t.expected = t.expected[1:]

	cmp(t, want, v)
}

// Like Compare, but wants an error
func (t *T) CompareErr(err error) {
	t.Helper()

	if err == nil && !t.replayMode {
		t.Fatalf("expected an error from realpg, got nil")
		return
	}
	t.Compare(err)
}

// Execute the SELECT-like statement, and t.Compare()s all returned rows.
// N is the number of expected rows (to make sure your query it does what you
// think it does).
// Query cannot return an error, use CompareSelectErr if it does.
func (t *T) CompareSelect(c Queryer, n int, sql string, args ...interface{}) {
	t.Helper()

	rows, err := c.Query(context.Background(), sql, args...)
	if err != nil {
		if !t.replayMode {
			t.Fatalf("did not expected an error, got: %s", err)
			return
		}
		t.Compare(err)
		return
	}

	var res []interface{}
	for rows.Next() {
		vals, err := rows.Values()
		if err != nil {
			t.Fatal(err)
		}
		res = append(res, vals)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if len(res) != n {
		t.Fatalf("expected %d rows, got %d", n, len(res))
	}
	t.Compare(res)
}

// Like CompareSelect, but this expects an error.
func (t *T) CompareSelectErr(c Queryer, sql string, args ...interface{}) {
	t.Helper()

	_, err := c.Query(context.Background(), sql, args...)
	t.CompareErr(err)
}

// Execute statement, and t.Compare()s the number of affected columns. Useful for DELETE, INSERT, and similar.
// Command can't return an error, use CompareExecErr() if it does.
func (t *T) CompareExec(c Queryer, sql string, args ...interface{}) {
	t.Helper()

	res, err := c.Exec(context.Background(), sql, args...)
	if err != nil {
		t.Fatalf("%s: %s", t.sys(), err)
		return
	}

	n := res.RowsAffected()
	t.Compare(n)
	t.Compare(string(res)) // it's the CommandTag, which is something like "INSERT 123"
}

// Like CompareExec, but this expects an error.
func (t *T) CompareExecErr(c Queryer, sql string, args ...interface{}) {
	t.Helper()

	_, err := c.Exec(context.Background(), sql, args...)
	t.CompareErr(err)
}

func cmp(t testing.TB, vReal, vSnap interface{}) {
	t.Helper()

	errReal, okReal := vReal.(error)
	errSnap, okSnap := vSnap.(error)
	if okReal && okSnap {
		errCmp(t, errReal, errSnap)
		return
	}

	if have, want := vSnap, vReal; !reflect.DeepEqual(have, want) {
		t.Fatalf("have(snap)\n  %#v\n  type: %T\nwant(real)\n  %#v\n  type: %T",
			have, have,
			want, want,
		)
	}
}

// compare two errors.
func errCmp(t testing.TB, errReal, errSnap error) {
	t.Helper()

	if errReal == nil && errSnap != nil {
		t.Fatalf("no error from real, snap error: %s", errSnap)
	}
	if errReal != nil && errSnap == nil {
		t.Fatalf("error from real, no snap error: %s", errReal)
	}
	if errReal == nil && errSnap == nil {
		return
	}

	if want, have := errReal.Error(), errSnap.Error(); have != want {
		t.Fatalf("have %q, want %q", have, want)
	}
}

func connect(t *testing.T, replay bool) (*pgx.Conn, *Snap) {
	ctx := context.Background()
	sn := NewSnap(ctx, t, addr, replay)

	db, err := pgx.Connect(ctx, sn.Addr())
	require.NoError(t, err)
	return db, sn
}
