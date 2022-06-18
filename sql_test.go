package pgsnap

import (
	"testing"

	"github.com/jackc/pgx/v4"
)

func TestExec(t *testing.T) {
	t.Run("run", runCmpT(func(t *T, c *pgx.Conn) {
		t.CompareExec(c, "DROP TABLE IF EXISTS foo")
		t.CompareExec(c, "CREATE TABLE foo (a int, b text)")
		t.CompareExec(c, "INSERT INTO foo (a, b) values (42, 'hello')")
		t.CompareExec(c, "INSERT INTO foo (a, b) values (43, 'world')")
		// t.CompareSelect(c, 2, "SELECT a, b FROM foo ORDER BY a") // prepares
		// t.CompareSelect(c, 2, "SELECT a, b FROM foo ORDER BY a") // uses prepared
		// t.CompareSelect(c, 0, "SELECT a, b FROM foo WHERE a = 9999")
	}))

	t.Skip("fixme")
	t.Run("errors", runCmpT(func(t *T, c *pgx.Conn) {
		t.CompareExecErr(c, "SELECT")
		t.CompareExecErr(c, "SELECT 1/0")
	}))
}
