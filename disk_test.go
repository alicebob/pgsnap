package pgsnap

import (
	"os"
	"testing"

	"github.com/jackc/pgproto3/v2"
	"github.com/stretchr/testify/require"
)

func TestDisk(t *testing.T) {
	file := "./disk_test.txt"
	os.Remove(file)

	{
		r := NewReplayWriter()

		r.Add(&pgproto3.Query{
			String: "hello world",
		})
		r.Add(&pgproto3.Query{
			String: "bye world",
		})
		r.Add(&pgproto3.Describe{ObjectType: 0x53, Name: "lrupsc_2_0"})
		require.NoError(t, r.WriteFile(file))
	}

	{
		r, err := ReadReplay(file)
		require.NoError(t, err)
		require.Len(t, r.packets, 3)
	}
}
