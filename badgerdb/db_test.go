package badgerdb

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/tendermint/tm-db/internal/dbtest"
)

func TestGetSetHasDelete(t *testing.T) {
	dirname := t.TempDir()
	conn, err := NewDB(dirname)
	require.NoError(t, err)
	dbtest.TestGetSetHasDelete(t, conn)
}

func TestVersioning(t *testing.T) {
	dirname := t.TempDir()
	conn, err := NewDB(dirname)
	require.NoError(t, err)
	dbtest.TestVersioning(t, conn)
}
