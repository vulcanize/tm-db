package memdb

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/tendermint/tm-db/internal/dbtest"
)

func BenchmarkMemDBRangeScans1M(b *testing.B) {
	db := NewDB()
	defer db.Close()

	dbtest.BenchmarkRangeScans(b, db, int64(1e6))
}

func BenchmarkMemDBRangeScans10M(b *testing.B) {
	db := NewDB()
	defer db.Close()

	dbtest.BenchmarkRangeScans(b, db, int64(10e6))
}

func BenchmarkMemDBRandomReadsWrites(b *testing.B) {
	db := NewDB()
	defer db.Close()

	dbtest.BenchmarkRandomReadsWrites(b, db)
}

func TestVersioning(t *testing.T) {
	conn := NewDB()
	defer conn.Close()

	db := conn.NewWriter()
	db.Set([]byte("0"), []byte("a"))
	db.Set([]byte("1"), []byte("b"))
	id := conn.SaveVersion()

	db.Set([]byte("0"), []byte("c"))
	db.Delete([]byte("1"))
	db.Set([]byte("2"), []byte("c"))

	view, err := conn.NewReaderAt(id)
	require.NoError(t, err)

	val, err := view.Get([]byte("0"))
	require.Equal(t, val, []byte("a"))
	require.NoError(t, err)

	val, err = view.Get([]byte("1"))
	require.Equal(t, val, []byte("b"))
	require.NoError(t, err)

	has, err := view.Has([]byte("2"))
	require.False(t, has)

	it, err := view.Iterator(nil, nil)
	require.NoError(t, err)
	require.Equal(t, it.Key(), []byte("0"))
	require.Equal(t, it.Value(), []byte("a"))
	it.Next()
	require.Equal(t, it.Key(), []byte("1"))
	require.Equal(t, it.Value(), []byte("b"))
	it.Next()
	require.False(t, it.Valid())
}
