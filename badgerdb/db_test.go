package badgerdb

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	tmdb "github.com/tendermint/tm-db"
)

func cleanup(dir string) {
	err := os.RemoveAll(dir)
	if err != nil {
		panic(err)
	}
}

func commit(t *testing.T, txn tmdb.DBReader) {
	err := txn.Commit()
	require.NoError(t, err)
}

func TestGetSetDelete(t *testing.T) {
	// Default
	dirname, err := ioutil.TempDir("", "test_badger_")
	require.Nil(t, err)
	db, err := NewDB(dirname)
	require.NoError(t, err)
	defer cleanup(dirname)

	{
		txn, err := db.NewReaderAt(0)
		require.NoError(t, err)

		// A nonexistent key should return nil.
		value, err := txn.Get([]byte("a"))
		require.NoError(t, err)
		require.Nil(t, value)

		ok, err := txn.Has([]byte("a"))
		require.NoError(t, err)
		require.False(t, ok)

		commit(t, txn)
	}

	{
		txn := db.NewWriter()

		// Set and get a value.
		err = txn.Set([]byte("a"), []byte{0x01})
		require.NoError(t, err)

		ok, err := txn.Has([]byte("a"))
		require.NoError(t, err)
		require.True(t, ok)

		value, err := txn.Get([]byte("a"))
		require.NoError(t, err)
		require.Equal(t, []byte{0x01}, value)

		// Deleting a non-existent value is fine.
		err = txn.Delete([]byte("x"))
		require.NoError(t, err)

		// Delete a value.
		err = txn.Delete([]byte("a"))
		require.NoError(t, err)

		value, err = txn.Get([]byte("a"))
		require.NoError(t, err)
		require.Nil(t, value)

		err = txn.Set([]byte("b"), []byte{0x02})
		require.NoError(t, err)

		commit(t, txn)
	}

	txn := db.NewWriter()
	defer commit(t, txn)

	// Get a committed value.
	value, err := txn.Get([]byte("b"))
	require.NoError(t, err)
	require.Equal(t, []byte{0x02}, value)

	// Setting, getting, and deleting an empty key should error.
	_, err = txn.Get([]byte{})
	require.Equal(t, tmdb.ErrKeyEmpty, err)
	_, err = txn.Get(nil)
	require.Equal(t, tmdb.ErrKeyEmpty, err)

	_, err = txn.Has([]byte{})
	require.Equal(t, tmdb.ErrKeyEmpty, err)
	_, err = txn.Has(nil)
	require.Equal(t, tmdb.ErrKeyEmpty, err)

	err = txn.Set([]byte{}, []byte{0x01})
	require.Equal(t, tmdb.ErrKeyEmpty, err)
	err = txn.Set(nil, []byte{0x01})
	require.Equal(t, tmdb.ErrKeyEmpty, err)

	err = txn.Delete([]byte{})
	require.Equal(t, tmdb.ErrKeyEmpty, err)
	err = txn.Delete(nil)
	require.Equal(t, tmdb.ErrKeyEmpty, err)

	// Setting a nil value should error, but an empty value is fine.
	err = txn.Set([]byte("x"), nil)
	require.Equal(t, tmdb.ErrValueNil, err)

	err = txn.Set([]byte("x"), []byte{})
	require.NoError(t, err)

	value, err = txn.Get([]byte("x"))
	require.NoError(t, err)
	require.Equal(t, []byte{}, value)
}
