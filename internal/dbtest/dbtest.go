package dbtest

import (
	"bytes"
	"encoding/binary"
	"math/rand"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	tmdb "github.com/tendermint/tm-db"
)

//----------------------------------------
// Helper functions.

func Valid(t *testing.T, itr tmdb.Iterator, expected bool) {
	valid := itr.Valid()
	require.Equal(t, expected, valid)
}

func Next(t *testing.T, itr tmdb.Iterator, expected bool) {
	itr.Next()
	// assert.NoError(t, err) TODO: look at fixing this
	valid := itr.Valid()
	require.Equal(t, expected, valid)
}

func NextPanics(t *testing.T, itr tmdb.Iterator) {
	assert.Panics(t, func() { itr.Next() }, "checkNextPanics expected an error but didn't")
}

func Domain(t *testing.T, itr tmdb.Iterator, start, end []byte) {
	ds, de := itr.Domain()
	assert.Equal(t, start, ds, "checkDomain domain start incorrect")
	assert.Equal(t, end, de, "checkDomain domain end incorrect")
}

func Item(t *testing.T, itr tmdb.Iterator, key []byte, value []byte) {
	v := itr.Value()

	k := itr.Key()

	assert.Exactly(t, key, k)
	assert.Exactly(t, value, v)
}

func Invalid(t *testing.T, itr tmdb.Iterator) {
	Valid(t, itr, false)
	KeyPanics(t, itr)
	ValuePanics(t, itr)
	NextPanics(t, itr)
}

func KeyPanics(t *testing.T, itr tmdb.Iterator) {
	assert.Panics(t, func() { itr.Key() }, "checkKeyPanics expected panic but didn't")
}

func Value(t *testing.T, db tmdb.DBReader, key []byte, valueWanted []byte) {
	valueGot, err := db.Get(key)
	assert.NoError(t, err)
	assert.Equal(t, valueWanted, valueGot)
}

func ValuePanics(t *testing.T, itr tmdb.Iterator) {
	assert.Panics(t, func() { itr.Value() })
}

func CleanupDBDir(dir, name string) {
	err := os.RemoveAll(filepath.Join(dir, name) + ".db")
	if err != nil {
		panic(err)
	}
}

const strChars = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz" // 62 characters

// RandStr constructs a random alphanumeric string of given length.
func RandStr(length int) string {
	chars := []byte{}
MAIN_LOOP:
	for {
		val := rand.Int63() // nolint:gosec // G404: Use of weak random number generator
		for i := 0; i < 10; i++ {
			v := int(val & 0x3f) // rightmost 6 bits
			if v >= 62 {         // only 62 characters in strChars
				val >>= 6
				continue
			} else {
				chars = append(chars, strChars[v])
				if len(chars) == length {
					break MAIN_LOOP
				}
				val >>= 6
			}
		}
	}

	return string(chars)
}

func Int642Bytes(i int64) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(i))
	return buf
}

func Bytes2Int64(buf []byte) int64 {
	return int64(binary.BigEndian.Uint64(buf))
}

func BenchmarkRangeScans(b *testing.B, db tmdb.DBWriter, dbSize int64) {
	b.StopTimer()

	rangeSize := int64(10000)
	if dbSize < rangeSize {
		b.Errorf("db size %v cannot be less than range size %v", dbSize, rangeSize)
	}

	for i := int64(0); i < dbSize; i++ {
		bytes := Int642Bytes(i)
		err := db.Set(bytes, bytes)
		if err != nil {
			// require.NoError() is very expensive (according to profiler), so check manually
			b.Fatal(b, err)
		}
	}
	b.StartTimer()

	for i := 0; i < b.N; i++ {

		start := rand.Int63n(dbSize - rangeSize) // nolint: gosec
		end := start + rangeSize
		iter, err := db.Iterator(Int642Bytes(start), Int642Bytes(end))
		require.NoError(b, err)
		count := 0
		for ; iter.Valid(); iter.Next() {
			count++
		}
		iter.Close()
		require.EqualValues(b, rangeSize, count)
	}
}

func BenchmarkRandomReadsWrites(b *testing.B, db tmdb.DBWriter) {
	b.StopTimer()

	// create dummy data
	const numItems = int64(1000000)
	internal := map[int64]int64{}
	for i := 0; i < int(numItems); i++ {
		internal[int64(i)] = int64(0)
	}

	// fmt.Println("ok, starting")
	b.StartTimer()

	for i := 0; i < b.N; i++ {
		// Write something
		{
			idx := rand.Int63n(numItems) // nolint: gosec
			internal[idx]++
			val := internal[idx]
			idxBytes := Int642Bytes(idx)
			valBytes := Int642Bytes(val)
			// fmt.Printf("Set %X -> %X\n", idxBytes, valBytes)
			err := db.Set(idxBytes, valBytes)
			if err != nil {
				// require.NoError() is very expensive (according to profiler), so check manually
				b.Fatal(b, err)
			}
		}

		// Read something
		{
			idx := rand.Int63n(numItems) // nolint: gosec
			valExp := internal[idx]
			idxBytes := Int642Bytes(idx)
			valBytes, err := db.Get(idxBytes)
			if err != nil {
				// require.NoError() is very expensive (according to profiler), so check manually
				b.Fatal(b, err)
			}
			// fmt.Printf("Get %X -> %X\n", idxBytes, valBytes)
			if valExp == 0 {
				if !bytes.Equal(valBytes, nil) {
					b.Errorf("Expected %v for %v, got %X", nil, idx, valBytes)
					break
				}
			} else {
				if len(valBytes) != 8 {
					b.Errorf("Expected length 8 for %v, got %X", idx, valBytes)
					break
				}
				valGot := Bytes2Int64(valBytes)
				if valExp != valGot {
					b.Errorf("Expected %v for %v, got %v", valExp, idx, valGot)
					break
				}
			}
		}

	}
}

func TestGetSetHasDelete(t *testing.T, conn tmdb.DB) {
	{
		txn, err := conn.NewReaderAt(conn.CurrentVersion())
		require.NoError(t, err)

		// A nonexistent key should return nil.
		value, err := txn.Get([]byte("a"))
		require.NoError(t, err)
		require.Nil(t, value)

		ok, err := txn.Has([]byte("a"))
		require.NoError(t, err)
		require.False(t, ok)

		txn.Discard()
	}

	{
		txn := conn.NewWriter()

		// Set and get a value.
		err := txn.Set([]byte("a"), []byte{0x01})
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

		require.NoError(t, txn.Commit())
	}

	txn := conn.NewWriter()

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

	require.NoError(t, txn.Commit())
}

func TestVersioning(t *testing.T, conn tmdb.DB) {
	db := conn.NewWriter()
	db.Set([]byte("0"), []byte("a"))
	db.Set([]byte("1"), []byte("b"))
	db.Commit()
	id := conn.SaveVersion()

	db.Set([]byte("0"), []byte("c"))
	db.Delete([]byte("1"))
	db.Set([]byte("2"), []byte("c"))

	view, err := conn.NewReaderAt(id)
	require.NoError(t, err)
	defer view.Discard()

	val, err := view.Get([]byte("0"))
	require.Equal(t, []byte("a"), val)
	require.NoError(t, err)
	val, err = view.Get([]byte("1"))
	require.Equal(t, []byte("b"), val)
	require.NoError(t, err)

	has, err := view.Has([]byte("2"))
	require.False(t, has)

	it, err := view.Iterator(nil, nil)
	require.NoError(t, err)
	require.Equal(t, []byte("0"), it.Key())
	require.Equal(t, []byte("a"), it.Value())
	it.Next()
	require.Equal(t, []byte("1"), it.Key())
	require.Equal(t, []byte("b"), it.Value())
	it.Next()
	require.False(t, it.Valid())
	it.Close()
}
