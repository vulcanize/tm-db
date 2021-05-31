package badgerdb

import (
	"bytes"
	"encoding/csv"
	"os"
	"path/filepath"
	"strconv"
	"sync"

	"github.com/dgraph-io/badger/v2"
	tmdb "github.com/tendermint/tm-db"
)

var (
	versionsFilename string = "versions.csv"
)

type BadgerDB struct {
	db       *badger.DB
	versions []uint64
	mtx      sync.RWMutex
}

type badgerTxn struct {
	txn *badger.Txn
}

// NewDB creates a Badger key-value store backed to the
// directory dir supplied. If dir does not exist, it will be created.
func NewDB(dir string) (*BadgerDB, error) {
	// Since Badger doesn't support database names, we join both to obtain
	// the final directory to use for the database.
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}
	opts := badger.DefaultOptions(dir)
	// todo: NumVersionsToKeep
	opts.SyncWrites = false // note that we have Sync methods
	opts.Logger = nil       // badger is too chatty by default
	return NewDBWithOptions(opts)
}

// NewDBWithOptions creates a BadgerDB key value store
// gives the flexibility of initializing a database with the
// respective options.
func NewDBWithOptions(opts badger.Options) (*BadgerDB, error) {
	db, err := badger.OpenManaged(opts)
	if err != nil {
		return nil, err
	}
	versions, err := readVersionsFile(filepath.Join(opts.Dir, versionsFilename))
	if err != nil {
		return nil, err
	}
	return &BadgerDB{
		db:       db,
		versions: versions,
	}, nil
}

// Load a CSV file containing valid versions
func readVersionsFile(path string) ([]uint64, error) {
	var ret []uint64
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return ret, err
	}
	r := csv.NewReader(file)
	r.FieldsPerRecord = 1
	rows, err := r.ReadAll()
	if err != nil {
		return ret, err
	}
	for _, row := range rows {
		version, err := strconv.ParseUint(row[0], 10, 64)
		if err != nil {
			return ret, err
		}
		ret = append(ret, version)
	}
	return ret, nil
}

var _ tmdb.DB = (*BadgerDB)(nil)
var _ tmdb.DBReader = (*badgerTxn)(nil)
var _ tmdb.DBWriter = (*badgerTxn)(nil)

func (b *BadgerDB) NewReaderAt(version uint64) (tmdb.DBReader, error) {
	if version == 0 {
		version = b.CurrentVersion()
	}
	return &badgerTxn{txn: b.db.NewTransactionAt(version, false)}, nil
}

func (b *BadgerDB) NewWriter() tmdb.DBWriter {
	return &badgerTxn{txn: b.db.NewTransactionAt(b.CurrentVersion(), true)}
}

func (b *BadgerDB) Close() error {
	return b.db.Close()
}

func (b *BadgerDB) CurrentVersion() uint64 {
	b.mtx.RLock()
	defer b.mtx.RUnlock()
	if len(b.versions) == 0 {
		return b.InitialVersion()
	}
	return b.versions[len(b.versions)-1] + 1
}

func (b *BadgerDB) InitialVersion() uint64 {
	b.mtx.RLock()
	defer b.mtx.RUnlock()
	if len(b.versions) == 0 {
		return 1
	}
	return b.versions[0]
}

func (b *BadgerDB) SaveVersion() uint64 {
	// TODO: wait on any pending txns
	b.mtx.Lock()
	defer b.mtx.Unlock()
	id := b.CurrentVersion()
	b.versions = append(b.versions, id)
	return id
}

func (b *badgerTxn) Get(key []byte) ([]byte, error) {
	if len(key) == 0 {
		return nil, tmdb.ErrKeyEmpty
	}

	item, err := b.txn.Get(key)
	if err == badger.ErrKeyNotFound {
		// panic("foo!")
		return nil, nil
	} else if err != nil {
		return nil, err
	}
	val, err := item.ValueCopy(nil)
	if err == nil && val == nil {
		val = []byte{}
	}
	return val, err
}

func (b *badgerTxn) Has(key []byte) (bool, error) {
	if len(key) == 0 {
		return false, tmdb.ErrKeyEmpty
	}

	_, err := b.txn.Get(key)
	if err != nil && err != badger.ErrKeyNotFound {
		return false, err
	}
	return (err != badger.ErrKeyNotFound), nil
}

func (b *badgerTxn) Set(key, value []byte) error {
	if len(key) == 0 {
		return tmdb.ErrKeyEmpty
	}
	if value == nil {
		return tmdb.ErrValueNil
	}
	return b.txn.Set(key, value)
}

func (b *badgerTxn) Delete(key []byte) error {
	if len(key) == 0 {
		return tmdb.ErrKeyEmpty
	}
	return b.txn.Delete(key)
}

func (b *badgerTxn) Commit() error {
	// All commits write to the same (current) version until next SaveVersion() call
	return b.txn.CommitAt(b.txn.ReadTs(), nil)
}

func (b *badgerTxn) iteratorOpts(start, end []byte, opts badger.IteratorOptions) (*badgerDBIterator, error) {
	if (start != nil && len(start) == 0) || (end != nil && len(end) == 0) {
		return nil, tmdb.ErrKeyEmpty
	}
	iter := b.txn.NewIterator(opts)
	iter.Rewind()
	iter.Seek(start)
	if opts.Reverse && iter.Valid() && bytes.Equal(iter.Item().Key(), start) {
		// If we're going in reverse, our starting point was "end", which is exclusive.
		iter.Next()
	}
	return &badgerDBIterator{
		reverse: opts.Reverse,
		start:   start,
		end:     end,
		iter:    iter,
	}, nil
}

func (b *badgerTxn) Iterator(start, end []byte) (tmdb.Iterator, error) {
	opts := badger.DefaultIteratorOptions
	return b.iteratorOpts(start, end, opts)
}

func (b *badgerTxn) ReverseIterator(start, end []byte) (tmdb.Iterator, error) {
	opts := badger.DefaultIteratorOptions
	opts.Reverse = true
	return b.iteratorOpts(end, start, opts)
}

func (b *BadgerDB) Stats() map[string]string {
	return nil
}

type badgerDBIterator struct {
	reverse    bool
	start, end []byte

	iter *badger.Iterator

	lastErr error
}

func (i *badgerDBIterator) Close() error {
	i.iter.Close()
	return nil
}

func (i *badgerDBIterator) Domain() (start, end []byte) { return i.start, i.end }
func (i *badgerDBIterator) Error() error                { return i.lastErr }

func (i *badgerDBIterator) Next() {
	if !i.Valid() {
		panic("iterator is invalid")
	}
	i.iter.Next()
}

func (i *badgerDBIterator) Valid() bool {
	if !i.iter.Valid() {
		return false
	}
	if len(i.end) > 0 {
		key := i.iter.Item().Key()
		if c := bytes.Compare(key, i.end); (!i.reverse && c >= 0) || (i.reverse && c < 0) {
			// We're at the end key, or past the end.
			return false
		}
	}
	return true
}

func (i *badgerDBIterator) Key() []byte {
	if !i.Valid() {
		panic("iterator is invalid")
	}
	return i.iter.Item().KeyCopy(nil)
}

func (i *badgerDBIterator) Value() []byte {
	if !i.Valid() {
		panic("iterator is invalid")
	}
	val, err := i.iter.Item().ValueCopy(nil)
	if err != nil {
		i.lastErr = err
	}
	return val
}
