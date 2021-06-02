package db

// Prefix Reader/Writer lets you namespace multiple DBs within a single DB.
type PrefixReader struct {
	db     DBReader
	prefix []byte
}

type PrefixWriter struct {
	db     DBWriter
	prefix []byte
}

var _ DBReader = (*PrefixReader)(nil)
var _ DBWriter = (*PrefixWriter)(nil)

func NewPrefixReader(db DBReader, prefix []byte) *PrefixReader {
	return &PrefixReader{
		prefix: prefix,
		db:     db,
	}
}

func NewPrefixWriter(db DBWriter, prefix []byte) *PrefixWriter {
	return &PrefixWriter{
		prefix: prefix,
		db:     db,
	}
}

func prefixed(prefix []byte, key []byte) []byte {
	return append(prefix, key...)
}

// Get implements DBReader.
func (pdb *PrefixReader) Get(key []byte) ([]byte, error) {
	if len(key) == 0 {
		return nil, ErrKeyEmpty
	}
	return pdb.db.Get(prefixed(pdb.prefix, key))
}

// Has implements DBReader.
func (pdb *PrefixReader) Has(key []byte) (bool, error) {
	if len(key) == 0 {
		return false, ErrKeyEmpty
	}
	return pdb.db.Has(prefixed(pdb.prefix, key))
}

// Iterator implements DBReader.
func (pdb *PrefixReader) Iterator(start, end []byte) (Iterator, error) {
	if (start != nil && len(start) == 0) || (end != nil && len(end) == 0) {
		return nil, ErrKeyEmpty
	}

	var pend []byte
	if end == nil {
		pend = cpIncr(pdb.prefix)
	} else {
		pend = prefixed(pdb.prefix, end)
	}
	itr, err := pdb.db.Iterator(prefixed(pdb.prefix, start), pend)
	if err != nil {
		return nil, err
	}
	return newPrefixIterator(pdb.prefix, start, end, itr)
}

// ReverseIterator implements DBReader.
func (pdb *PrefixReader) ReverseIterator(start, end []byte) (Iterator, error) {
	if (start != nil && len(start) == 0) || (end != nil && len(end) == 0) {
		return nil, ErrKeyEmpty
	}

	var pend []byte
	if end == nil {
		pend = cpIncr(pdb.prefix)
	} else {
		pend = prefixed(pdb.prefix, end)
	}
	ritr, err := pdb.db.ReverseIterator(prefixed(pdb.prefix, start), pend)
	if err != nil {
		return nil, err
	}
	return newPrefixIterator(pdb.prefix, start, end, ritr)
}

// Commit implements DBReader.
func (pdb *PrefixReader) Commit() error { return pdb.db.Commit() }

// Discard implements DBReader.
func (pdb *PrefixReader) Discard() { pdb.db.Discard() }

// Set implements DBWriter.
func (pdb *PrefixWriter) Set(key []byte, value []byte) error {
	if len(key) == 0 {
		return ErrKeyEmpty
	}
	return pdb.db.Set(prefixed(pdb.prefix, key), value)
}

// Delete implements DBWriter.
func (pdb *PrefixWriter) Delete(key []byte) error {
	if len(key) == 0 {
		return ErrKeyEmpty
	}
	return pdb.db.Delete(prefixed(pdb.prefix, key))
}

// Get implements DBWriter.
func (pdb *PrefixWriter) Get(key []byte) ([]byte, error) {
	return NewPrefixReader(pdb.db, pdb.prefix).Get(key)
}

// Has implements DBWriter.
func (pdb *PrefixWriter) Has(key []byte) (bool, error) {
	return NewPrefixReader(pdb.db, pdb.prefix).Has(key)
}

// Iterator implements DBWriter.
func (pdb *PrefixWriter) Iterator(start, end []byte) (Iterator, error) {
	return NewPrefixReader(pdb.db, pdb.prefix).Iterator(start, end)
}

// ReverseIterator implements DBWriter.
func (pdb *PrefixWriter) ReverseIterator(start, end []byte) (Iterator, error) {
	return NewPrefixReader(pdb.db, pdb.prefix).ReverseIterator(start, end)
}

// Close implements DBWriter.
func (pdb *PrefixWriter) Commit() error { return pdb.db.Commit() }

// Discard implements DBWriter.
func (pdb *PrefixWriter) Discard() { pdb.db.Discard() }
