package fbptree

import "fmt"

// FBPTree represents B+ tree store in the file.
type FBPTree struct {
	records *records
	order   int
	rootId  uint32
}

// Opens an existent B+ tree or creates a new file.
func Open(path string, pageSize uint16, order int) (*FBPTree, error) {
	if order < 3 {
		return nil, fmt.Errorf("order must be >= 3")
	}

	pager, err := openPager(path, pageSize)
	if err != nil {
		return nil, fmt.Errorf("failed to instantiate the pager: %w", err)
	}

	records := newRecords(pager)

	return &FBPTree{records: records, order: order, rootId: records.firstRecordId()}, nil
}

// Put puts the key and the value into the tree. Returns true if the
// key already exists and anyway overwrites it.
func (t *FBPTree) Put(key, value []byte) ([]byte, bool, error) {

	return nil, false, nil
}

// Get return the value by the key. Returns true if the
// key exists.
func (t *FBPTree) Get(key []byte) ([]byte, bool, error) {

	return nil, false, nil
}

// Delete deletes the value by the key. Returns true if the
// key exists.
func (t *FBPTree) Delete(key []byte) ([]byte, bool, error) {

	return nil, false, nil
}
