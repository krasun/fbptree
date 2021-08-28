package fbptree

import (
	"fmt"
	"os"
)

// FBPTree represents B+ tree store in the file.
type FBPTree struct {
	records *records
	pager   *pager
	rootId  uint32
	order   int
}

type config struct {
	order    int
	pageSize uint16
}

func Order(order int) func(*config) error {
	return func(c *config) error {
		if order < 3 {
			return fmt.Errorf("order must be >= 3")
		}

		c.order = order

		return nil
	}
}

func PageSize(pageSize int) func(*config) error {
	return func(t *config) error {
		if pageSize < minPageSize {
			return fmt.Errorf("page size must be greater than or equal to %d", minPageSize)
		}

		if pageSize > maxPageSize {
			return fmt.Errorf("page size must not be greater than %d", maxPageSize)
		}

		t.pageSize = uint16(pageSize)

		return nil
	}
}

// Opens an existent B+ tree or creates a new file.
func Open(path string, options ...func(*config) error) (*FBPTree, error) {
	defaultPageSize := os.Getpagesize()
	if defaultPageSize > maxPageSize {
		defaultPageSize = maxPageSize
	}

	cfg := &config{pageSize: uint16(defaultPageSize), order: 500}
	for _, option := range options {
		err := option(cfg)
		if err != nil {
			return nil, err
		}
	}

	pager, err := openPager(path, cfg.pageSize)
	if err != nil {
		return nil, fmt.Errorf("failed to instantiate the pager: %w", err)
	}

	records := newRecords(pager)

	return &FBPTree{records: records, order: cfg.order, rootId: records.firstRecordId()}, nil
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

// Close closes the tree and free the underlying resources.
func (t *FBPTree) Close() error {
	if err := t.pager.close(); err != nil {
		return fmt.Errorf("failed to close the pager: %w", err)
	}

	return nil
}
