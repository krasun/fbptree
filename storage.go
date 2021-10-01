package fbptree

import "fmt"

// storage an abstraction over the storing mechanism.
type storage struct {
	pager   *pager
	records *records
}

func newStorage(path string, pageSize uint16) (*storage, error) {
	pager, err := openPager(path, pageSize)
	if err != nil {
		return nil, fmt.Errorf("failed to instantiate the pager: %w", err)
	}

	return &storage{pager: pager, records: newRecords(pager)}, nil
}

func (s *storage) loadMetadata() (*treeMetadata, error) {

	return nil, nil
}

func (s *storage) updateMetadata(metadata *treeMetadata) error {

	return nil
}

func (s *storage) deleteMetadata() error {
	return nil
}

func (s *storage) newNode() (uint32, error) {

	return 0, nil
}

func (s *storage) updateNodeByID(nodeID uint32, node *node) error {

	return nil
}

func (s *storage) loadNodeByID(nodeID uint32) (*node, error) {

	return nil, nil
}

func (s *storage) deleteNodeByID(nodeID uint32) error {
	return nil
}

// Close closes the tree and free the underlying resources.
func (s *storage) close() error {
	if err := s.pager.close(); err != nil {
		return fmt.Errorf("failed to close the pager: %w", err)
	}

	return nil
}
