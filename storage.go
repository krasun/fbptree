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
	data, err := s.pager.readCustomMetadata()
	if err != nil {
		return nil, fmt.Errorf("failed to read metadata: %w", err)
	}

	if data == nil {
		return nil, nil
	}

	metadata, err := decodeTreeMetadata(data)
	if err != nil {
		return nil, fmt.Errorf("failed to decode tree metadata: %w", err)
	}

	return metadata, nil
}

func (s *storage) updateMetadata(metadata *treeMetadata) error {
	data := encodeTreeMetadata(metadata)
	err := s.pager.writeCustomMetadata(data)
	if err != nil {
		return fmt.Errorf("failed to write metadata: %w", err)
	}

	return nil
}

func (s *storage) deleteMetadata() error {
	var empty [0]byte
	err := s.pager.writeCustomMetadata(empty[:])
	if err != nil {
		return fmt.Errorf("failed to write metadata: %w", err)
	}

	return nil
}

func (s *storage) newNode() (uint32, error) {
	recordID, err := s.records.new()
	if err != nil {
		return 0, fmt.Errorf("failed to instantiate new record: %w", err)
	}

	return recordID, nil
}

func (s *storage) updateNodeByID(nodeID uint32, node *node) error {
	data := encodeNode(node)
	err := s.records.write(nodeID, data)

	if err != nil {
		return fmt.Errorf("failed to write the record %d: %w", nodeID, err)
	}

	return nil
}

func (s *storage) loadNodeByID(nodeID uint32) (*node, error) {
	data, err := s.records.read(nodeID)
	if err != nil {
		return nil, fmt.Errorf("failed to read record %d: %w", nodeID, err)
	}

	node, err := decodeNode(data)
	if err != nil {
		return nil, fmt.Errorf("failed to decode record %d: %w", nodeID, err)
	}

	return node, nil
}

func (s *storage) deleteNodeByID(nodeID uint32) error {
	err := s.records.free(nodeID)
	if err != nil {
		return fmt.Errorf("failed to free the record %d: %w", nodeID, err)
	}

	return nil
}

// Close closes the tree and free the underlying resources.
func (s *storage) close() error {
	if err := s.pager.close(); err != nil {
		return fmt.Errorf("failed to close the pager: %w", err)
	}

	return nil
}
