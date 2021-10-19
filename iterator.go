package fbptree

import "fmt"

// Iterator returns a stateful Iterator for traversing the tree
// in ascending key order.
type Iterator struct {
	next    *node
	i       int
	storage *storage
}

// Iterator returns a stateful iterator that traverses the tree
// in ascending key order.
func (t *FBPTree) Iterator() (*Iterator, error) {
	if t.metadata == nil {
		return &Iterator{nil, 0, t.storage}, nil
	}

	next, err := t.storage.loadNodeByID(t.metadata.leftmostID)
	if err != nil {
		return nil, fmt.Errorf("failed to load the leftmost node %d: %w", t.metadata.leftmostID, err)
	}

	return &Iterator{next, 0, t.storage}, nil
}

// HasNext returns true if there is a next element to retrive.
func (it *Iterator) HasNext() bool {
	return it.next != nil && it.i < it.next.keyNum
}

// Next returns a key and a value at the current position of the iteration
// and advances the iterator.
// Caution! Next panics if called on the nil element.
func (it *Iterator) Next() ([]byte, []byte, error) {
	if !it.HasNext() {
		// to sleep well
		return nil, nil, fmt.Errorf("there is no next node")
	}

	key, value := it.next.keys[it.i], it.next.pointers[it.i].asValue()

	it.i++
	if it.i == it.next.keyNum {
		nextPointer := it.next.next()
		if nextPointer != nil {
			nodeID := nextPointer.asNodeID()
			next, err := it.storage.loadNodeByID(nodeID)
			if err != nil {
				return nil, nil, fmt.Errorf("failed to load the next node: %w", err)
			}

			it.next = next
		} else {
			it.next = nil
		}

		it.i = 0
	}

	return key, value, nil
}
