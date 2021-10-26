package fbptree

import (
	"reflect"
	"testing"
)

func TestEncodeDecodeTreeMetadata(t *testing.T) {
	treeMetadata := &treeMetadata{
		order:      542,
		rootID:     12,
		leftmostID: 42,
	}

	decoded, err := decodeTreeMetadata(encodeTreeMetadata(treeMetadata))
	if err != nil {
		t.Fatalf("failed to decode node: %s", err)
	}

	if !reflect.DeepEqual(treeMetadata, decoded) {
		t.Fatalf("tree metadata %v != decoded tree metadata %v", treeMetadata, decoded)
	}
}

func TestEncodeDecodeNode(t *testing.T) {
	node := &node{
		id:       42,
		leaf:     true,
		parentID: 75,
		keys: [][]byte{
			{1, 2, 3, 4},
			{5, 6, 7, 8},
			nil,
		},
		pointers: []*pointer{
			{uint32(42)},
			{[]byte{1, 2, 3, 4}},
			{uint32(17)},
		},
		keyNum: 2,
	}

	decoded, err := decodeNode(encodeNode(node))
	if err != nil {
		t.Fatalf("failed to decode node: %s", err)
	}

	if !reflect.DeepEqual(node, decoded) {
		t.Fatalf("node %v != decoded node %v", node, decoded)
	}
}
