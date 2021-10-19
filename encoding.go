package fbptree

import (
	"encoding/binary"
)

func decodeUint16(data []byte) uint16 {
	return binary.BigEndian.Uint16(data)
}

func encodeUint16(v uint16) []byte {
	var data [2]byte
	binary.BigEndian.PutUint16(data[:], v)

	return data[:]
}

func decodeUint32(data []byte) uint32 {
	return binary.BigEndian.Uint32(data)
}

func encodeUint32(v uint32) []byte {
	var data [4]byte
	binary.BigEndian.PutUint32(data[:], v)

	return data[:]
}

func encodeBool(v bool) []byte {
	var data [1]byte
	if v {
		data[0] = 1
	}

	return data[:]
}

func decodeBool(data []byte) bool {
	return data[0] == 1
}

func encodeNode(node *node) []byte {
	data := make([]byte, 0)

	data = append(data, encodeUint32(node.id)...)
	data = append(data, encodeUint32(node.parentID)...)
	data = append(data, encodeBool(node.leaf)...)
	data = append(data, encodeUint16(uint16(node.keyNum))...)
	data = append(data, encodeUint16(uint16(len(node.keys)))...)

	for _, key := range node.keys {
		if key == nil {
			break
		}

		data = append(data, encodeUint16(uint16(len(key)))...)
		data = append(data, key...)
	}

	pointerNum := node.keyNum
	if !node.leaf {
		pointerNum += 1
	}

	data = append(data, encodeUint16(uint16(pointerNum))...)
	data = append(data, encodeUint16(uint16(len(node.pointers)))...)
	for _, pointer := range node.pointers {
		if pointer == nil {
			return data
		}

		if pointer.isNodeID() {
			data = append(data, 0)
			data = append(data, encodeUint32(pointer.asNodeID())...)
		} else if pointer.isValue() {
			data = append(data, 1)
			data = append(data, encodeUint16(uint16(len(pointer.asValue())))...)
			data = append(data, pointer.asValue()...)
		}
	}

	return data
}

func decodeNode(data []byte) (*node, error) {
	position := 0
	nodeID := decodeUint32(data[position : position+4])
	position += 4
	parentID := decodeUint32(data[position : position+4])
	position += 4
	leaf := decodeBool(data[position : position+1])
	position += 1

	keyNum := decodeUint16(data[position : position+2])
	position += 2
	keyLen := int(decodeUint16(data[position : position+2]))
	position += 2
	keys := make([][]byte, keyLen)
	for k := 0; k < int(keyNum); k++ {
		keySize := int(decodeUint16(data[position : position+2]))
		position += 2

		key := data[position : position+keySize]
		keys[k] = key
		position += keySize
	}

	pointerNum := decodeUint16(data[position : position+2])
	position += 2
	pointerLen := int(decodeUint16(data[position : position+2]))
	position += 2
	pointers := make([]*pointer, pointerLen)
	for p := 0; p < int(pointerNum); p++ {
		if data[position] == 0 {
			position += 1
			// nodeID

			nodeID := decodeUint32(data[position : position+4])
			position += 4
			pointers[p] = &pointer{nodeID}
		} else if data[position] == 1 {
			position += 1
			// value
			valueSize := int(decodeUint16(data[position : position+2]))
			position += 2
			value := data[position : position+valueSize]
			position += valueSize

			pointers[p] = &pointer{value}
		}
	}

	return &node{
		nodeID,
		leaf,
		parentID,
		keys,
		int(keyNum),
		pointers,
	}, nil
}

func encodeTreeMetadata(metadata *treeMetadata) []byte {
	var data [10]byte

	copy(data[0:2], encodeUint16(metadata.order))
	copy(data[2:6], encodeUint32(metadata.rootID))
	copy(data[6:10], encodeUint32(metadata.leftmostID))

	return data[:]
}

func decodeTreeMetadata(data []byte) (*treeMetadata, error) {
	return &treeMetadata{
		order:      decodeUint16(data[0:2]),
		rootID:     decodeUint32(data[2:6]),
		leftmostID: decodeUint32(data[6:10]),
	}, nil
}
