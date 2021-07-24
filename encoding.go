package fbptree

import "encoding/binary"

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
