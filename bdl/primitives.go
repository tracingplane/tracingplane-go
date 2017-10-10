package bdl

import (
	"github.com/tracingplane/tracingplane-go/baggageprotocol"
	"encoding/binary"
	"math"
)

func ReadLexVarUint32(bytes []byte) *uint32 {
	value, length := baggageprotocol.DecodeUnsignedLexVarint(bytes)
	if length == 0 || value > math.MaxUint32 { return nil }
	v := uint32(value)
	return &v
}

func WriteLexVarUint32(v uint32) []byte {
	return baggageprotocol.EncodeUnsignedLexVarint(uint64(v))
}

func ReadLexVarUint64(bytes []byte) *uint64 {
	value, length := baggageprotocol.DecodeUnsignedLexVarint(bytes)
	if length == 0 { return nil }
	return &value
}

func WriteLexVarUint64(v uint64) []byte {
	return baggageprotocol.EncodeUnsignedLexVarint(v)
}

func ReadUint32Fixed(bytes []byte) *uint32 {
	if len(bytes) != 4 { return nil }
	value := binary.BigEndian.Uint32(bytes)
	return &value
}

func WriteUint32Fixed(v uint32) []byte {
	bytes := make([]byte, 4)
	binary.BigEndian.PutUint32(bytes, v)
	return bytes
}

func ReadUint64Fixed(bytes []byte) *uint64 {
	if len(bytes) != 8 { return nil }
	value := binary.BigEndian.Uint64(bytes)
	return &value
}

func WriteUint64Fixed(v uint64) []byte {
	bytes := make([]byte, 8)
	binary.BigEndian.PutUint64(bytes, v)
	return bytes
}