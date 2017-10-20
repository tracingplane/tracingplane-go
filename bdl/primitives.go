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

func ReadLexVarInt32(bytes []byte) *int32 {
	value, length := baggageprotocol.DecodeSignedLexVarint(bytes)
	if length == 0 || value > math.MaxInt32 || value < math.MinInt32 { return nil }
	v := int32(value)
	return &v
}

func WriteLexVarInt32(v int32) []byte {
	return baggageprotocol.EncodeSignedLexVarint(int64(v))
}

func ReadLexVarInt64(bytes []byte) *int64 {
	value, length := baggageprotocol.DecodeSignedLexVarint(bytes)
	if length == 0 { return nil }
	return &value
}

func WriteLexVarInt64(v int64) []byte {
	return baggageprotocol.EncodeSignedLexVarint(v)
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

func ReadInt32Fixed(bytes []byte) *int32 {
	if len(bytes) != 4 { return nil }
	v := int32(binary.BigEndian.Uint32(bytes))
	return &v
}

func WriteInt32Fixed(v int32) []byte {
	return WriteUint32Fixed(uint32(v))
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

func ReadInt64Fixed(bytes []byte) *int64 {
	if len(bytes) != 8 { return nil }
	value := int64(binary.BigEndian.Uint64(bytes))
	return &value
}

func WriteInt64Fixed(v int64) []byte {
	return WriteUint64Fixed(uint64(v))
}

func ReadBool(bytes []byte) *bool {
	var value bool
	switch {
	case len(bytes) != 1: return nil
	case bytes[0] == byte(0): value = false
	case bytes[0] == byte(1): value = true
	default: return nil
	}
	return &value
}

func WriteBool(v bool) []byte {
	if v { return []byte{1} }
	return []byte{0}
}

func ReadTaint(bytes []byte) *bool {
	//boolValue := ReadBool(bytes)
	//if boolValue == nil { return nil }
	//*boolValue = !*boolValue
	//return boolValue
	// This should be read inverted, but currently not
	return ReadBool(bytes)
}

func WriteTaint(v bool) []byte {
	//if v { return []byte{0} }
	//return []byte{1}
	// This should be written inverted, but currently not
	return WriteBool(v)
}