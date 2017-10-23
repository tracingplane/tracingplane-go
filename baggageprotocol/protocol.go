package baggageprotocol

import (
	"github.com/tracingplane/tracingplane-go/atomlayer"
	"fmt"
)

const header_prefix_byte = 0x80
const data_prefix_byte = 0x00

func IsHeader(atom atomlayer.Atom) bool {
	return len(atom) != 0 && (atom[0] & 0x80) == 0x80
}

func IsData(atom atomlayer.Atom) bool {
	return len(atom) != 0 && (atom[0] & 0x80) == 0x00
}

func IsIndexedHeader(atom atomlayer.Atom) bool {
	return len(atom) != 0 && (atom[0] & 0x03) == 0x00
}

func IsKeyedHeader(atom atomlayer.Atom) bool {
	return len(atom) != 0 && (atom[0] & 0x03) == 0x02
}

func HeaderLevel(atom atomlayer.Atom) (int, error) {
	if len(atom) == 0 { return 0, fmt.Errorf("Invalid zero-length header atom") }
	return 15 - int((atom[0] & 0x78) >> 3), nil
}

func HeaderIndex(atom atomlayer.Atom) (uint64, error) {
	if len(atom) == 0 { return 0, fmt.Errorf("Invalid zero-length header atom") }
	index, length := DecodeUnsignedLexVarint(atom[1:])
	if length == 0 { return 0, fmt.Errorf("Malformed indexed header atom -- cannot decode varint %v", atom[1:]) }
	return uint64(index), nil
}

func HeaderKey(atom atomlayer.Atom) ([]byte, error) {
	if len(atom) == 0 { return nil, fmt.Errorf("Invalid zero-length header atom") }
	return atom[1:], nil
}

func Payload(atom atomlayer.Atom) ([]byte, error) {
	if len(atom) == 0 { return nil, fmt.Errorf("Invalid zero-length data atom") }
	return atom[1:], nil
}

func MakeIndexedHeader(level int, index uint64) []byte {
	prefix := 0x80 | ((uint8(15 - level) << 3) & 0x78) | 0x00
	payload := EncodeUnsignedLexVarint(index)
	return append(append(make([]byte, 0, len(payload)+1), prefix), payload...)
}

func MakeKeyedHeader(level int, key []byte) []byte {
	prefix := 0x80 | ((uint8(15 - level) << 3) & 0x78) | 0x04
	return append(append(make([]byte, 0, len(key)+1), prefix), key...)
}

func MakeDataAtom(payload []byte) []byte {
	return append(append(make([]byte, 0, len(payload)+1), 0x00), payload...)
}