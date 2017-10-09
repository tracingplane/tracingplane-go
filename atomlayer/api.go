package atomlayer

import (
	"bytes"
	"github.com/golang/protobuf/proto"
	"encoding/base64"
	"fmt"
)

// Provides the base declaration of BaggageContext and Atoms.  BaggageContext is just a slice of atoms.
// Also provides implementation of the five fundamental propagation primitives:
//  * Branch -- duplicate a context because execution is branching
//  * Merge -- merge two contexts from merging execution branches
//  * Serialize / Deserialize --
//  * Trim -- impose size restrictions on context

type Atom []byte
type BaggageContext []Atom

// Merges two BaggageContexts by lexicographically comparing their atoms
func Merge(a, b BaggageContext) BaggageContext {
	if a == nil && b == nil { return nil }
	merged := BaggageContext(make([]Atom, 0, len(a)+len(b)))
	i, j := 0, 0
	for i < len(a) && j < len(b) {
		switch bytes.Compare(a[i], b[j]) {
		case -1: merged = append(merged, a[i]); i++;
		case 0: merged = append(merged, a[i]); i++; j++;
		case 1: merged = append(merged, b[j]); j++;
		}
	}
	merged = append(merged, a[i:]...)
	merged = append(merged, b[j:]...)
	return merged
}

// Duplicates a BaggageContext
func Branch(a BaggageContext) BaggageContext {
	return append(BaggageContext(nil), a...)
}

// Returns the serialized size in bytes of this atom array.
func (atoms BaggageContext) SerializedSize() (size int) {
	for _, atom := range atoms { size += atom.serializedSize() }
	return
}

// Calculate the serialized size in bytes of this atom
func (atom Atom) serializedSize() int {
	return proto.SizeVarint(uint64(len(atom))) + len(atom)
}

// Serializes the baggage context by varint-prefixing each atom.]
func Serialize(atoms BaggageContext) []byte {
	if atoms == nil { return nil }
	length := atoms.SerializedSize()
	serializedAtoms := make([]byte, 0, length)
	for _, atom := range atoms {
		serializedAtoms = append(serializedAtoms, proto.EncodeVarint(uint64(len(atom)))...)
		serializedAtoms = append(serializedAtoms, atom...)
	}
	return serializedAtoms
}

// Deserializes a baggage context from bytes
func Deserialize(bytes []byte) (atoms BaggageContext, err error) {
	pos := 0
	for len(bytes) > 0 {
		x, n := proto.DecodeVarint(bytes)
		switch {
		case n == 0 && len(bytes) > 10:  bytes = bytes[:10]; fallthrough
		case n == 0: err = fmt.Errorf("Encountered at position %v invalid varint %v", pos, bytes); return
		case n + int(x) > len(bytes): err = fmt.Errorf("Insufficient bytes remaining in buffer for %v-length atom at position %v", x, pos); return
		default: {
			bytes = bytes[n:]
			atoms = append(atoms, Atom(bytes[:int(x)]))
			bytes = bytes[int(x):]
			pos += n + int(x)
		}}
	}
	return
}

// Serializes the provided BaggageContext then base64 encodes it into a string
func EncodeBase64(ctx BaggageContext) string {
	return base64.StdEncoding.EncodeToString(Serialize(ctx))
}

// Decodes and deserializes a BaggageContext from the provided base64-encoded string
func DecodeBase64(encoded string) (BaggageContext, error) {
	bytes, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return nil, err
	}
	return Deserialize(bytes)
}

var trimMarker = Atom(make([]byte, 0, 0)) // Special zero-length atom used to indicate trim

func IsTrimMarker(a Atom) bool {
	return bytes.Equal(trimMarker, a)
}

// Drop atoms from the BaggageContext so that it fits into the specified number of bytes
func Trim(atoms BaggageContext, maxSize int) BaggageContext {
	switch trimAt := atoms.indexForTrim(maxSize); {
	case trimAt == len(atoms): return atoms
	default: return append(atoms[:trimAt], trimMarker)
	}
}

// Calculates the index at which to trim the baggage to fit in the specified size
func (baggage BaggageContext) indexForTrim(size int) int {
	for i, atom := range baggage {
		switch atomSize := atom.serializedSize(); {
		case atomSize < size: size -= atomSize;
		case atomSize > size: return i
		case i == len(baggage)-1: size -= atomSize;
		default: return i
		}
	}
	return len(baggage)
}

