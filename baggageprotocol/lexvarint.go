package baggageprotocol


// The number of bytes required to encode the provided uint64 such that it is lexicographically comparable to other uint64s
func SizeUnsignedLexVarint(value uint64) int {
	switch {
	case value & 0xFFFFFFFFFFFFFF80 == 0: return 1
	case value & 0xFFFFFFFFFFFFC000 == 0: return 2
	case value & 0xFFFFFFFFFFE00000 == 0: return 3
	case value & 0xFFFFFFFFF0000000 == 0: return 4
	case value & 0xFFFFFFF800000000 == 0: return 5
	case value & 0xFFFFFC0000000000 == 0: return 6
	case value & 0xFFFE000000000000 == 0: return 7
	case value & 0xFF00000000000000 == 0: return 8
	default: return 9
	}
}

// The number of bytes required to encode the provided int64 such that it is lexicographically comparable to other int64s
func SizeSignedLexVarint(svalue int64) int {
	if (svalue < 0) { svalue = -(svalue + 1) }
	switch value := uint64(svalue); {
	case value & 0xFFFFFFFFFFFFFFC0 == 0: return 1
	case value & 0xFFFFFFFFFFFFE000 == 0: return 2
	case value & 0xFFFFFFFFFFF00000 == 0: return 3
	case value & 0xFFFFFFFFF8000000 == 0: return 4
	case value & 0xFFFFFFFC00000000 == 0: return 5
	case value & 0xFFFFFE0000000000 == 0: return 6
	case value & 0xFFFF000000000000 == 0: return 7
	case value & 0xFF80000000000000 == 0: return 8
	default: return 9
	}
}

// Lexicographically encodes the int.
// Binary comparison of encoded values is same as numeric comparison of integer values
func EncodeUnsignedLexVarint(value uint64) []byte {
	size := SizeUnsignedLexVarint(value)
	bytes := make([]byte, size, size)

	// Encode from the end forwards
	for i := size - 1; i >= 0; i-- {
		bytes[i] = byte(value)
		value = value >> 8
	}

	// Encode the size
	bytes[0] |= byte(0xFF << (9-uint(size)))
	return bytes
}

func EncodeSignedLexVarint(value int64) []byte {
	// Negative values just invert the bytes
	if value < 0 {
		bytes := EncodeSignedLexVarint(-(value + 1))
		for i, b := range bytes { bytes[i] = ^b }
		return bytes
	} else {
		size := SizeSignedLexVarint(value)
		bytes := make([]byte, size, size)

		// Encode from the end forwards
		for i := size - 1; i >= 0; i-- {
			bytes[i] = byte(value)
			value >>= 8
		}

		// Encode the size in the first and possibly second byte
		if size == 9 { bytes[1] |= 0x80 }
		bytes[0] |= 0x7F & (0xFF << (9 - uint(size)))
		return bytes
	}
}

// Lexicographically encodes, but s.t. the binary comparison is the inverse of numeric comparison
func EncodeUnsignedLexVarintReverse(value uint64) []byte {
	return invert(EncodeUnsignedLexVarint(value))
}

func EncodeSignedLexVarintReverse(value int64) []byte {
	return EncodeSignedLexVarint(-(value + 1))
}

func invert(bytes []byte) []byte {
	for i := 0; i < len(bytes); i++ {
		bytes[i] = ^bytes[i]
	}
	return bytes
}

// Returns the result, and length
// Returns 0 for length if the bytes are invalid
func DecodeUnsignedLexVarint(bytes []byte) (uint64, int) {
	// Insufficient bytes in slice to decode int
	if len(bytes) == 0 { return 0, 0 }

	// First byte encodes the length of the varint
	var size uint
	switch b := bytes[0]; {
	case b & 0x80 == 0: size = 1; return uint64(b), int(size);
	case b & 0x40 == 0: size = 2; break
	case b & 0x20 == 0: size = 3; break
	case b & 0x10 == 0: size = 4; break
	case b & 0x08 == 0: size = 5; break
	case b & 0x04 == 0: size = 6; break
	case b & 0x02 == 0: size = 7; break
	case b & 0x01 == 0: size = 8; break
	default: size = 9; break
	}

	// Check size
	if len(bytes) < int(size) { return 0, 0 }

	// First byte
	result := uint64(bytes[0] & (0xFF >> size))

	// Remaining bytes
	for i := uint(1); i < size; i++ {
		result = (result << 8) | uint64(bytes[i])
	}
	return result, int(size);
}

func DecodeUnsignedLexVarintReverse(bytes []byte) (uint64, int) {
	defer invert(bytes) // invert them back to original
	return DecodeUnsignedLexVarint(invert(bytes))
}

func DecodeSignedLexVarint(bytes []byte) (int64, int) {
	if len(bytes) == 0 { return 0, 0 } // Need at least one byte

	//////////////////////////////////////////////////////////////////////////////////////////////////////
	// Interpret the first byte, which tells us:
	// -- the sign (first bit)
	// -- the exact length in bytes, if the length is <= 7 bytes
	//
	// If the length is 8 or 9 bytes, we inspect the first bit of the second byte to distinguish.
	//////////////////////////////////////////////////////////////////////////////////////////////////////

	// Negative integers are just encoded as positive, then bitflipped.  Use an accessor function to do this for us
	isNegative := bytes[0] & 0x80 == 0x00
	b := func (i uint) byte { return bytes[i] }
	makeResult := func (r uint64) int64 { return int64(r) }
	if isNegative {
		b = func (index uint) byte { return ^bytes[index] }
		makeResult = func (r uint64) int64 { return -int64(r) - 1 }
	}

	// Determine size
	var size uint
	switch b0 := b(0); {
	case b0 & 0x40 == 0: size = 1; return makeResult(uint64(b0 & 0x3F)), int(size)
	case b0 & 0x20 == 0: size = 2; break
	case b0 & 0x10 == 0: size = 3; break
	case b0 & 0x08 == 0: size = 4; break
	case b0 & 0x04 == 0: size = 5; break
	case b0 & 0x02 == 0: size = 6; break
	case b0 & 0x01 == 0: size = 7; break
	default:
		switch {
		case len(bytes) == 1: return 0, 0  // Need a second byte
		case b(1) & 0x80 == 0: size = 8; break
		default: size = 9;
		}
	}

	// Check size
	if len(bytes) < int(size) { return 0, 0 }

	// First byte
	result := uint64(b(0) & (0x7F >> size))

	// Second byte
	switch size {
	case 8: case 9: result = (result << 7) | uint64(b(1) & 0x7F)
	default: result = (result << 8) | uint64(b(1))
	}

	// Do remaining bytes
	for i := uint(2); i < size; i++ { result = (result << 8) | uint64(b(i)) }

	return makeResult(result), int(size)
}

func DecodeSignedLexVarintReverse(bytes []byte) (int64, int) {
	result, nbytes := DecodeSignedLexVarint(bytes)
	return -(result + 1), nbytes
}