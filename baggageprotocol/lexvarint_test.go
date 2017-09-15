package baggageprotocol

import (
  "testing"
  "github.com/stretchr/testify/assert"
  "math/rand"
	"bytes"
)


func TestWriteLexVarUInt64(t *testing.T) {
	for i := 0; i < 128; i++ {
		bytes := EncodeUnsignedLexVarint(uint64(i))
		assert.Equal(t, 1, len(bytes))
		assert.Equal(t, byte(i), bytes[0])
	}
}

func TestSizeUnsignedLexVarint(t *testing.T) {
	assert.Equal(t, 1, SizeUnsignedLexVarint(0))
	assert.Equal(t, 2, SizeUnsignedLexVarint(128))
	assert.Equal(t, 3, SizeUnsignedLexVarint(128 * 256))
	assert.Equal(t, 4, SizeUnsignedLexVarint(128 * 256 * 256))
	assert.Equal(t, 5, SizeUnsignedLexVarint(128 * 256 * 256 * 256))
	assert.Equal(t, 5, SizeUnsignedLexVarint(2147483647))
	assert.Equal(t, 6, SizeUnsignedLexVarint(128 * 256 * 256 * 256 * 256))
	assert.Equal(t, 7, SizeUnsignedLexVarint(128 * 256 * 256 * 256 * 256 * 256))
	assert.Equal(t, 8, SizeUnsignedLexVarint(128 * 256 * 256 * 256 * 256 * 256 * 256))
	assert.Equal(t, 9, SizeUnsignedLexVarint(9223372036854775807))
}

func TestWriteReadLexVarUInt64(t *testing.T) {
	r := rand.New(rand.NewSource(0))
	numtests := 1000

	min := uint64(0)
	max := uint64(128)

	for size := 1; size < 9; size++ {
		for i := 0; i < numtests; i++ {
			value := min + ((uint64(r.Uint32())<<32 + uint64(r.Uint32())) % (max - min))
			assert.True(t, value >= min)
			assert.True(t, value < max)

			bytes := EncodeUnsignedLexVarint(value)
			assert.Equal(t, size, len(bytes))

			valueRead, lengthRead := DecodeUnsignedLexVarint(bytes)
			assert.Equal(t, value, valueRead)
			assert.Equal(t, size, lengthRead)
		}
		min = max
		max = min * 128

	}

	for i := 0; i < numtests; i++ {
		value := int64(uint64(r.Uint32())<<32 + uint64(r.Uint32()))
		if value >= 0 {
			value = -value;
		}
		assert.True(t, value < 0)
		uvalue := uint64(value)

		bytes := EncodeUnsignedLexVarint(uvalue)
		assert.Equal(t, 9, len(bytes))

		valueRead, lengthRead := DecodeUnsignedLexVarint(bytes)
		assert.Equal(t, uvalue, valueRead)
		assert.Equal(t, 9, lengthRead)
	}
}

func TestLexVarUint64Comparison(t *testing.T) {

	int64max := EncodeUnsignedLexVarint(9223372036854775807)
	uint64max := EncodeUnsignedLexVarint(18446744073709551615)

	assert.True(t, bytes.Compare(int64max, uint64max) < 0)

}

func generate(r *rand.Rand, size int) uint64 {
	if size == 9 {
		value := int64(uint64(r.Uint32())<<32 + uint64(r.Uint32()))
		if value >= 0 {
			value = -value
		}
		return uint64(value)
	}
	min := uint64(0)
	max := uint64(128)
	for i := 1; i < size; i++ {
		min = max
		max *= 128
	}

	return min + ((uint64(r.Uint32())<<32 + uint64(r.Uint32())) % (max - min))
}

func TestLexVarUint64Comparison2(t *testing.T) {
	r := rand.New(rand.NewSource(0))

	numtests := 100
	for i := 0; i < numtests; i++ {
		for sizea := 1; sizea <= 9; sizea++ {
			for sizeb := sizea; sizeb <= 9; sizeb++ {
				a := generate(r, sizea)
				b := generate(r, sizeb)

				bytesa := EncodeUnsignedLexVarint(a)
				bytesb := EncodeUnsignedLexVarint(b)

				assert.Equal(t, sizea, len(bytesa))
				assert.Equal(t, sizeb, len(bytesb))

				assert.Equal(t, a == b, bytes.Compare(bytesa, bytesb) == 0)
				assert.Equal(t, a < b, bytes.Compare(bytesa, bytesb) < 0)
				assert.Equal(t, a > b, bytes.Compare(bytesa, bytesb) > 0)
			}
		}
	}
}

func TestLexVarUint64Comparison2Reverse(t *testing.T) {
	r := rand.New(rand.NewSource(0))

	numtests := 100
	for i := 0; i < numtests; i++ {
		for sizea := 1; sizea <= 9; sizea++ {
			for sizeb := sizea; sizeb <= 9; sizeb++ {
				a := generate(r, sizea)
				b := generate(r, sizeb)

				bytesa := EncodeUnsignedLexVarintReverse(a)
				bytesb := EncodeUnsignedLexVarintReverse(b)

				assert.Equal(t, sizea, len(bytesa))
				assert.Equal(t, sizeb, len(bytesb))

				assert.Equal(t, a == b, bytes.Compare(bytesa, bytesb) == 0)
				assert.Equal(t, a < b, bytes.Compare(bytesa, bytesb) > 0)
				assert.Equal(t, a > b, bytes.Compare(bytesa, bytesb) < 0)
			}
		}
	}
}

func TestReverseUnsignedVarint64(t *testing.T) {
	decoded, length := DecodeUnsignedLexVarintReverse(EncodeUnsignedLexVarintReverse(174))
	assert.Equal(t, 2, length)
	assert.Equal(t, uint64(174), decoded)

}

func TestLexVarUInt64EncodeSimple(t *testing.T) {
	assert.Equal(t, []byte{0}, EncodeUnsignedLexVarint(0))
	assert.Equal(t, []byte{64}, EncodeUnsignedLexVarint(64))
	assert.Equal(t, []byte{127}, EncodeUnsignedLexVarint(127))
	assert.Equal(t, []byte{128,128}, EncodeUnsignedLexVarint(128))
	assert.Equal(t, []byte{255-64,255}, EncodeUnsignedLexVarint(16383))
	assert.Equal(t, []byte{255-32,255,255}, EncodeUnsignedLexVarint(2097151))
	assert.Equal(t, []byte{255-16,255,255,255}, EncodeUnsignedLexVarint(268435455))
	assert.Equal(t, []byte{255-8,255,255,255,255}, EncodeUnsignedLexVarint(34359738367))
	assert.Equal(t, []byte{240,255,255,255,255}, EncodeUnsignedLexVarint(4294967295))
}


func TestLexVarUInt64DecodeSimple(t *testing.T) {
	decoded, length := DecodeUnsignedLexVarint([]byte{255,0,0,0,0,0,0,0,0})
	assert.Equal(t, 9, length)
	assert.Equal(t, uint64(0), decoded)

	decoded, length = DecodeUnsignedLexVarint([]byte{255,0,0,0,0,0,0,0,55})
	assert.Equal(t, 9, length)
	assert.Equal(t, uint64(55), decoded)

	decoded, length = DecodeUnsignedLexVarint([]byte{0})
	assert.Equal(t, 1, length)
	assert.Equal(t, uint64(0), decoded)

	decoded, length = DecodeUnsignedLexVarint([]byte{64})
	assert.Equal(t, 1, length)
	assert.Equal(t, uint64(64), decoded)

	decoded, length = DecodeUnsignedLexVarint([]byte{127})
	assert.Equal(t, 1, length)
	assert.Equal(t, uint64(127), decoded)


	decoded, length = DecodeUnsignedLexVarint([]byte{128})
	assert.Equal(t, 0, length) // error


	decoded, length = DecodeUnsignedLexVarint([]byte{255-64,255})
	assert.Equal(t, 2, length)
	assert.Equal(t, uint64(16383), decoded)

	decoded, length = DecodeUnsignedLexVarint([]byte{255-32,255,255})
	assert.Equal(t, 3, length)
	assert.Equal(t, uint64(2097151), decoded)

	decoded, length = DecodeUnsignedLexVarint([]byte{255-16,255,255,255})
	assert.Equal(t, 4, length)
	assert.Equal(t, uint64(268435455), decoded)

	decoded, length = DecodeUnsignedLexVarint([]byte{255-8,255,255,255,255})
	assert.Equal(t, 5, length)
	assert.Equal(t, uint64(34359738367), decoded)

	decoded, length = DecodeUnsignedLexVarint([]byte{240,255,255,255,255})
	assert.Equal(t, 5, length)
	assert.Equal(t, uint64(4294967295), decoded)

	decoded, length = DecodeUnsignedLexVarint([]byte{248,0,255,255,255,255})
	assert.Equal(t, 6, length)
	assert.Equal(t, uint64(4294967295), decoded)

	decoded, length = DecodeUnsignedLexVarint([]byte{252,0,0,255,255,255,255})
	assert.Equal(t, 7, length)
	assert.Equal(t, uint64(4294967295), decoded)

	decoded, length = DecodeUnsignedLexVarint([]byte{254,0,0,0,255,255,255,255})
	assert.Equal(t, 8, length)
	assert.Equal(t, uint64(4294967295), decoded)

	decoded, length = DecodeUnsignedLexVarint([]byte{255,0,0,0,0,255,255,255,255})
	assert.Equal(t, 9, length)
	assert.Equal(t, uint64(4294967295), decoded)
}

func TestLexVarInt64Simple(t *testing.T) {
	decoded, length := DecodeSignedLexVarint([]byte{0,0,0,0,0,0,0,0,0})
	assert.Equal(t, 9, length)
	assert.Equal(t, int64(-9223372036854775808), decoded)

	decoded, length = DecodeSignedLexVarint([]byte{63, 191})
	assert.Equal(t, 2, length)
	assert.Equal(t, int64(-65), decoded)

	decoded, length = DecodeSignedLexVarint([]byte{64})
	assert.Equal(t, 1, length)
	assert.Equal(t, int64(-64), decoded)

	decoded, length = DecodeSignedLexVarint([]byte{109})
	assert.Equal(t, 1, length)
	assert.Equal(t, int64(-19), decoded)

	decoded, length = DecodeSignedLexVarint([]byte{124})
	assert.Equal(t, 1, length)
	assert.Equal(t, int64(-4), decoded)

	decoded, length = DecodeSignedLexVarint([]byte{127})
	assert.Equal(t, 1, length)
	assert.Equal(t, int64(-1), decoded)

	decoded, length = DecodeSignedLexVarint([]byte{128})
	assert.Equal(t, 1, length)
	assert.Equal(t, int64(0), decoded)

	decoded, length = DecodeSignedLexVarint([]byte{129})
	assert.Equal(t, 1, length)
	assert.Equal(t, int64(1), decoded)

	decoded, length = DecodeSignedLexVarint([]byte{147})
	assert.Equal(t, 1, length)
	assert.Equal(t, int64(19), decoded)

	decoded, length = DecodeSignedLexVarint([]byte{191})
	assert.Equal(t, 1, length)
	assert.Equal(t, int64(63), decoded)

	decoded, length = DecodeSignedLexVarint([]byte{192, 64})
	assert.Equal(t, 2, length)
	assert.Equal(t, int64(64), decoded)

	decoded, length = DecodeSignedLexVarint([]byte{255,255,255,255,255,255,255,255,255})
	assert.Equal(t, 9, length)
	assert.Equal(t, int64(9223372036854775807), decoded)
}
