package bdl

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestUint32(t *testing.T) {
	assert.Equal(t, 4, len(WriteUint32Fixed(0)))

	assert.Equal(t, uint32(0), *ReadUint32Fixed(WriteUint32Fixed(0)))
	assert.Equal(t, uint32(1), *ReadUint32Fixed(WriteUint32Fixed(1)))
	assert.Equal(t, uint32(2), *ReadUint32Fixed(WriteUint32Fixed(2)))
	assert.Equal(t, uint32(3), *ReadUint32Fixed(WriteUint32Fixed(3)))
	assert.Equal(t, uint32(4), *ReadUint32Fixed(WriteUint32Fixed(4)))
	assert.Equal(t, uint32(5), *ReadUint32Fixed(WriteUint32Fixed(5)))
	assert.Equal(t, uint32(256), *ReadUint32Fixed(WriteUint32Fixed(256)))
	assert.Equal(t, uint32(65536), *ReadUint32Fixed(WriteUint32Fixed(65536)))
	assert.Equal(t, uint32(16777216), *ReadUint32Fixed(WriteUint32Fixed(16777216)))
	assert.Equal(t, uint32(4294967295), *ReadUint32Fixed(WriteUint32Fixed(4294967295)))
}

func TestUint64(t *testing.T) {
	assert.Equal(t, 8, len(WriteUint64Fixed(0)))

	assert.Equal(t, uint64(0), *ReadUint64Fixed(WriteUint64Fixed(0)))
	assert.Equal(t, uint64(1), *ReadUint64Fixed(WriteUint64Fixed(1)))
	assert.Equal(t, uint64(256), *ReadUint64Fixed(WriteUint64Fixed(256)))
	assert.Equal(t, uint64(65536), *ReadUint64Fixed(WriteUint64Fixed(65536)))
	assert.Equal(t, uint64(16777216), *ReadUint64Fixed(WriteUint64Fixed(16777216)))
	assert.Equal(t, uint64(4294967296), *ReadUint64Fixed(WriteUint64Fixed(4294967296)))
	assert.Equal(t, uint64(1099511627776), *ReadUint64Fixed(WriteUint64Fixed(1099511627776)))
	assert.Equal(t, uint64(281474976710656), *ReadUint64Fixed(WriteUint64Fixed(281474976710656)))
	assert.Equal(t, uint64(72057594037927940), *ReadUint64Fixed(WriteUint64Fixed(72057594037927940)))
	assert.Equal(t, uint64(18446744073709549999), *ReadUint64Fixed(WriteUint64Fixed(18446744073709549999)))
}