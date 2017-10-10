package baggageprotocol

import (
	"testing"
	"github.com/stretchr/testify/assert"
	"github.com/tracingplane/tracingplane-go/atomlayer"
)


func TestSliceEqual(t *testing.T) {
	assert.NotEqual(t, make([]byte, 0), nil)
	assert.NotEqual(t, nil, make([]byte, 0))
	assert.False(t, make([]byte, 0) == nil)

	arr := []byte{0}
	assert.NotEqual(t, arr[1:], nil)
}

func TestFind(t *testing.T) {
	baggage := atoms(
		header(0, 0),
			data(5),
		header(0, 2),
			data(8),
			[]byte{},
		header(0, 4),
			data(8),
	)

	found, overflowed, i := find(baggage, 0, header(0, 0))

	assert.True(t, found)
	assert.False(t, overflowed)
	assert.Equal(t, 0, i)

	found, overflowed, i = find(baggage, i+1, header(0, 0))
	assert.False(t, found)
	assert.False(t, overflowed)
	assert.Equal(t, 2, i)


	found, overflowed, i = find(baggage, 0, header(0, 1))

	assert.False(t, found)
	assert.False(t, overflowed)
	assert.Equal(t, 2, i)


	found, overflowed, i = find(baggage, 0, header(0, 2))

	assert.True(t, found)
	assert.False(t, overflowed)
	assert.Equal(t, 2, i)

	found, overflowed, i = find(baggage, i+1, header(0, 2))
	assert.False(t, found)
	assert.True(t, overflowed)
	assert.Equal(t, 5, i)


	found, overflowed, i = find(baggage, 0, header(0, 3))

	assert.False(t, found)
	assert.True(t, overflowed)
	assert.Equal(t, 5, i)


	found, overflowed, i = find(baggage, 0, header(0, 4))

	assert.True(t, found)
	assert.True(t, overflowed)
	assert.Equal(t, 5, i)

	found, overflowed, i = find(baggage, i+1, header(0, 4))
	assert.False(t, found)
	assert.False(t, overflowed)
	assert.Equal(t, 7, i)


	found, overflowed, i = find(baggage, 0, header(0, 5))

	assert.False(t, found)
	assert.True(t, overflowed)
	assert.Equal(t, 7, i)
}

func TestDrop(t *testing.T) {
	b0 := atoms(header(0, 0), data(5))
	b1 := atoms(header(0, 2), data(8), []byte{})
	b2 := atoms(header(0, 4), data(8))

	baggage := append(append(append([]atomlayer.Atom(nil), b0...), b1...), b2...)

	test1 := Drop(baggage, 0, DropMarker)
	assert.Equal(t, append(append([]atomlayer.Atom(nil), b1...), b2...), test1)

	test2 := Drop(baggage, 1, DropMarker)
	assert.Equal(t, baggage, test2)

	test3 := Drop(baggage, 2, DropMarker)
	assert.Equal(t, append(append([]atomlayer.Atom(nil), b0...), b2...), test3)

	test4 := Drop(baggage, 3, DropMarker)
	assert.Equal(t, baggage, test4)

	test5 := Drop(baggage, 4, DropMarker)
	assert.Equal(t, append(append([]atomlayer.Atom(nil), b0...), b1...), test5)
}