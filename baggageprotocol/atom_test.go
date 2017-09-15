package baggageprotocol

import (
	"testing"
	"github.com/stretchr/testify/assert"
	"github.com/tracingplane/tracingplane-go/atomlayer"
)


func TestInterpretAtoms(t *testing.T) {
	assert.True(t, atomlayer.IsTrimMarker(atomlayer.Atom(nil)))
	assert.False(t, IsHeader(atomlayer.Atom(nil)))
	assert.False(t, IsData(atomlayer.Atom(nil)))

	assert.False(t, atomlayer.IsTrimMarker(atomlayer.Atom([]byte{0})))
	assert.True(t, IsData(atomlayer.Atom([]byte{0})))
	assert.False(t, IsHeader(atomlayer.Atom([]byte{0})))

	assert.False(t, atomlayer.IsTrimMarker(atomlayer.Atom([]byte{0, 128, 5, 3})))
	assert.True(t, IsData(atomlayer.Atom([]byte{0, 128, 5, 3})))
	assert.False(t, IsHeader(atomlayer.Atom([]byte{0, 128, 5, 3})))

	assert.False(t, atomlayer.IsTrimMarker(atomlayer.Atom([]byte{128})))
	assert.False(t, IsData(atomlayer.Atom([]byte{128})))
	assert.True(t, IsHeader(atomlayer.Atom([]byte{128})))
}

func TestInterpretDataAtoms(t *testing.T) {
	for i:=0; i<128; i++ {
		atom := atomlayer.Atom([]byte{byte(i)})
		assert.True(t, IsData(atom))
		assert.False(t, IsHeader(atom))
		assert.False(t, atomlayer.IsTrimMarker(atom))
	}
	for i:=128; i<256; i++ {
		atom := atomlayer.Atom([]byte{byte(i)})
		assert.False(t, IsData(atom))
		assert.True(t, IsHeader(atom))
		assert.False(t, atomlayer.IsTrimMarker(atom))
	}
}

func checkHeaderAtomLevel(t *testing.T, expectedLevel int, bytes []byte) {
	actualLevel, err := HeaderLevel(atomlayer.Atom(bytes))
	assert.Nil(t, err)
	assert.Equal(t, expectedLevel, actualLevel)
}

func TestInterpretHeaderAtoms(t *testing.T) {
	checkHeaderAtomLevel(t, 0, []byte{31 << 3})
	checkHeaderAtomLevel(t, 1, []byte{30 << 3})
	checkHeaderAtomLevel(t, 2, []byte{29 << 3})
	checkHeaderAtomLevel(t, 3, []byte{28 << 3})
	checkHeaderAtomLevel(t, 4, []byte{27 << 3})
	checkHeaderAtomLevel(t, 5, []byte{26 << 3})
	checkHeaderAtomLevel(t, 6, []byte{25 << 3})
	checkHeaderAtomLevel(t, 7, []byte{24 << 3})
	checkHeaderAtomLevel(t, 8, []byte{23 << 3})
	checkHeaderAtomLevel(t, 9, []byte{22 << 3})
	checkHeaderAtomLevel(t, 10, []byte{21 << 3})
	checkHeaderAtomLevel(t, 11, []byte{20 << 3})
	checkHeaderAtomLevel(t, 12, []byte{19 << 3})
	checkHeaderAtomLevel(t, 13, []byte{18 << 3})
	checkHeaderAtomLevel(t, 14, []byte{17 << 3})
	checkHeaderAtomLevel(t, 15, []byte{16 << 3})
}