package atomlayer

import (
	"testing"
	"github.com/stretchr/testify/assert"
)

func TestLexicographicMerge(t *testing.T) {
	a := []Atom{Atom{0,1,1,1}, Atom{1}}
	b := []Atom{Atom{0,1,1,1}, Atom{2}}
	assert.Equal(t, []Atom{Atom{0,1,1,1}, Atom{1}, Atom{2}}, Merge(a, b))

	c := []Atom{Atom{1}, Atom{0,1,1,1}}
	assert.Equal(t, []Atom{Atom{0,1,1,1}, Atom{1}, Atom{0,1,1,1}}, Merge(a, c))
	assert.Equal(t, []Atom{Atom{0,1,1,1}, Atom{1}, Atom{0,1,1,1}, Atom{2}}, Merge(b, c))
}

func TestLexicographicMerge2(t *testing.T) {
	a := []Atom{Atom{1}, Atom{0,3,1,6}, Atom{3,1,1,1}, }
	b := []Atom{Atom{1}, Atom{2,10}}
	assert.Equal(t, []Atom{Atom{1}, Atom{0,3,1,6}, Atom{2,10}, Atom{3,1,1,1}}, Merge(a, b))
}

func TestLexicographicMergeNils(t *testing.T) {
	assert.Equal(t, []Atom(nil), Merge(nil, nil))
	assert.Equal(t, []Atom{Atom{1}}, Merge([]Atom{Atom{1}}, nil))
	assert.Equal(t, []Atom{Atom{1}}, Merge(nil, []Atom{Atom{1}}))
	assert.Equal(t, []Atom{}, Merge(nil, []Atom{}))
}


func TestSerializeEmptyAtoms(t *testing.T) {
	atomContext := []Atom{Atom{}}
	assert.Equal(t, 1, len(atomContext))
	assert.Equal(t, 0, len(atomContext[0]))
	serialized := Serialize(atomContext)
	assert.NotNil(t, serialized)
	assert.Equal(t, 1, len(serialized))
	assert.Equal(t, byte(0), serialized[0])

	atomContext = []Atom{Atom{}, Atom{}, Atom{}}
	assert.Equal(t, 3, len(atomContext))
	assert.Equal(t, 0, len(atomContext[0]))
	assert.Equal(t, 0, len(atomContext[1]))
	assert.Equal(t, 0, len(atomContext[2]))
	serialized = Serialize(atomContext)
	assert.NotNil(t, serialized)
	assert.Equal(t, 3, len(serialized))
	assert.Equal(t, byte(0), serialized[0])
	assert.Equal(t, byte(0), serialized[1])
	assert.Equal(t, byte(0), serialized[2])
}

func TestSerializeOneAtom(t *testing.T) {
	atomContext := []Atom{Atom{5, 10, 20}}
	assert.Equal(t, 1, len(atomContext))
	assert.Equal(t, 3, len(atomContext[0]))
	serialized := Serialize(atomContext)
	assert.NotNil(t, serialized)
	assert.Equal(t, 4, len(serialized))
	assert.Equal(t, byte(3), serialized[0])
	assert.Equal(t, byte(5), serialized[1])
	assert.Equal(t, byte(10), serialized[2])
	assert.Equal(t, byte(20), serialized[3])
}

func TestDeserializeEmpty(t *testing.T) {
	emptyBytes := Atom{}
	atoms, err := Deserialize(emptyBytes)
	assert.Equal(t, 0, len(atoms))
	assert.Nil(t, err)
}

func TestDeserializeEmptyAtoms(t *testing.T) {
	emptyBytes := Atom{0,0,0,0,0}
	atoms, err := Deserialize(emptyBytes)
	assert.Equal(t, 5, len(atoms))
	for i := 0; i < 5; i++ {
		assert.Equal(t, 0, len(atoms[i]))
	}
	assert.Nil(t, err)
}

func TestInsufficientAtomBytesRemaining(t *testing.T) {
	badBytes := Atom{1}
	atoms, err := Deserialize(badBytes)
	assert.Equal(t, 0, len(atoms))
	assert.NotNil(t, err)
}

func TestBadVarintPrefix(t *testing.T) {
	badBytes := Atom{255}
	atoms, err := Deserialize(badBytes)
	assert.Equal(t, 0, len(atoms))
	assert.NotNil(t, err)
}

func TestSerializeDeserialize(t *testing.T) {
	atomContext := []Atom{Atom{1,2,3,4,5}, Atom{7,3,7}, Atom{}, Atom{1}, Atom{1}}

	bytes := Serialize(atomContext)
	assert.Equal(t, 15, len(bytes))

	deserializedContext, err := Deserialize(bytes)
	assert.Nil(t, err)
	assert.Equal(t, atomContext, deserializedContext)

	deserializedContext, err = Deserialize(bytes[:14])
	assert.NotNil(t, err)
	assert.Equal(t, 4, len(deserializedContext))
	assert.Equal(t, atomContext[:4], deserializedContext[:4])
}

func TestTrim(t *testing.T) {
	assert.Equal(t, []Atom{Atom{1,2,3,4,5}}, Trim([]Atom{Atom{1,2,3,4,5}}, 6))
	assert.Equal(t, []Atom{Atom{}}, 		 Trim([]Atom{Atom{1,2,3,4,5}}, 5))
	assert.Equal(t, []Atom{Atom{}}, 		 Trim([]Atom{Atom{1,2,3,4,5}}, 4))
	assert.Equal(t, []Atom{Atom{}}, 		 Trim([]Atom{Atom{1,2,3,4,5}}, 3))
	assert.Equal(t, []Atom{Atom{}}, 		 Trim([]Atom{Atom{1,2,3,4,5}}, 2))
	assert.Equal(t, []Atom{Atom{}}, 		 Trim([]Atom{Atom{1,2,3,4,5}}, 1))
	assert.Equal(t, []Atom{Atom{}}, 		 Trim([]Atom{Atom{1,2,3,4,5}}, 0))
	assert.Equal(t, []Atom(nil), 		 	 Trim(nil, 3))

	assert.Equal(t, []Atom{Atom{1,2,3,4,5}, Atom{3, 2, 1}}, Trim([]Atom{Atom{1,2,3,4,5}, Atom{3, 2, 1}}, 10))
	assert.Equal(t, []Atom{Atom{1,2,3,4,5}, Atom{}}, 		Trim([]Atom{Atom{1,2,3,4,5}, Atom{3, 2, 1}}, 9))
	assert.Equal(t, []Atom{Atom{1,2,3,4,5}, Atom{}}, 		Trim([]Atom{Atom{1,2,3,4,5}, Atom{3, 2, 1}}, 8))
	assert.Equal(t, []Atom{Atom{1,2,3,4,5}, Atom{}}, 		Trim([]Atom{Atom{1,2,3,4,5}, Atom{3, 2, 1}}, 7))
	assert.Equal(t, []Atom{Atom{}}, 						Trim([]Atom{Atom{1,2,3,4,5}, Atom{3, 2, 1}}, 6))
	assert.Equal(t, []Atom{Atom{}}, 						Trim([]Atom{Atom{1,2,3,4,5}, Atom{3, 2, 1}}, 5))
	assert.Equal(t, []Atom{Atom{}}, 						Trim([]Atom{Atom{1,2,3,4,5}, Atom{3, 2, 1}}, 4))
	assert.Equal(t, []Atom{Atom{}}, 						Trim([]Atom{Atom{1,2,3,4,5}, Atom{3, 2, 1}}, 3))
	assert.Equal(t, []Atom{Atom{}}, 						Trim([]Atom{Atom{1,2,3,4,5}, Atom{3, 2, 1}}, 2))
	assert.Equal(t, []Atom{Atom{}}, 						Trim([]Atom{Atom{1,2,3,4,5}, Atom{3, 2, 1}}, 1))
	assert.Equal(t, []Atom{Atom{}}, 						Trim([]Atom{Atom{1,2,3,4,5}, Atom{3, 2, 1}}, 0))

}