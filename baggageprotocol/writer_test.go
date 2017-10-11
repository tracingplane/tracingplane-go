package baggageprotocol

import (
	"testing"
	"github.com/stretchr/testify/assert"
	"github.com/tracingplane/tracingplane-go/atomlayer"
)

//func header(level int, index uint64) atomlayer.Atom {
//	return MakeIndexedHeader(level, index)
//}
//
//func keyed(level int, key string) atomlayer.Atom {
//	return MakeKeyedHeader(level, []byte(key))
//}
//
//func data(bytes ...byte) atomlayer.Atom {
//	return append([]byte{0}, bytes...)
//}
//
//func atoms(atoms ...atomlayer.Atom) []atomlayer.Atom {
//	return []atomlayer.Atom(atoms)
//}

func TestWriteNothing(t *testing.T) {
	w := NewWriter()

	atoms, err := w.Atoms()

	assert.Nil(t, err)
	assert.Empty(t, atoms)
}

func TestBadExit(t *testing.T) {
	w := NewWriter()
	w.Exit()

	atoms, err := w.Atoms()

	assert.NotNil(t, err)
	assert.Empty(t, atoms)
}

func TestWriteData(t *testing.T) {
	w := NewWriter()

	w.Write([]byte{5, 10, 15})

	as, err := w.Atoms()

	assert.Nil(t, err)

	expect := atoms(
		data(5,10,15),
	)
	assert.Equal(t, expect, as)
}

func TestEmptyBagIsIgnored(t *testing.T) {
	w := NewWriter()

	w.Enter(22)
	w.Exit()

	as, err := w.Atoms()

	assert.Nil(t, err)
	assert.Equal(t, []atomlayer.Atom{}, as)
}

func TestBadExit2(t *testing.T) {
	w := NewWriter()

	w.Enter(22)
	w.Exit()
	w.Exit()

	_, err := w.Atoms()

	assert.NotNil(t, err)
}

func TestWriteIndexedBagData(t *testing.T) {
	w := NewWriter()

	w.Enter(22)
	w.Write([]byte{5, 10, 15})

	as, err := w.Atoms()

	assert.Nil(t, err)
	expect := atoms(
		header(0,22),
			data(5,10,15),
	)
	assert.Equal(t, expect, as)
}

func TestWriteNestedBag(t *testing.T) {
	w := NewWriter()

	w.Enter(22)
	w.Enter(4)
	w.Write([]byte{5, 10, 15})
	w.Exit()
	w.Exit()

	as, err := w.Atoms()

	assert.Nil(t, err)
	expect := atoms(
		header(0,22),
			header(1, 4),
				data(5,10,15),
	)
	assert.Equal(t, expect, as)
}

func TestWriteKeyedAndIndexedBag(t *testing.T) {
	w := NewWriter()

	w.Enter(22)
	w.Write([]byte{5, 10, 15})
	w.Exit()

	w.EnterKey([]byte("hello"))
	w.Write([]byte{23})
	w.Exit()

	as, err := w.Atoms()

	assert.Nil(t, err)
	expect := atoms(
		header(0,22),
			data(5,10,15),
		keyed(0, "hello"),
			data(23),
	)
	assert.Equal(t, expect, as)
}

func TestOutOfOrderWrite(t *testing.T) {
	w := NewWriter()

	w.Enter(2)
	w.Write([]byte{5, 10, 15})
	w.Exit()

	w.Enter(1)
	w.Write([]byte{33})
	w.Exit()


	as, err := w.Atoms()

	assert.NotNil(t, err)
	expect := atoms(
		header(0,2),
			data(5,10,15),
		header(0,1),
			data(33),
	)
	assert.Equal(t, expect, as)
}

func TestWriteOverflow(t *testing.T) {
	w := NewWriter()

	w.Enter(2)
	w.Write([]byte{5, 10, 15})
	w.MarkOverflow()
	w.Exit()

	as, err := w.Atoms()

	assert.Nil(t, err)
	expect := atoms(
		header(0,2),
			data(5,10,15),
			atomlayer.TrimMarker,
	)
	assert.Equal(t, expect, as)
}