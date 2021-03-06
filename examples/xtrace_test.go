package examples

import (
	"github.com/stretchr/testify/assert"
	"testing"
	"github.com/tracingplane/tracingplane-go/tracingplane"
	"github.com/tracingplane/tracingplane-go/baggageprotocol"
	"github.com/tracingplane/tracingplane-go/atomlayer"
)


func header(level int, index uint64) atomlayer.Atom {
	return baggageprotocol.MakeIndexedHeader(level, index)
}

func keyed(level int, key string) atomlayer.Atom {
	return baggageprotocol.MakeKeyedHeader(level, []byte(key))
}

func data(bytes ...byte) atomlayer.Atom {
	return append([]byte{0}, bytes...)
}

func atoms(atoms ...atomlayer.Atom) []atomlayer.Atom {
	return []atomlayer.Atom(atoms)
}

func TestXTrace(t *testing.T) {
	var xtrace XTraceMetadata

	assert.Empty(t, xtrace.parentEventIDs)


	var baggage tracingplane.BaggageContext

	// Read bag 5 into the xtrace object.  Assumes xtrace is index 5
	baggage.ReadBag(5, &xtrace)

	baggage.Drop(5)

	baggage.Set(5, &xtrace)

}

func TestXTrace2(t *testing.T) {
	var err error
	var baggage tracingplane.BaggageContext
	baggage.Atoms = atoms(
		header(0, 3),
			data(5),
		header(0, 5),
			header(1, 0),
				data(143, 189, 154, 1, 65, 170, 219, 47),
			header(1, 1),
				data(242, 64, 253, 113, 224, 239, 96, 55),
				data(2, 62, 33, 56, 120, 22, 229, 128),
				data(125, 152, 88, 29, 177, 134, 140, 248),
			header(1, 3),
				data(3),
	)

	var xtrace XTraceMetadata
	err = baggage.ReadBag(5, &xtrace)
	assert.Nil(t, err)

	assert.NotNil(t, xtrace.taskID)
	assert.Equal(t, int64(-8089140025500181713), *xtrace.taskID)

	assert.Equal(t, len(xtrace.parentEventIDs), 3)
	expectParentIds := make(map[int64](struct{}))
	expectParentIds[int64(-990513252474593225)] = struct{}{}
	expectParentIds[int64(161603163048568192)] = struct{}{}
	expectParentIds[int64(9050080335756692728)] = struct{}{}

	assert.Equal(t, expectParentIds, xtrace.parentEventIDs)

	assert.False(t, xtrace.overflowed)

	assert.Equal(t, []atomlayer.Atom{baggageprotocol.MakeIndexedHeader(1, 3), baggageprotocol.MakeDataAtom([]byte{3})}, xtrace.unknown)
}

func TestXTraceTaskID(t *testing.T) {
	var xtrace XTraceMetadata
	taskID := int64(55)
	xtrace.taskID = &taskID

	var baggage tracingplane.BaggageContext
	baggage.Set(5, &xtrace)

	expect := atoms(
		header(0, 5),
			header(1, 0),
				data(0,0,0,0,0,0,0,55),
	)
	assert.Equal(t, expect, baggage.Atoms)
}

func TestXTraceTaskIDAndParents(t *testing.T) {
	var xtrace XTraceMetadata
	xtrace.SetTaskID(55)
	xtrace.AddParentEventID(70, 71)

	var baggage tracingplane.BaggageContext
	err := baggage.Set(5, &xtrace)

	assert.Nil(t, err)

	expect := atoms(
		header(0, 5),
		header(1, 0),
		data(0,0,0,0,0,0,0,55),
		header(1, 1),
		data(0,0,0,0,0,0,0,70),
		data(0,0,0,0,0,0,0,71),
	)
	assert.Equal(t, expect, baggage.Atoms)
}

func TestUpdateXTraceParents(t *testing.T) {
	var xtrace XTraceMetadata
	xtrace.SetTaskID(55)
	xtrace.AddParentEventID(70)

	var baggage tracingplane.BaggageContext
	err := baggage.Set(5, &xtrace)

	assert.Nil(t, err)

	expect := atoms(
		header(0, 5),
			header(1, 0),
				data(0,0,0,0,0,0,0,55),
			header(1, 1),
				data(0,0,0,0,0,0,0,70),
	)
	assert.Equal(t, expect, baggage.Atoms)

	xtrace = XTraceMetadata{}
	err = baggage.ReadBag(5, &xtrace)

	assert.Nil(t, err)

	assert.Equal(t, int64(55), xtrace.GetTaskID())
	assert.Equal(t, 1, xtrace.ParentEventIDsCount())
	assert.Equal(t, []int64{70}, xtrace.GetParentEventIDs())

	xtrace.ClearParentEventIDs()
	xtrace.AddParentEventID(50)

	err = baggage.Set(5, &xtrace)

	assert.Nil(t, err)

	expect = atoms(
		header(0, 5),
			header(1, 0),
				data(0,0,0,0,0,0,0,55),
			header(1, 1),
				data(0,0,0,0,0,0,0,50),
	)
	assert.Equal(t, expect, baggage.Atoms)

	xtrace = XTraceMetadata{}
	err = baggage.ReadBag(5, &xtrace)
	assert.Equal(t, int64(55), xtrace.GetTaskID())
	assert.Equal(t, 1, xtrace.ParentEventIDsCount())
	assert.Equal(t, []int64{50}, xtrace.GetParentEventIDs())

}

func TestXTraceUnprocessed(t *testing.T) {
	var baggage tracingplane.BaggageContext
	baggage.Atoms = atoms(
		header(0, 3),
			data(5),
		header(0, 5),
			header(1, 0),
				data(143, 189, 154, 1, 65, 170, 219, 47),
			header(1, 1),
				data(2, 62, 33, 56, 120, 22, 229, 128),
				data(125, 152, 88, 29, 177, 134, 140, 248),
				data(242, 64, 253, 113, 224, 239, 96, 55),
			header(1, 3),
				data(3),
		header(0, 10),
			data(100),
	)

	xtrace := XTraceMetadata{}
	err := baggage.ReadBag(5, &xtrace)

	assert.Nil(t, err)
	expectUnprocessed := atoms(
		header(1, 3),
		data(3),
	)
	assert.Equal(t, expectUnprocessed, xtrace.GetUnprocessedAtoms())

	baggage.Drop(5)
	expectBaggage := atoms(
		header(0,3),
			data(5),
		header(0,10),
			data(100),
	)
	assert.Equal(t, expectBaggage, baggage.Atoms)

	baggage.Set(7, &xtrace)
	expect := atoms(
		header(0, 3),
			data(5),
		header(0, 7),
			header(1, 0),
				data(143, 189, 154, 1, 65, 170, 219, 47),
			header(1, 1),
				data(2, 62, 33, 56, 120, 22, 229, 128),
				data(125, 152, 88, 29, 177, 134, 140, 248),
				data(242, 64, 253, 113, 224, 239, 96, 55),
			header(1, 3),
				data(3),
		header(0, 10),
			data(100),
	)
	assert.Equal(t, expect, baggage.Atoms)
}