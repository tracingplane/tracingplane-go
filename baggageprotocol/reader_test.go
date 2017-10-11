package baggageprotocol

import (
	"testing"
	"github.com/stretchr/testify/assert"
	"github.com/tracingplane/tracingplane-go/atomlayer"
	"encoding/binary"
	"bytes"
	"github.com/gogo/protobuf/proto"
)


func header(level int, index uint64) atomlayer.Atom {
	return MakeIndexedHeader(level, index)
}

func keyed(level int, key string) atomlayer.Atom {
	return MakeKeyedHeader(level, []byte(key))
}

func data(bytes ...byte) atomlayer.Atom {
	return append([]byte{0}, bytes...)
}

func atoms(atoms ...atomlayer.Atom) []atomlayer.Atom {
	return []atomlayer.Atom(atoms)
}

func TestSimpleEnter1(t *testing.T) {
	baggage := []byte{}
	atoms, err := atomlayer.Deserialize(baggage);
	assert.Nil(t, err)
	assert.Equal(t, 0, len(atoms))

	reader := Read(atoms)
	assert.Nil(t, reader.Enter())
	assert.Nil(t, reader.Next())
}

func TestSimpleEnter2(t *testing.T) {
	reader := Read(nil)
	assert.Nil(t, reader.Enter())
	assert.Nil(t, reader.Next())
}

func TestSimpleReadData(t *testing.T) {
	header := atomlayer.Atom{ 248, 5 }
	r := Read([]atomlayer.Atom{header})
	entered := r.Enter()
	assert.NotNil(t, entered)
	assert.Equal(t, header, entered)

	assert.Equal(t, 0, r.level)
	assert.Nil(t, r.next)
	assert.Empty(t, r.remaining)
	assert.Empty(t, r.Skipped)
	assert.Equal(t, []atomlayer.Atom{header}, r.currentPath)

	assert.Nil(t, r.Enter())
	assert.Nil(t, r.Next())
	assert.Equal(t, 0, r.level)
	assert.Equal(t, []atomlayer.Atom{header}, r.currentPath)

	r.Exit()
	assert.Equal(t, -1, r.level)
	assert.Empty(t, r.currentPath)

	r.Exit()
	assert.Equal(t, -1, r.level)
	assert.Empty(t, r.currentPath)
}

func TestValidLevelJump(t *testing.T) {
	baggage := []atomlayer.Atom{
		[]byte{248, 5},
		[]byte{0, 185, 124, 187, 14, 103, 240, 88, 153},
		[]byte{240, 0},
		[]byte{0, 131, 154, 212, 173, 65, 53, 70, 55},
	}

	r := Read(baggage)
	assert.Nil(t, r.Err)

	header := r.Enter()
	assert.NotNil(t, header)
	assert.Nil(t, r.Err)
}

func TestInvalidLevelJump(t *testing.T) {
	baggage := []atomlayer.Atom{
		[]byte{240, 0},
		[]byte{0, 131, 154, 212, 173, 65, 53, 70, 55},
		[]byte{248, 5},
		[]byte{0, 185, 124, 187, 14, 103, 240, 88, 153},
	}

	r := Read(baggage)
	assert.Nil(t, r.Err)

	header := r.Enter()
	assert.Nil(t, header)
	assert.NotNil(t, r.Err)
}

func TestInvalidHeaderAtom(t *testing.T) {
	baggage := []atomlayer.Atom{
		[]byte{240},
		[]byte{0, 131, 154, 212, 173, 65, 53, 70, 55},
	}

	r := Read(baggage)
	assert.Nil(t, r.Err)

	header := r.Enter()
	assert.Nil(t, header)
	assert.NotNil(t, r.Err)
}

func TestMultipleDataAtoms(t *testing.T) {
	baggage := []atomlayer.Atom{
		[]byte{0, 0},
		[]byte{0, 1},
		[]byte{0, 2},
	}

	r := Read(baggage)
	assert.Nil(t, r.Err)

	assert.NotNil(t, r.Next())
	assert.NotNil(t, r.Next())
	assert.NotNil(t, r.Next())
	assert.Nil(t, r.Next())
}

func TestEnterSkipsBags(t *testing.T) {
	baggage := []atomlayer.Atom{
		[]byte{248, 3},
		[]byte{0, 6},
		[]byte{248, 5},
		[]byte{0, 7},
	}

	r := Read(baggage)

	assert.True(t, r.EnterIndexed(5))
	assert.Equal(t, []byte{7}, r.Next())
	assert.Nil(t, r.Next())
	assert.Nil(t, r.Enter())
	assert.Equal(t, []atomlayer.Atom{[]byte{248, 5}}, r.currentPath)
	r.Exit()
	assert.Equal(t, []atomlayer.Atom{}, r.currentPath)
}

func TestSkippedAtomsSimple(t *testing.T) {
	baggage := []atomlayer.Atom{
		[]byte{248, 3},
		[]byte{0, 6},
		[]byte{248, 5},
		[]byte{0, 7},
	}

	r := Read(baggage)

	assert.True(t, r.EnterIndexed(5))
	assert.Equal(t, []atomlayer.Atom{[]byte{248, 3}, []byte{0, 6}}, r.Skipped)
	r.Exit()
	assert.Equal(t, []atomlayer.Atom{[]byte{248, 3}, []byte{0, 6}}, r.Skipped)
	r.Exit()
	assert.Equal(t, []atomlayer.Atom{[]byte{248, 3}, []byte{0, 6}}, r.Skipped)
}

func TestSkipNestedBags(t *testing.T) {
	baggage := []atomlayer.Atom{
		[]byte{248, 3},
		[]byte{0, 7},
		[]byte{0, 100},
		[]byte{240, 0},
		[]byte{0, 6},
		[]byte{248, 5},
		[]byte{0, 7},
	}

	r := Read(baggage)

	assert.True(t, r.EnterIndexed(5))
	assert.Equal(t,
		[]atomlayer.Atom{[]byte{248, 3}, []byte{0, 7}, []byte{0, 100}, []byte{240, 0}, []byte{0, 6}},
		r.Skipped)
	r.Exit()
	r.Exit()
	assert.Equal(t,
		[]atomlayer.Atom{[]byte{248, 3}, []byte{0, 7}, []byte{0, 100}, []byte{240, 0}, []byte{0, 6}},
		r.Skipped)
}

func TestSkippedAtomsPartial(t *testing.T) {
	baggage := []atomlayer.Atom{
		[]byte{248, 3},
		[]byte{240, 0},
		[]byte{0, 6},
	}

	r := Read(baggage)

	assert.True(t, r.EnterIndexed(3))
	r.Exit()

	assert.Equal(t, []atomlayer.Atom{[]byte{248, 3}, []byte{240, 0}, []byte{0, 6}}, r.Skipped)
	r.Exit()
	r.Exit()
	assert.Equal(t, []atomlayer.Atom{[]byte{248, 3}, []byte{240, 0}, []byte{0, 6}}, r.Skipped)
}

func TestSkippedAtomsDropsInitialDataAtoms(t *testing.T) {
	baggage := []atomlayer.Atom{
		[]byte{248, 3},
		[]byte{0, 7},
		[]byte{0, 100},
		[]byte{240, 0},
		[]byte{0, 6},
	}

	r := Read(baggage)

	assert.True(t, r.EnterIndexed(3))
	r.Exit()

	assert.Equal(t, []atomlayer.Atom{[]byte{248, 3}, []byte{240, 0}, []byte{0, 6}}, r.Skipped)
	r.Exit()
	r.Exit()
	assert.Equal(t, []atomlayer.Atom{[]byte{248, 3}, []byte{240, 0}, []byte{0, 6}}, r.Skipped)
}

func TestMultipleSkippedAtoms(t *testing.T) {
	baggage := []atomlayer.Atom{
		header(0, 3),
			data(7),
			data(100),
			header(1, 0),
				data(6),
			header(1, 3),
				data(15),
		header(0, 4),
			data(2),
			header(1,0),
				data(20),
		header(0,5),
			data(2),
			data(11),
			header(1, 1000000),
				data(15),
		header(0, 10000001),
			data(5,5,5,5,5),
	}

	r := Read(baggage)

	assert.True(t, r.EnterIndexed(3))
	assert.NotNil(t, r.Next())
	assert.True(t, r.EnterIndexed(0))
	r.Exit()
	r.Exit()

	assert.Equal(t, atoms(
		header(0,3),
			header(1, 3),
				data(15),
	), r.Skipped)

	assert.True(t, r.EnterIndexed(5))
	assert.Equal(t, atoms(
		header(0,3),
			header(1, 3),
				data(15),
		header(0, 4),
			data(2),
			header(1, 0),
				data(20),
	), r.Skipped)
	r.Exit()
	assert.Equal(t, atoms(
		header(0,3),
			header(1, 3),
				data(15),
		header(0, 4),
			data(2),
			header(1, 0),
				data(20),
		header(0, 5),
			header(1, 1000000),
				data(15),
	), r.Skipped)

	r.Close()
	assert.Equal(t, atoms(
		header(0,3),
			header(1, 3),
				data(15),
		header(0, 4),
			data(2),
			header(1, 0),
				data(20),
		header(0, 5),
			header(1, 1000000),
				data(15),
		header(0, 10000001),
			data(5,5,5,5,5),
		), r.Skipped)
}

func TestFailedEnterSkipsSomeAtoms(t *testing.T) {
	baggage := []atomlayer.Atom{
		[]byte{248, 3},
		[]byte{0, 7},
		[]byte{248, 5},
		[]byte{0, 9},
	}

	r := Read(baggage)

	assert.False(t, r.EnterIndexed(4))
	assert.Equal(t, []atomlayer.Atom{[]byte{248, 3}, []byte{0, 7}}, r.Skipped)
	assert.False(t, r.EnterIndexed(6))
	assert.Equal(t, []atomlayer.Atom{[]byte{248, 3}, []byte{0, 7}, []byte{248, 5}, []byte{0, 9}}, r.Skipped)
}

func TestEnterKeyed(t *testing.T) {
	baggage := []atomlayer.Atom{
		keyed(0, "hello"),
		data(10),
	}

	r := Read(baggage)

	assert.True(t, r.EnterKeyed([]byte("hello")))
	assert.Equal(t, []byte{10}, r.Next())
	assert.Equal(t, []atomlayer.Atom{keyed(0, "hello")}, r.currentPath)
}

func TestEnterSkips(t *testing.T) {
	baggage := []atomlayer.Atom{
		header(0, 1),
			data(7),
		header(0, 2),
			data(7),
		header(0, 3),
			data(7),
	}

	r := Read(baggage)

	assert.True(t, r.EnterIndexed(3))
	assert.Equal(t, atoms(
		header(0, 1),
			data(7),
		header(0, 2),
			data(7),
	), r.Skipped)
	r.Exit()
	assert.Equal(t, atoms(
		header(0, 1),
			data(7),
		header(0, 2),
			data(7),
	), r.Skipped)
}

func TestEnterKeyedSkipsIndexed(t *testing.T) {
	baggage := []atomlayer.Atom{
		header(0, 3),
			data(7),
		keyed(0, "hello"),
			data(10),
		keyed(0, "jon"),
			data(15),
	}

	r := Read(baggage)

	assert.True(t, r.EnterKeyed([]byte("jon")))
	assert.Equal(t, atoms(
		header(0, 3),
			data(7),
		keyed(0, "hello"),
			data(10),
	), r.Skipped)
	r.Exit()
	assert.Equal(t, atoms(
		header(0, 3),
			data(7),
		keyed(0, "hello"),
			data(10),
	), r.Skipped)

}

func TestClose(t *testing.T) {
	r := Read([]atomlayer.Atom{})
	r.Close()
}

func TestClose2(t *testing.T) {
	r := Open([]atomlayer.Atom{}, 2)
	r.Close()
}

func TestCloseInBag(t *testing.T) {
	r := Read([]atomlayer.Atom{})
	r.Close()
}

func TestCloseSkipsAtoms(t *testing.T) {
	baggage := []atomlayer.Atom{
		[]byte{248, 3},
			[]byte{0, 7},
			[]byte{0, 100},
		[]byte{240, 0},
			[]byte{0, 6},
		[]byte{248, 5},
			[]byte{0, 10},
	}

	r := Read(baggage)

	assert.True(t, r.EnterIndexed(3))
	r.Exit()

	assert.Equal(t, []atomlayer.Atom{[]byte{248, 3}, []byte{240, 0}, []byte{0, 6}}, r.Skipped)
	r.Close()
	assert.Equal(t,
		[]atomlayer.Atom{[]byte{248, 3}, []byte{240, 0}, []byte{0, 6}, []byte{248, 5}, []byte{0, 10}},
		r.Skipped)
}

func TestTrimMarker(t *testing.T) {
	baggage := []atomlayer.Atom{
		header(0, 3),
			data(7),
			[]byte{},
			data(10),
		keyed(0, "hello"),
			data(15),
	}

	r := Read(baggage)

	assert.True(t, r.EnterIndexed(3))
	assert.False(t, r.Overflowed)

	assert.Equal(t, []byte{7}, r.Next())
	assert.False(t, r.Overflowed)

	assert.Equal(t, []byte{10}, r.Next())
	assert.True(t, r.Overflowed)

	r.Exit()
	assert.True(t, r.Overflowed)

	assert.True(t, r.EnterKeyed([]byte("hello")))
	assert.True(t, r.Overflowed)

	assert.Equal(t, []byte{15}, r.Next())
	assert.True(t, r.Overflowed)

	r.Close()
	assert.True(t, r.Overflowed)
}

func TestTrimMarker2(t *testing.T) {
	baggage := []atomlayer.Atom{
		header(0, 3),
			data(7),
			[]byte{},
			data(10),
		keyed(0, "hello"),
			data(15),
	}

	r := Read(baggage)

	assert.False(t ,r.Overflowed)
	assert.True(t, r.EnterKeyed([]byte("hello")))
	assert.True(t, r.Overflowed)
	assert.Equal(t, atoms(
		header(0, 3),
			data(7),
			[]byte{},
			data(10),
	), r.Skipped)

	assert.Equal(t, []byte{15}, r.Next())
	assert.True(t, r.Overflowed)

	r.Close()
	assert.True(t, r.Overflowed)
}

func TestDropDuplicateTrimMarkers(t *testing.T) {
	baggage := []atomlayer.Atom{
		header(0, 3),
			data(7),
			[]byte{},
			[]byte{},
			data(10),
			[]byte{},
		keyed(0, "hello"),
			[]byte{},
			data(15),
	}

	r := Read(baggage)

	assert.False(t ,r.Overflowed)
	assert.True(t, r.EnterKeyed([]byte("hello")))
	assert.True(t, r.Overflowed)
	assert.Equal(t, atoms(
		header(0, 3),
		data(7),
		[]byte{},
		data(10),
	), r.Skipped)

	assert.Equal(t, []byte{15}, r.Next())
	assert.True(t, r.Overflowed)

	r.Close()
	assert.True(t, r.Overflowed)
}

func TestSimpleXTraceBaggage(t *testing.T) {
	baggage := []byte{	2, 248, 5,
							2, 240, 0,
								9, 0, 131, 154, 212, 173, 65, 53, 70, 55,
							2, 240, 1,
								9, 0, 185, 124, 187, 14, 103, 240, 88, 153 }

	// bag is registered to index 5
	//
	// bag XTraceBaggage {
	//	fixed64 task_id = 0;
	//	set<fixed64> parent_ids = 1;
	//	fixed64 discovery_id = 2;
	//	int32 logging_level = 3;
	//}

	xtraceBagIndex := uint64(5)
	taskIdIndex := uint64(0)
	parentEventIdIndex := uint64(1)

	atoms, err := atomlayer.Deserialize(baggage);
	assert.Nil(t, err)
	assert.Equal(t, 5, len(atoms))

	reader := Read(atoms)
	assert.Nil(t, reader.Next())
	assert.Equal(t, 0, len(reader.currentPath))

	assert.True(t, reader.EnterIndexed(xtraceBagIndex))
	assert.Equal(t, 1, len(reader.currentPath))
	assert.Equal(t, []atomlayer.Atom{[]byte{248, 5}}, reader.currentPath)

	assert.True(t, reader.EnterIndexed(taskIdIndex))
	assert.Equal(t, 2, len(reader.currentPath))
	assert.Equal(t, []atomlayer.Atom{[]byte{248, 5}, []byte{240, 0}}, reader.currentPath)

	var taskId int64
	err = binary.Read(bytes.NewReader(reader.Next()), binary.BigEndian, &taskId)
	assert.Nil(t, err)
	assert.Equal(t, int64(-8963618267739109833), taskId)
	assert.Nil(t, reader.Next())
	assert.Nil(t, reader.Enter())


	assert.Equal(t, 1, reader.level)
	reader.Exit()
	assert.Equal(t, 0, reader.level)
	assert.Equal(t, 1, len(reader.currentPath))
	assert.Equal(t, []atomlayer.Atom{[]byte{248, 5}}, reader.currentPath)

	assert.Nil(t, reader.Next())
	assert.True(t, reader.EnterIndexed(parentEventIdIndex))
	assert.Equal(t, 2, len(reader.currentPath))
	assert.Equal(t, []atomlayer.Atom{[]byte{248, 5}, []byte{240, 1}}, reader.currentPath)

	var parentId int64
	err = binary.Read(bytes.NewReader(reader.Next()), binary.BigEndian, &parentId)
	assert.Nil(t, err)
	assert.Equal(t, int64(-5080980609033021287), parentId)
	assert.Nil(t, reader.Next())
	assert.Nil(t, reader.Enter())

	assert.Equal(t, 1, reader.level)
	reader.Exit()
	assert.Equal(t, 0, reader.level)
	assert.Equal(t, 1, len(reader.currentPath))
	assert.Equal(t, []atomlayer.Atom{[]byte{248, 5}}, reader.currentPath)

	assert.Nil(t, reader.Next())
	assert.Nil(t, reader.Enter())

	assert.Equal(t, 0, reader.level)
	reader.Exit()
	assert.Equal(t, -1, reader.level)
	assert.Equal(t, 0, len(reader.currentPath))
	assert.Equal(t, []atomlayer.Atom{}, reader.currentPath)
	assert.Nil(t, reader.Next())
	assert.Nil(t, reader.Enter())
}

func TestSimpleXTraceBaggage2(t *testing.T) {
	baggage := []byte{	2, 248, 5,
							2, 240, 0,
								9, 0, 143, 189, 154, 1, 65, 170, 219, 47,
							2, 240, 1,
								9, 0, 242, 64, 253, 113, 224, 239, 96, 55,
								9, 0, 2, 62, 33, 56, 120, 22, 229, 128,
								9, 0, 125, 152, 88, 29, 177, 134, 140, 248,
							2, 240, 3,
								2, 0, 3 }

	// bag is registered to index 5
	//
	// bag XTraceBaggage {
	//	fixed64 task_id = 0;
	//	set<fixed64> parent_ids = 1;
	//	fixed64 discovery_id = 2;
	//	int32 logging_level = 3;
	//}

	xtraceBagIndex := uint64(5)
	taskIdIndex := uint64(0)
	parentEventIdIndex := uint64(1)
	loggingLevelIndex := uint64(3)

	atoms, err := atomlayer.Deserialize(baggage);
	assert.Nil(t, err)
	assert.Equal(t, 9, len(atoms))

	reader := Read(atoms)
	assert.Nil(t, reader.Next())
	assert.True(t, reader.EnterIndexed(xtraceBagIndex))
	assert.True(t, reader.EnterIndexed(taskIdIndex))

	var taskId int64
	err = binary.Read(bytes.NewReader(reader.Next()), binary.BigEndian, &taskId)
	assert.Nil(t, err)
	assert.Equal(t, int64(-8089140025500181713), taskId)
	assert.Nil(t, reader.Next())
	assert.Nil(t, reader.Enter())

	reader.Exit()
	assert.Nil(t, reader.Next())
	assert.True(t, reader.EnterIndexed(parentEventIdIndex))

	var parentId int64
	err = binary.Read(bytes.NewReader(reader.Next()), binary.BigEndian, &parentId)
	assert.Nil(t, err)
	assert.Equal(t, int64(-990513252474593225), parentId)
	err = binary.Read(bytes.NewReader(reader.Next()), binary.BigEndian, &parentId)
	assert.Nil(t, err)
	assert.Equal(t, int64(161603163048568192), parentId)
	err = binary.Read(bytes.NewReader(reader.Next()), binary.BigEndian, &parentId)
	assert.Nil(t, err)
	assert.Equal(t, int64(9050080335756692728), parentId)
	assert.Nil(t, reader.Next())
	assert.Nil(t, reader.Enter())

	reader.Exit()
	assert.Nil(t, reader.Next())
	assert.True(t, reader.EnterIndexed(loggingLevelIndex))

	loggingLevel, bytesRead := proto.DecodeVarint(reader.Next())
	assert.Equal(t, 1, bytesRead)
	assert.Equal(t, uint64(3), loggingLevel)

	reader.Exit()
	assert.Nil(t, reader.Next())
	assert.Nil(t, reader.Enter())

	reader.Exit()
	assert.Nil(t, reader.Next())
	assert.Nil(t, reader.Enter())
}

func TestOpenBag(t *testing.T) {
	baggage := []atomlayer.Atom{
		header(0, 3),
			data(7),
			data(100),
			header(1, 0),
				data(6),
			header(1, 3),
				data(15),
		header(0, 4),
			data(2),
			header(1,0),
				data(20),
		header(0,5),
			data(2),
			data(11),
			[]byte{},
			header(1, 1000000),
				data(15),
		header(0, 10000001),
			data(5,5,5,5,5),
	}

	r := Open(baggage, 4)

	assert.Equal(t, 0, r.level)
	assert.Equal(t, 0, len(r.Skipped))
	assert.Equal(t, 0, len(r.currentPath))
	assert.False(t, r.Overflowed)

	assert.Equal(t, []byte{2}, r.Next())
	assert.True(t, r.EnterIndexed(0))
	assert.Equal(t, []byte{20}, r.Next())
	assert.Nil(t, r.Next())
	r.Exit()
	assert.Nil(t, r.Error())
	assert.Nil(t, r.Next())
	assert.Nil(t, r.Enter())

	r.Exit()
	assert.NotNil(t, r.Error())
}

func TestOpenBagOverflow(t *testing.T) {
	baggage := atoms(
		header(0, 0),
			data(7),
			data(100),
			[]byte{},
		header(0, 3),
			data(6),
	)

	r := Open(baggage, 3)
	assert.True(t, r.Overflowed)

	r = Open(baggage, 0)
	assert.False(t, r.Overflowed)

	baggage = atoms(
		header(0, 0),
			data(7),
			data(100),
		header(0, 3),
			[]byte{},
			data(6),
	)

	r = Open(baggage, 3)
	assert.False(t, r.Overflowed)

	r = Open(baggage, 0)
	assert.False(t, r.Overflowed)
}