package baggageprotocol

import (
	"testing"
	"github.com/stretchr/testify/assert"
	"github.com/tracingplane/tracingplane-go/atomlayer"
	"encoding/binary"
	"bytes"
	"github.com/gogo/protobuf/proto"
)


func TestSliceEqual(t *testing.T) {
	assert.NotEqual(t, make([]byte, 0), nil)
	assert.NotEqual(t, nil, make([]byte, 0))
	assert.False(t, make([]byte, 0) == nil)

	arr := []byte{0}
	assert.NotEqual(t, arr[1:], nil)
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

	reader := ReadBaggageAtoms(atoms)
	assert.Nil(t, reader.Next())
	assert.True(t, reader.EnterIndexed(xtraceBagIndex))
	assert.True(t, reader.EnterIndexed(taskIdIndex))

	var taskId int64
	err = binary.Read(bytes.NewReader(reader.Next()), binary.BigEndian, &taskId)
	assert.Nil(t, err)
	assert.Equal(t, int64(-8963618267739109833), taskId)
	assert.Nil(t, reader.Next())
	assert.Nil(t, reader.Enter())

	reader.Exit()
	assert.Nil(t, reader.Next())
	assert.True(t, reader.EnterIndexed(parentEventIdIndex))

	var parentId int64
	err = binary.Read(bytes.NewReader(reader.Next()), binary.BigEndian, &parentId)
	assert.Nil(t, err)
	assert.Equal(t, int64(-5080980609033021287), parentId)
	assert.Nil(t, reader.Next())
	assert.Nil(t, reader.Enter())

	reader.Exit()
	assert.Nil(t, reader.Next())
	assert.Nil(t, reader.Enter())

	reader.Exit()
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

	reader := ReadBaggageAtoms(atoms)
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