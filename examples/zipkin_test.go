package examples

import (
	"github.com/stretchr/testify/assert"
	"github.com/tracingplane/tracingplane-go/tracingplane"
	"testing"
	"github.com/tracingplane/tracingplane-go/atomlayer"
	"fmt"
)

func TestZipkin(t *testing.T) {
	var baggage tracingplane.BaggageContext
	baggage.Atoms = []atomlayer.Atom{
		{248,2},
		{240,0},
		{0,0,0,0,0,0,0,0,55},
		{240,1},
		{0,0,0,0,0,0,0,0,70},
		{240,2},
		{0,0,0,0,0,0,0,0,10},
	}

	zmd := ZipkinMetadata{}
	err := baggage.ReadBag(2, &zmd)

	assert.Nil(t, err)
	assert.Empty(t, zmd.unknown)
	assert.False(t, zmd.overflowed)
	assert.NotNil(t, zmd.traceID)
	assert.Equal(t, int64(55), *zmd.traceID)
	assert.NotNil(t, zmd.spanID)
	assert.Equal(t, int64(70), *zmd.spanID)
	assert.NotNil(t, zmd.parentSpanID)
	assert.Equal(t, int64(10), *zmd.parentSpanID)

	var baggage2 tracingplane.BaggageContext
	baggage2.Set(2, &zmd)

	assert.Equal(t, baggage.Atoms, baggage2.Atoms)
}