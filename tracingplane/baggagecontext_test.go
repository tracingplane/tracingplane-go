package tracingplane

import (
	"testing"
	"github.com/stretchr/testify/assert"
	"math/rand"
)

func TestEmptyBaggage(t *testing.T) {
	var a BaggageContext

	assert.Empty(t, a.atoms)
	assert.Nil(t, a.componentId)

	var b BaggageContext

	c := a.MergeWith(b)

	assert.Empty(t, c.atoms)
	assert.Nil(t, c.componentId)

	d := c.Branch()

	assert.Empty(t, d.atoms)
	assert.Nil(t, d.componentId)
}

func TestComponentId(t *testing.T) {
	var a BaggageContext

	rand.Seed(0)

	assert.False(t, a.hasComponentID())
	assert.Equal(t, uint32(4059586549), a.ComponentID())			// First RNG number
	assert.True(t, a.hasComponentID())
	assert.Equal(t, uint32(4059586549), **a.componentId)
	assert.Equal(t, uint32(4059586549), a.ComponentID())
	assert.Equal(t, uint32(4059586549), **a.componentId)

	b := a.Branch()

	assert.True(t, a.hasComponentID())
	assert.Equal(t, uint32(4059586549), a.ComponentID())
	assert.False(t, b.hasComponentID())
	assert.Equal(t, uint32(1052117029), b.ComponentID())			// Second RNG number
	assert.True(t, b.hasComponentID())
	assert.Equal(t, uint32(1052117029), **b.componentId)
	assert.Equal(t, uint32(1052117029), b.ComponentID())
	assert.Equal(t, uint32(1052117029), **b.componentId)

	c := a.Branch()
	assert.Equal(t, uint32(4059586549), **a.componentId)
	assert.False(t, c.hasComponentID())

	d := a.Branch()
	assert.Equal(t, uint32(4059586549), **a.componentId)
	assert.False(t, d.hasComponentID())

	e := a.Branch()
	assert.Equal(t, uint32(4059586549), **a.componentId)
	assert.False(t, e.hasComponentID())

	f := a.MergeWith(b)

	assert.NotNil(t, a.componentId)
	assert.NotNil(t, b.componentId)
	assert.NotNil(t, f.componentId)
	assert.Nil(t, *a.componentId)
	assert.NotNil(t, *b.componentId)
	assert.NotNil(t, *f.componentId)
	assert.False(t, a.hasComponentID())
	assert.True(t, b.hasComponentID())
	assert.True(t, f.hasComponentID())
	assert.Equal(t, uint32(4059586549), **f.componentId)
	assert.Equal(t, uint32(1052117029), **b.componentId)

	g := d.MergeWith(e)
	assert.Nil(t, g.componentId)
	assert.Nil(t, d.componentId)
	assert.Nil(t, e.componentId)
	assert.False(t, g.hasComponentID())
	assert.False(t, d.hasComponentID())
	assert.False(t, e.hasComponentID())

	h := c.MergeWith(f)
	assert.Nil(t, c.componentId)
	assert.NotNil(t, h.componentId)
	assert.NotNil(t, f.componentId)
	assert.NotNil(t, *h.componentId)
	assert.Nil(t, *f.componentId)
	assert.False(t, c.hasComponentID())
	assert.False(t, f.hasComponentID())
	assert.True(t, h.hasComponentID())
	assert.Equal(t, uint32(4059586549), **h.componentId)
}