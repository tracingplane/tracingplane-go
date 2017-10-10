package tracingplane

import (
	"encoding/base64"
	"github.com/tracingplane/tracingplane-go/atomlayer"
	"context"
	"math/rand"
)

// This file has the main declaration of the tracing plane's BaggageContext object, and the main API calls for
//   propagating it.
// BaggageContext uses an Atom representation for the main context data.
// BaggageContexts also carry a golang context.Context for convenience.  However, the merge behavior of context.Context
//  is unspecified, so when two contexts merge, we simply retain of the contexts and drop the other

// Provides the base declaration of BaggageContext which internally uses the atom layer's atom representation
type BaggageContext struct {
	atoms []atomlayer.Atom				// The underlying atoms of this baggagecontext
	Context context.Context				// A golang context carried with this baggagecontext.  Propagates through calls
										// to branch, but not with all merge calls.
	componentId **uint32				// A randomly generated ID for this component; only propagates to one side of
										// branch calls.
}


// Returns a new BaggageContext with the contents of A and B merged together.
// The returned BaggageContext will NOT contain anything from B's golang context -- only A's
func (a BaggageContext) MergeWith(bs ...BaggageContext) BaggageContext {
	for _, b := range(bs) {
		a.atoms = atomlayer.Merge(a.atoms, b.atoms)
		if !a.hasComponentID() {
			a.componentId = b.componentId
			// TODO: If multiple baggages have component IDs, keep all of them for later reuse?
		}
	}

	// Remove the component ID from whichever input baggage (a or one of the bs) it came from
	if a.hasComponentID() {
		componentId := **a.componentId
		componentIdAddr := &componentId
		*a.componentId = nil
		a.componentId = &componentIdAddr
	}

	return a
}

// Derives a new BaggageContext instance that will be passed, for example, to a different goroutine
func (a BaggageContext) Branch() (c BaggageContext) {
	a.atoms = atomlayer.Branch(a.atoms)
	a.componentId = nil
	return a
}

// Returns the serialized size in bytes of this BaggageContext
func (baggage BaggageContext) SerializedSize() int {
	return atomlayer.SerializedSize(baggage.atoms)
}

// Serializes the atoms of the BaggageContext.  The serialized representation doesn't include anything from the golang
// context, or the component ID
func Serialize(baggage BaggageContext) []byte {
	return atomlayer.Serialize(baggage.atoms)
}

// Deserializes a BaggageContext from bytes
func Deserialize(bytes []byte) (baggage BaggageContext, err error) {
	baggage.atoms, err = atomlayer.Deserialize(bytes)
	return
}

// Serializes the provided BaggageContext then base64 encodes it into a string
func EncodeBase64(baggage BaggageContext) string {
	return base64.StdEncoding.EncodeToString(Serialize(baggage))
}

// Decodes and deserializes a BaggageContext from the provided base64-encoded string
func DecodeBase64(encoded string) (BaggageContext, error) {
	bytes, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return BaggageContext{}, err
	} else {
		return Deserialize(bytes)
	}
}

// Drop atoms from the BaggageContext so that it fits into the specified number of bytes
func Trim(baggage BaggageContext, maxSize int) BaggageContext {
	baggage.atoms = atomlayer.Trim(baggage.atoms, maxSize)
	return baggage
}

func (baggage *BaggageContext) hasComponentID() bool {
	return baggage.componentId != nil && *baggage.componentId != nil
}

// This function exists because BDL datatypes such as counters use a randomly generated component ID to avoid concurrent
// modifications
func (baggage *BaggageContext) ComponentID() uint32 {
	if !baggage.hasComponentID() {
		baggage.componentId = newComponentID()
	}
	return **baggage.componentId
}

func newComponentID() **uint32{
	componentId := rand.Uint32()
	componentIdAddr := &componentId
	return &componentIdAddr
}