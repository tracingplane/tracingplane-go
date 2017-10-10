package tracingplane

import (
	"github.com/tracingplane/tracingplane-go/bdl"
	"github.com/tracingplane/tracingplane-go/baggageprotocol"
	"github.com/tracingplane/tracingplane-go/atomlayer"
)

// BDL methods for reading and writing from and to BaggageContexts

// Read the specified bag index into the provided bag object
func (baggage *BaggageContext) ReadBag(bagIndex uint64, bag bdl.Bag) error {
	reader := baggageprotocol.Open(baggage.atoms, bagIndex)
	bag.Read(reader)
	reader.Close()
	bag.SetUnprocessedAtoms(reader.Skipped)
	return reader.Err
}

// Drops the specified bag index from the provided baggage object
func (baggage *BaggageContext) Drop(bagIndex uint64) {
	baggage.atoms = baggageprotocol.Drop(baggage.atoms, bagIndex, baggageprotocol.PushMarkerDown)
}

func (baggage *BaggageContext) WriteBag(bagIndex uint64, bag bdl.Bag) error {
	// Remove the bag from the baggage
	baggage.atoms = baggageprotocol.Drop(baggage.atoms, bagIndex, baggageprotocol.DropMarker)

	// Write the new bag
	writer := baggageprotocol.WriteBag(bagIndex)
	bag.Write(writer)
	newAtoms, err := writer.Atoms()

	// Merge it back in
	baggage.atoms = atomlayer.Merge(baggage.atoms, newAtoms)
	return err
}