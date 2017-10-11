package tracingplane

import (
	"github.com/tracingplane/tracingplane-go/bdl"
	"github.com/tracingplane/tracingplane-go/baggageprotocol"
	"github.com/tracingplane/tracingplane-go/atomlayer"
)

// This file contains extra methods for the BaggageContext structs that are used by BDL-generated code to read and
// write atoms.

// Read the specified bag index into the provided bag object
func (baggage *BaggageContext) ReadBag(bagIndex uint64, bag bdl.Bag) error {
	reader := baggageprotocol.Open(baggage.Atoms, bagIndex)
	bag.Read(reader)
	reader.Close()
	bag.SetUnprocessedAtoms(reader.Skipped)
	return reader.Err
}

// Drops the specified bag index from the provided baggage object
func (baggage *BaggageContext) Drop(bagIndex uint64) {
	baggage.Atoms = baggageprotocol.Drop(baggage.Atoms, bagIndex, baggageprotocol.PushMarkerDown)
}

func (baggage *BaggageContext) Set(bagIndex uint64, bag bdl.Bag) error {
	// Remove the bag from the baggage
	baggage.Atoms = baggageprotocol.Drop(baggage.Atoms, bagIndex, baggageprotocol.DropMarker)

	// Write the new bag
	writer := baggageprotocol.WriteBag(bagIndex)
	bag.Write(writer)
	writer.AddUnprocessedAtoms(bag.GetUnprocessedAtoms())
	newAtoms, err := writer.Atoms()

	// Merge it back in
	baggage.Atoms = atomlayer.Merge(baggage.Atoms, newAtoms)
	return err
}