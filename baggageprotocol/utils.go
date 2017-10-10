package baggageprotocol

import (
	"github.com/tracingplane/tracingplane-go/atomlayer"
	"bytes"
)

// Provides useful methods for slicing and dicing Atoms




// Finds the specified atom, stopping at the first atom lexicographically larger than it.
// Returns:
// 		exists - true if the atom was found, false otherwise
//		overflowed - true if the overflow marker was found between startat and i, false otherwise
// 		i - the index of the match, or insertion index if not found
func find(atoms []atomlayer.Atom, startat int, target atomlayer.Atom) (exists bool, overflowed bool, i int) {
	for i=startat; i<len(atoms); i++ {
		overflowed = overflowed || atomlayer.IsTrimMarker(atoms[i])

		switch bytes.Compare(atoms[i], target) {
		case -1: continue							// Haven't encountered yet
		case 0:  exists = true; return				// Found it
		case 1:  exists = false; return				// Went past it
		}
	}
	return
}

// Gets the fully qualified path to the overflow marker, if it exists in the atoms
func overflowPath(atoms []atomlayer.Atom) []atomlayer.Atom {
	// TODO
	return nil
}

// Enum specifies what to do with overflow markers if they exist in a bag about to be dropped
type OverflowMarkerBehavior int
const (
	DropMarker OverflowMarkerBehavior = iota	// Just drop the marker entirely; usually used if we're about to merge
												// an update back into the atoms (so we won't actually lose the marker)
	RetainMarkerPosition						// Keep the fully qualified headers marking the exact marker position
	PushMarkerDown								// Push the marker down to the root
)

// Drops the specified bag from the provided atoms.  Can also specify what to do with any overflow markers
// we find.
func Drop(atoms []atomlayer.Atom, bagIndex uint64, overflow OverflowMarkerBehavior) []atomlayer.Atom {
	target := MakeIndexedHeader(0, bagIndex)
	exists, _, i := find(atoms, 0, target)

	if !exists { return atoms }

	_,_,j := find(atoms, i+1, target)
	// TODO: implement choice of overflow behavior; current behavior is Drop
	var r []atomlayer.Atom
	r = append(r, atoms[:i]...)
	r = append(r, atoms[j:]...)
	return r
}

type LexicographicAtomSorter []atomlayer.Atom

func (a LexicographicAtomSorter) Len() int           { return len(a) }
func (a LexicographicAtomSorter) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a LexicographicAtomSorter) Less(i, j int) bool { return bytes.Compare(a[i], a[j]) == -1 }