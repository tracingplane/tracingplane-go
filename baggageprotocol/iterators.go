package baggageprotocol

import (
	"github.com/tracingplane/tracingplane-go/atomlayer"
	"fmt"
	"bytes"
)

type reader struct {
	next       atomlayer.Atom
	currentPath []atomlayer.Atom
	remaining  atomlayer.BaggageContext
	skipped    atomlayer.BaggageContext
	level      int
	overflowed bool
	err        error
}

func Read(atoms atomlayer.BaggageContext) (r reader) {
	r.remaining = atoms
	r.level = -1
	r.advance()
	return
}

func Open(atoms atomlayer.BaggageContext, bagIndex uint64) (r reader) {
	// TODO: this
	return Read(atoms)
}

// Closes the reader, treating all remaining atoms as skipped
func (r *reader) Close() {
	// Exit any current bags
	for r.level >= 0 { r.Exit() }

	// Make sure we're not at data atoms
	r.advanceToNextHeader()

	// Remaining are skipped
	if r.next != nil {
		r.skipped = append(r.skipped, r.next)
		if len(r.remaining) > 0 {
			r.skipped = append(r.skipped, r.remaining...)
			r.remaining = nil
		}
		r.next = nil
	}
}

// Advances r.next zero or more atoms, until it's a header atom.  If it's already a header atom, does nothing.
// Returns the headeratom and its level.
func (r *reader) advanceToNextHeader() (atomlayer.Atom, int) {
	for {
		switch {
		case r.next == nil: 					goto noheader							// End of baggage or error
		case atomlayer.IsTrimMarker(r.next): 	r.overflowed = true; goto nextatom		// Handle overflow marker
		case IsHeader(r.next): 					goto foundheader 						// Found the next header atom
		case IsData(r.next): 					goto nextatom							// Skip any data atoms
		}

		foundheader:
		switch level, err := HeaderLevel(r.next); {
		case err != nil: 						r.seterror(err); goto noheader			// Cannot interpret the header
		default: 								return r.next, level					// Valid header; return it
		}

		noheader:
		return nil, -1

		nextatom:
		r.advance()
	}
}

// Advance into the next child bag of the next bag, if there is one; if there isn't, does nothing, and returns nil
func (r *reader) Enter() atomlayer.Atom {
	header, level := r.advanceToNextHeader()

	switch {
	case header == nil: 		goto exhausted 													// End of baggage/error
	case level <= r.level: 		goto exhausted													// Bag exhausted
	case level == r.level+1: 	goto found														// Found child bag
	default: 					r.seterror(invalidGrandchild(r.level, level)); goto exhausted	// Invalid jump >1 level
	}

	found:
	r.level++
	r.currentPath = append(r.currentPath, r.next)
	r.advance()
	return header

	exhausted:
	return nil
}

// Advance to the specified child bag, ignoring all preceding child bags, and stopping if we reach the end of bag
func (r *reader) EnterIndexed(index uint64) bool {
	return r.enter(MakeIndexedHeader(r.level + 1, index))
}

// Advance to the specified child bag, ignoring all preceding child bags, and stopping if we reach the end of bag
func (r *reader) EnterKeyed(key []byte) bool {
	return r.enter(MakeKeyedHeader(r.level + 1, key))
}

// Advance to provided header atom, ignoring all preceding child bags, and stopping if we reach the end of bag
func (r *reader) enter(target []byte) bool {
	for {
		header, level := r.advanceToNextHeader()

		// Check if parent/child
		switch {
		case header == nil: 		goto notfound				// End of baggage or error was encountered
		case level <= r.level: 		goto notfound				// Reached end of current bag
		case level > r.level+1: 	goto nextbag				// A descendent bag that we want to ignore
		}

		// Check sibling bag precedence
		switch bytes.Compare(header, target) {
		case -1:					goto nextbag				// A preceding sibling; bag can still appear later
		case 0: 					goto found					// Found the target bag; enter it and return
		case 1:						goto notfound				// We've advanced past where the bag would have been
		}

		nextbag:
		r.skipuntil(r.level+1);
		continue

		found:
		r.level++
		r.currentPath = append(r.currentPath, r.next)
		r.advance()
		return true

		notfound:
		return false
	}
}

// Skips bags, treating them as unprocessed, until we reach a bag at or below the specified level
func (r *reader) skipuntil(stopAtLevel int) {
	skippedAtoms := append(append([]atomlayer.Atom(nil), r.currentPath...), r.next)
	r.advance()
	for {
		// Non-header atoms
		switch {
		case r.next == nil:						goto finish							// End of baggage so we're done
		case atomlayer.IsTrimMarker(r.next):	goto trimmarker						// Include trim marker in skipped
		case IsData(r.next): 					goto skipatom 						// Skip all data atoms
		}

		// Header atoms
		switch level, err := HeaderLevel(r.next); {
		case err != nil: 						r.seterror(err); goto finish		// Invalid header, abort
		case level <= stopAtLevel:				goto finish							// End of the bag being skipped
		default:								goto skipatom						// A descendent bag; keep skipping
		}

		trimmarker:
		switch r.overflowed {
		case true: goto nextatom											// Ignore redundant trim marker
		case false: r.overflowed = true; goto skipatom								// First trim marker seen
		}

		skipatom:
		skippedAtoms = append(skippedAtoms, r.next);

		nextatom:
		r.advance()
	}

	finish:
	r.skipped = atomlayer.Merge(r.skipped, skippedAtoms);
}

// Advance to the end of the next bag and pop back up to the parent
func (r *reader) Exit() {
	for {
		switch header, level := r.advanceToNextHeader(); {
		case len(r.currentPath) == 0:	r.seterror(invalidExit()); return 		// Called exit too many times
		case header == nil: 			goto exit								// End of baggage or error encountered
		case level <= r.level:			goto exit								// Reached end of current bag
		case level > r.level: 			goto skipbag							// A descendent bag to ignore
		}

		exit:
		r.level--
		r.currentPath = r.currentPath[:len(r.currentPath)-1]
		return

		skipbag:
		r.skipuntil(r.level)
	}
}

// Reads the payload of the next data atom from the next bag.  Returns nil if there are no data atoms remaining
func (r *reader) Next() []byte {
	for {
		// Non-data atoms
		switch {
		case r.next == nil:						goto nodata								// End of baggage or an error
		case atomlayer.IsTrimMarker(r.next): 	r.overflowed = true; goto nextatom		// Trim marker, continue
		case !IsData(r.next): 					goto nodata								// Not a data atom
		}

		// Data atoms
		switch payload, err := Payload(r.next); {
		case err != nil: 						r.seterror(err); goto nodata			// Invalid data atom
		default: 								r.advance(); return payload				// Valid data atom
		}

		nodata:
		return nil

		nextatom:
		r.advance();
	}
}

// Returns the error if one occurred.  All operations stop after an error occurs
func (r *reader) Error() error {
	return r.err
}

func (r *reader) seterror(err error) error {
	if err != nil {
		r.err = err
		r.next = nil
	}
	return r.err
}

func (r *reader) advance() {
	switch {
	case r.err != nil: 							goto exhausted 							// Error occurred - stop
	case len(r.remaining) == 0: 				goto exhausted							// No atoms remaining
	default: 									goto advance							// Advance to next atom
	}

	advance:
	r.next = r.remaining[0]
	r.remaining = r.remaining[1:]
	return

	exhausted:
	r.next = nil
	return
}

func invalidGrandchild(currentLevel, childLevel int) error {
	return fmt.Errorf("Child bag jumped more than one level from %v to %v", currentLevel, childLevel)
}

func invalidExit() error {
	return fmt.Errorf("Exit called too many times without corresponding bag entries")
}