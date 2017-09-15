package baggageprotocol

import (
	"github.com/tracingplane/tracingplane-go/atomlayer"
	"fmt"
	"bytes"
)

type reader struct {
	current atomlayer.Atom
	remaining atomlayer.BaggageContext
	level int
	overflowed bool
	err error
}

func ReadBaggageBytes(serializedBaggageContext []byte) (r reader) {
	r.remaining, r.err = atomlayer.Deserialize(serializedBaggageContext)
	r.level = -1
	r.advance()
	return
}

func ReadBaggageAtoms(atoms atomlayer.BaggageContext) (r reader) {
	r.remaining = atoms
	r.level = -1
	r.advance()
	return
}

// Advances r.current zero or more atoms, until it's a header atom.  If it's already a header atom, does nothing.
// Returns the headeratom and its level.
func (r *reader) advanceToNextHeader() (atomlayer.Atom, int) {
	// Find a header atom if we're not already at one
	for r.current != nil && !IsHeader(r.current) {
		r.advance()
	}

	if r.current == nil { return nil, -1 }  // Reached end of baggage or error

	// Interpret the header level
	level, err := HeaderLevel(r.current);
	if err != nil {
		r.seterror(err)
		return nil, -1
	}

	return r.current, level
}

// Advance into the next child bag of the current bag, if there is one; if there isn't, does nothing, and returns nil
func (r *reader) Enter() atomlayer.Atom {
	switch header, level := r.advanceToNextHeader(); {
	case header == nil:       return nil                             // End of baggage or error was encountered
	case level <= r.level:    return nil                             // End of current bag
	case level == r.level+1:  r.level++; r.advance(); return header  // Found child bag 1 level deeper
	default: {                                                       // Unexpected grandchild 2 levels or deeper
		r.seterror(fmt.Errorf("Child bag jumped more than one level from %v to %v", r.level, level))
		return nil
	}}
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
		switch header, level := r.advanceToNextHeader(); {
		case header == nil:      return false              // End of baggage or error was encountered
		case level <= r.level:   return false              // End of current bag
		case level == r.level+1: {                         // Compare to child bag
			switch bytes.Compare(header, target) {
			case -1:  r.advance()                          // Found a preceding child; enter it and continue
			case 0:   r.level++; r.advance(); return true  // Found the target bag; enter it and return
			case 1:   return false                         // We've advanced past where the header would have been
			}
		}
		case level > r.level+1: r.advance()                // Ignore all descendent bags of current bag and continue
		}
	}
}

// Advance to the end of the current bag and pop back up to the parent
func (r *reader) Exit() {
	for {
		switch header, level := r.advanceToNextHeader(); {
		case header == nil:     r.level -= 1; return  // Reached end of baggage or error
		case level <= r.level:  r.level -= 1; return  // Successfully reached end of current bag
		case level > r.level:   r.advance();          // Ignore all descendent bags of current bag and continue
		}
	}
}

// Reads the payload of the next data atom from the current bag.  Returns nil if there are no data atoms remaining
func (r *reader) Next() []byte {
	if IsData(r.current) {
		payload, err := Payload(r.current)
		if err != nil {
			r.seterror(err)
			return nil
		}
		r.advance()
		return payload
	} else {
		return nil
	}
}

// Returns the error if one occurred.  All operations stop after an error occurs
func (r *reader) Error() error {
	return r.err
}

// Returns true or false if the

func (r *reader) seterror(err error) error {
	if err != nil {
		r.err = err
		r.current = nil
	}
	return r.err
}

func (r *reader) advance() {
	r.current = nil
	if r.err != nil { return }  // We're done once we encounter an error

	for len(r.remaining) > 0 {
		r.current = r.remaining[0]
		r.remaining = r.remaining[1:]

		switch atomlayer.IsTrimMarker(r.current) {
		case true: r.overflowed = true   // We overflowed here -- advance to next atom
		case false: return               // At a valid atom, continue
		}
	}
}