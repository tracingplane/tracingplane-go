package baggageprotocol

import (
	"github.com/tracingplane/tracingplane-go/atomlayer"
	"bytes"
	"fmt"
	"sort"
)

type Writer struct {
	atoms		[]atomlayer.Atom
	prev		atomlayer.Atom		// Previous header
	currentPath []atomlayer.Atom
	level 		int
	err 		error
	overflowed 	bool
}

var emptyAtom = []byte{}

func NewWriter() *Writer {
	var w Writer
	w.level = -1
	w.prev = emptyAtom
	return &w
}

// Writes to a specific bag
func WriteBag(bagIndex uint64) *Writer {
	var w Writer
	w.atoms = append(w.atoms, MakeIndexedHeader(0, bagIndex))
	w.prev = emptyAtom
	return &w
}

func (w *Writer) Enter(bagIndex uint64) {
	w.enter(MakeIndexedHeader(w.level+1, bagIndex))
}

func (w *Writer) EnterKey(bagKey []byte) {
	w.enter(MakeKeyedHeader(w.level+1, bagKey))
}

func (w *Writer) enter(header atomlayer.Atom) {
	// Make sure we're writing bags in ascending index order, as well as indices before keys
	switch bytes.Compare(w.prev, header) {
	case 0: w.seterror(duplicateEnter())
	case 1: w.seterror(outOfOrderEnter())
	}

	// Always write the header, even if it's in an erroneous order
	w.atoms = append(w.atoms, header)
	w.prev = emptyAtom
	w.currentPath = append(w.currentPath, header)
	w.level++
}

func (w *Writer) Exit() {
	if len(w.currentPath) == 0 {
		w.seterror(mismatchedEnterExit())
	} else {
		w.prev = w.currentPath[len(w.currentPath)-1]
		w.currentPath = w.currentPath[:len(w.currentPath)-1]
		w.level--

		if bytes.Equal(w.atoms[len(w.atoms)-1], w.prev) {
			w.atoms = w.atoms[:len(w.atoms)-1]
		}
	}

}

func (w *Writer) Write(data []byte) {
	w.atoms = append(w.atoms, MakeDataAtom(data))
}

func (w *Writer) WriteSorted(datas ...[]byte) {
	atoms := make([]atomlayer.Atom, 0, len(datas))
	for _,data := range(datas) {
		atoms = append(atoms, MakeDataAtom(data))
	}
	sort.Sort(LexicographicAtomSorter(atoms))
}

func (w *Writer) MarkOverflow() {
	if !w.overflowed {
		w.overflowed = true
		w.atoms = append(w.atoms, emptyAtom)
	}
}

func (w *Writer) Atoms() ([]atomlayer.Atom, error) {
	return w.atoms, w.err
}

func (w *Writer) seterror(err error) error {
	if w.err == nil {
		w.err = err		// Save the first error encountered
	}
	return err
}

func mismatchedEnterExit() error {
	return fmt.Errorf("Exit called more times than Enter; each Enter call must be matched with only one Exit")
}

func outOfOrderEnter() error {
	return fmt.Errorf("Bags must be written in order, ie. ascending by index, followed by keys in lexorder")
}

func duplicateEnter() error {
	return fmt.Errorf("Bags cannot be written to more than once")
}