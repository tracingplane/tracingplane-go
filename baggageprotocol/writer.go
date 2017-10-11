package baggageprotocol

import (
	"github.com/tracingplane/tracingplane-go/atomlayer"
	"bytes"
	"fmt"
	"sort"
)

type Writer struct {
	finalized	[]atomlayer.Atom
	atoms		[]atomlayer.Atom
	prev		atomlayer.Atom		// Previous header
	basePath	[]atomlayer.Atom
	currentPath []atomlayer.Atom
	level 		int
	err 		error
	overflowed 	bool
}

func NewWriter() *Writer {
	return write()
}

// Writes to a specific bag
func WriteBag(bagIndex uint64) *Writer {
	return write(MakeIndexedHeader(0, bagIndex))
}

// Returns a writer that writes data starting at the provided path
func write(basePath ...atomlayer.Atom) *Writer {
	var w Writer
	w.level = len(basePath) - 1
	w.basePath = basePath
	w.prev = nil
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
	w.prev = nil
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
	w.atoms = append(w.atoms, atoms...)
}

func (w *Writer) MarkOverflow() {
	if !w.overflowed {
		w.overflowed = true
		w.atoms = append(w.atoms, atomlayer.TrimMarker)
	}
}

func (w *Writer) AddUnprocessedAtoms(atoms []atomlayer.Atom) {
	w.finalized = atomlayer.Merge(w.finalized, atoms)
}

func (w *Writer) Atoms() ([]atomlayer.Atom, error) {
	atoms := make([]atomlayer.Atom, 0, len(w.basePath) + len(w.atoms) + len(w.finalized))
	return append(append(atoms, w.basePath...), atomlayer.Merge(w.atoms, w.finalized)...), w.err
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