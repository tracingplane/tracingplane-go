package bdl

import (
	"github.com/tracingplane/tracingplane-go/baggageprotocol"
	"github.com/tracingplane/tracingplane-go/atomlayer"
)

type Bag interface {
	Read(r *baggageprotocol.Reader)
	Write(w *baggageprotocol.Writer)
	SetUnprocessedAtoms(atoms []atomlayer.Atom)
}