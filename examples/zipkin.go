package examples

import (
	"github.com/tracingplane/tracingplane-go/bdl"
	"github.com/tracingplane/tracingplane-go/baggageprotocol"
	"github.com/tracingplane/tracingplane-go/atomlayer"
	"sort"
)

// An example of a class that would be generated by BDL for Zipkin

type ZipkinMetadata struct {
	traceID			*int64				// sfixed64 traceID = 0;
	spanID			*int64				// sfixed64 spanID = 1;
	parentSpanID	*int64				// sfixed64 parentSpanID = 2;
	sampled			*bool				// taint sampled = 3;
	tags			map[string](string) // map<string, string> tags = 4;
	overflowed 		bool
	unknown			[]atomlayer.Atom
}


func (zipkinMetadata *ZipkinMetadata) Read(r *baggageprotocol.Reader) {
	// traceID
	if r.EnterIndexed(0) {
		zipkinMetadata.traceID = bdl.ReadInt64Fixed(r.Next());
		r.Exit()
	}

	// spanID
	if r.EnterIndexed(1) {
		zipkinMetadata.spanID = bdl.ReadInt64Fixed(r.Next());
		r.Exit()
	}

	// parentSpanID
	if r.EnterIndexed(2) {
		zipkinMetadata.parentSpanID = bdl.ReadInt64Fixed(r.Next());
		r.Exit()
	}

	// sampled
	if r.EnterIndexed(3) {
		zipkinMetadata.sampled = bdl.ReadTaint(r.Next())
		r.Exit()
	}

	// tags
	if r.EnterIndexed(4) {
		zipkinMetadata.tags = make(map[string](string))
		for {
			key := r.Enter()
			if key == nil { break }

			value := r.Next()
			if value != nil {
				tagsKey := string(key[1:])
				tagsValue := string(value)
				zipkinMetadata.tags[tagsKey] = tagsValue
			}
			r.Exit()
		}
	}

	// Overflow
	zipkinMetadata.overflowed = r.Overflowed
}

func (zipkinMetadata *ZipkinMetadata) Write(w *baggageprotocol.Writer) {
	// traceID
	if zipkinMetadata.traceID != nil {
		w.Enter(0)
		w.Write(bdl.WriteInt64Fixed(*zipkinMetadata.traceID))
		w.Exit()
	}

	// spanID
	if zipkinMetadata.spanID != nil {
		w.Enter(1)
		w.Write(bdl.WriteInt64Fixed(*zipkinMetadata.spanID))
		w.Exit()
	}

	// parentSpanID
	if zipkinMetadata.parentSpanID != nil {
		w.Enter(2)
		w.Write(bdl.WriteInt64Fixed(*zipkinMetadata.parentSpanID))
		w.Exit()
	}

	// sampled
	if zipkinMetadata.sampled != nil {
		w.Enter(3)
		w.Write(bdl.WriteTaint(*zipkinMetadata.sampled))
		w.Exit()
	}

	// tags
	if len(zipkinMetadata.tags) > 0 {
		var tagKeys []atomlayer.Atom
		for tagKey := range(zipkinMetadata.tags) {
			tagKeys = append(tagKeys, atomlayer.Atom(tagKey))
		}

		sort.Sort(baggageprotocol.LexicographicAtomSorter(tagKeys))

		for _,tagKey := range(tagKeys) {
			tagValue := []byte(zipkinMetadata.tags[string(tagKey)])
			w.EnterKey(tagKey)
			w.Write(tagValue)
			w.Exit()
		}
	}

	// Overflow
	if zipkinMetadata.overflowed {
		w.MarkOverflow()
	}
}

func (zipkinMetadata *ZipkinMetadata) SetUnprocessedAtoms(atoms []atomlayer.Atom) {
	zipkinMetadata.unknown = atoms
}

func (zipkinMetadata *ZipkinMetadata) GetUnprocessedAtoms() []atomlayer.Atom {
	return zipkinMetadata.unknown
}