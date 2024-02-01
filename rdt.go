package main

import "github.com/learn-decentralized-systems/toytlv"

// RDT is a Replicated Data Type.
// In the Chotki model, that is a type of an object's field.
// On every mutation, it issues a serialized change to be
// applied by all remote replicas.
type RDT interface {
	// Apply applies state/mutation records
	Apply(changes []byte)
	// Diff generates a mutation record given the previous
	// state of the field for reference
	Diff(state []byte) (changes []byte)
}

func MakeObjectPacket(id, typedecl ID, states ...[]byte) (packet []byte) {
	packet = toytlv.Append(packet, 'I', id.ZipBytes())
	packet = toytlv.Append(packet, 'R', typedecl.ZipBytes())
	for i, state := range states {
		ref := MakeID(0, 0, uint16(i+1))
		packet = toytlv.Append(packet, 'R', ref.ZipBytes())
		packet = append(packet, state...)
	}
	return packet
}
