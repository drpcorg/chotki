package main

// Differ is an object or a part of an object that can
// accept/generate state changes.
type Differ interface {
	// Applies a state or a delta. The way the state
	// was generated was Diff()+Merge().
	Apply(state []byte)
	// Diff generates a delta
	Diff(id ID, state []byte) (changes []byte)
}

// TODO MVCC storage format
// T
//   0. type name
//   1. name C/L/N/F/etc
//   2. name C/L/N/F/etc
//   3. name C/L/N/F/etc
// O id  todo T
//   F name
//     (MVCC counter/LWW)
//     C (A id val)*
//     L (S id val)*

type GenericObject map[string]any
