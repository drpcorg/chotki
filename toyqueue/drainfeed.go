package toyqueue

import "io"

// Records (a batch of) as a very universal primitive, especially
// for database/network op/packet processing. Batching allows
// for writev() and other performance optimizations. ALso, if
// you have cryptography, blobs are way handier than structs.
// Records converts easily to net.Buffers.
type Records [][]byte

type Feeder interface {
	// Feed reads and returns records.
	// The EoF convention follows that of io.Reader:
	// can either return `records, EoF` or
	// `records, nil` followed by `nil/{}, EoF`
	Feed() (recs Records, err error)
}

type FeedCloser interface {
	Feeder
	io.Closer
}

type FeedSeeker interface {
	Feeder
	io.Seeker
}

type FeedSeekCloser interface {
	Feeder
	io.Seeker
	io.Closer
}

type Drainer interface {
	Drain(recs Records) error
}

type DrainSeeker interface {
	Drainer
	io.Seeker
}

type DrainCloser interface {
	Drainer
	io.Closer
}

type DrainSeekCloser interface {
	Drainer
	io.Seeker
	io.Closer
}

type FeedDrainer interface {
	Feeder
	Drainer
}

type FeedDrainCloser interface {
	Feeder
	Drainer
	io.Closer
}
