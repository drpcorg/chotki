package protocol

import (
	"context"
	"io"
)

// Package protocol provides the two fundamental interfaces Feeder and Drainer
// that are used throughout the codebase for reading and writing Records.
// These interfaces form the core abstraction for data flow operations,
// allowing components to be decoupled and easily testable.
// Feeder handles reading records from a source, while Drainer handles
// writing records to a destination.

// Feeder is an interface that defines the contract for reading records.
// Implementations should provide a method to feed records from a source.
type Feeder interface {
	// Feed reads and returns records.
	// The EoF convention follows that of io.Reader:
	// can either return `records, EoF` or
	// `records, nil` followed by `nil/{}, EoF`
	Feed(ctx context.Context) (recs Records, err error)
}

// FeedCloser combines the Feeder interface with io.Closer,
// allowing implementations to both feed records and be properly closed.
type FeedCloser interface {
	Feeder
	io.Closer
}

// Drainer is an interface that defines the contract for writing records.
// Implementations should provide a method to drain records to a destination.
type Drainer interface {
	Drain(ctx context.Context, recs Records) error
}

// DrainCloser combines the Drainer interface with io.Closer,
// allowing implementations to both drain records and be properly closed.
type DrainCloser interface {
	Drainer
	io.Closer
}

// FeedDrainCloser combines Feeder, Drainer, and io.Closer interfaces,
// providing a complete read-write-close capability.
type FeedDrainCloser interface {
	Feeder
	Drainer
	io.Closer
}

// Traced is an interface for objects that can provide a trace ID
// for debugging and monitoring purposes.
type Traced interface {
	GetTraceId() string
}

// FeedDrainCloserTraced combines FeedDrainCloser with Traced interface,
// providing complete read-write-close capability with tracing support.
type FeedDrainCloserTraced interface {
	FeedDrainCloser
	Traced
}

// Relay performs a single feed-drain operation between a feeder and drainer.
// It reads records from the feeder and writes them to the drainer in one operation.
// Returns an error if either the feed or drain operation fails.
func Relay(feeder Feeder, drainer Drainer) error {
	recs, err := feeder.Feed(context.Background())
	if err != nil {
		if len(recs) > 0 {
			_ = drainer.Drain(context.Background(), recs)
		}
		return err
	}
	err = drainer.Drain(context.Background(), recs)
	return err
}

// Pump continuously relays records from feeder to drainer until an error occurs.
// This function runs indefinitely until the feeder returns an error (typically EOF).
// It uses context.Background() for all operations.
func Pump(feeder Feeder, drainer Drainer) (err error) {
	for err == nil {
		err = Relay(feeder, drainer)
	}
	return
}

// PumpCtx continuously relays records from feeder to drainer until an error occurs
// or the context is cancelled. This function respects context cancellation and
// will stop pumping when the context is done.
func PumpCtx(ctx context.Context, feeder Feeder, drainer Drainer) (err error) {
	for err == nil && ctx.Err() == nil {
		err = Relay(feeder, drainer)
	}
	return
}

// PumpCtxCallback continuously relays records from feeder to drainer until an error occurs,
// the context is cancelled, or the callback function returns false.
// The callback function is called after each relay operation and can be used
// to implement custom stopping conditions.
func PumpCtxCallback(ctx context.Context, feeder Feeder, drainer Drainer, f func() bool) (err error) {
	for err == nil && ctx.Err() == nil {
		err = Relay(feeder, drainer)
		if !f() {
			return
		}
	}
	return
}

// PumpN relays records from feeder to drainer exactly n times.
// This function will stop after n successful relay operations,
// regardless of whether more data is available.
func PumpN(feeder Feeder, drainer Drainer, n int) (err error) {
	for err == nil && n > 0 {
		err = Relay(feeder, drainer)
		n--
	}
	return
}

// PumpThenClose continuously pumps records from feed to drain until an error occurs,
// then properly closes both the feeder and drainer. This function ensures
// that resources are cleaned up even if errors occur during pumping.
// Returns the first error encountered (feed error takes precedence over drain error).
func PumpThenClose(feed FeedCloser, drain DrainCloser) error {
	var ferr, derr error
	for ferr == nil && derr == nil {
		var recs Records
		recs, ferr = feed.Feed(context.Background())
		if len(recs) > 0 { // e.g. Feed() may return data AND EOF
			derr = drain.Drain(context.Background(), recs)
		}
	}
	_ = feed.Close()
	_ = drain.Close()
	if ferr != nil {
		return ferr
	} else {
		return derr
	}
}
