package replication

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/drpcorg/chotki/protocol"
	"github.com/drpcorg/chotki/utils"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

type fakeSyncHost struct{}

func (f *fakeSyncHost) Drain(ctx context.Context, recs protocol.Records) error { return nil }
func (f *fakeSyncHost) DrainApplied(ctx context.Context, recs protocol.Records) (int, error) {
	return len(recs), nil
}
func (f *fakeSyncHost) AbortSyncsVia(ctx context.Context, sessionId string) {}
func (f *fakeSyncHost) Snapshot() pebble.Reader                             { return nil }
func (f *fakeSyncHost) Broadcast(ctx context.Context, recs protocol.Records, except string) {
}

// countSeries returns how many series of the vector carry the given session id label.
func countSeries(t *testing.T, vec *prometheus.GaugeVec, id string) int {
	t.Helper()
	reg := prometheus.NewPedanticRegistry()
	require.NoError(t, reg.Register(vec))
	mfs, err := reg.Gather()
	require.NoError(t, err)
	count := 0
	for _, mf := range mfs {
		for _, m := range mf.GetMetric() {
			for _, l := range m.GetLabel() {
				if l.GetName() == "id" && l.GetValue() == id {
					count++
				}
			}
		}
	}
	return count
}

func TestCloseRemovesSessionMetricSeries(t *testing.T) {
	name := "listen:test-close-removes-series"
	syn := &Syncer{
		Name: name,
		Host: &fakeSyncHost{},
		Log:  utils.NewDefaultLogger(slog.LevelError),
	}

	// Simulate what a live session does: state gauges for feed/drain,
	// snapshot and iterator gauges from the handshake.
	syn.SetFeedState(context.Background(), SendLive)
	syn.SetDrainState(context.Background(), SendLive)
	OpenedSnapshots.WithLabelValues(name, version).Set(1)
	OpenedIterators.WithLabelValues(name, version).Set(1)

	require.Equal(t, 2, countSeries(t, SessionsStates, name), "feed+drain series must exist before Close")
	require.Equal(t, 1, countSeries(t, OpenedSnapshots, name))
	require.Equal(t, 1, countSeries(t, OpenedIterators, name))

	require.NoError(t, syn.Close())

	require.Equal(t, 0, countSeries(t, SessionsStates, name), "session state series must be deleted on Close")
	require.Equal(t, 0, countSeries(t, OpenedSnapshots, name), "snapshot series must be deleted on clean Close")
	require.Equal(t, 0, countSeries(t, OpenedIterators, name), "iterator series must be deleted on clean Close")
}

type failingReader struct{}

func (f *failingReader) Get(key []byte) (value []byte, closer io.Closer, err error) {
	return nil, nil, pebble.ErrNotFound
}
func (f *failingReader) NewIter(o *pebble.IterOptions) (*pebble.Iterator, error) {
	return nil, errors.New("not implemented")
}
func (f *failingReader) Close() error { return errors.New("snapshot close failed") }

func TestCloseKeepsSnapshotSeriesWhenSnapshotCloseFails(t *testing.T) {
	name := "listen:test-close-snap-fail"
	syn := &Syncer{
		Name: name,
		Host: &fakeSyncHost{},
		Log:  utils.NewDefaultLogger(slog.LevelError),
	}
	syn.snap = &failingReader{}
	OpenedSnapshots.WithLabelValues(name, version).Set(1)
	t.Cleanup(func() { OpenedSnapshots.DeleteLabelValues(name, version) })
	syn.SetFeedState(context.Background(), SendLive)

	require.NoError(t, syn.Close())

	require.Equal(t, 1, countSeries(t, OpenedSnapshots, name), "failed snapshot close must keep the leak signal")
	require.Equal(t, 0, countSeries(t, SessionsStates, name))
}

func TestCloseKeepsSeriesAfterMidSessionCloseFailure(t *testing.T) {
	name := "listen:test-close-mid-session-fail"
	syn := &Syncer{
		Name: name,
		Host: &fakeSyncHost{},
		Log:  utils.NewDefaultLogger(slog.LevelError),
	}
	// Simulate Feed()/FeedDiffVV() having failed to close the snapshot and
	// iterators mid-session: the handles are nil'ed there, but the gauge
	// series stay at 1 as the leak signal.
	syn.snapCloseFailed = true
	syn.iterCloseFailed = true
	OpenedSnapshots.WithLabelValues(name, version).Set(1)
	OpenedIterators.WithLabelValues(name, version).Set(1)
	t.Cleanup(func() {
		OpenedSnapshots.DeleteLabelValues(name, version)
		OpenedIterators.DeleteLabelValues(name, version)
	})

	require.NoError(t, syn.Close())

	require.Equal(t, 1, countSeries(t, OpenedSnapshots, name), "mid-session snapshot close failure must keep the leak signal")
	require.Equal(t, 1, countSeries(t, OpenedIterators, name), "mid-session iterator close failure must keep the leak signal")
}

func TestCloseWithNilHostStillRemovesSessionSeries(t *testing.T) {
	name := "listen:test-close-nil-host"
	syn := &Syncer{
		Name: name,
		Log:  utils.NewDefaultLogger(slog.LevelError),
	}

	syn.SetFeedState(context.Background(), SendLive)
	require.Equal(t, 1, countSeries(t, SessionsStates, name))

	require.Error(t, syn.Close()) // nil Host reports ErrClosed, but must not leak series

	require.Equal(t, 0, countSeries(t, SessionsStates, name), "series must be deleted even on the nil-Host path")
}
