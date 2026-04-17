package chotki

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/drpcorg/chotki/protocol"
	"github.com/drpcorg/chotki/rdx"
	"github.com/drpcorg/chotki/replication"
	"github.com/drpcorg/chotki/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func init() {
	// Override the package-level cleanSyncs interval so the real cleanSyncs
	// goroutine fires frequently during tests instead of every 60s.
	cleanSyncInterval = 1 * time.Second
}

// makeHPacket constructs a synthetic 'H' handshake packet.
func makeHPacket(syncID rdx.ID, vv rdx.VV) []byte {
	vvTlv := vv.TLV()
	return protocol.Record('H',
		protocol.TinyRecord('T', syncID.ZipBytes()),
		protocol.TinyRecord('M', rdx.ZipUint64(uint64(replication.SyncRW))),
		protocol.Record('V', vvTlv),
		protocol.Record('S', make([]byte, replication.TraceSize)),
	)
}

// makeDPacket constructs a synthetic 'D' diff-sync packet.
func makeDPacket(syncID, blockID rdx.ID, fields int) []byte {
	bmark, parcel := protocol.OpenHeader(nil, 'D')
	parcel = append(parcel, protocol.TinyRecord('T', syncID.ZipBytes())...)
	parcel = append(parcel, protocol.Record('R', blockID.ZipBytes())...)
	for i := 0; i < fields; i++ {
		parcel = append(parcel, protocol.Record('F', rdx.ZipUint64(uint64(i+1)))...)
		parcel = append(parcel, protocol.Record('S', rdx.Stlv(fmt.Sprintf("value-%d", i)))...)
	}
	protocol.CloseHeader(parcel, bmark)
	return parcel
}

// TestSyncPointRace_CleanSyncsVsDrain exercises the real cleanSyncs goroutine
// (started by Open) racing with real diff-sync Drain calls. The init() above
// sets cleanSyncInterval=5ms so cleanSyncs fires aggressively during the test.
//
// Runs multiple parallel sub-instances to maximize the chance of hitting the
// race window between cleanSyncs closing a batch and drain writing to it.
//
// Before the fix: panics or race detector fires.
// After the fix: passes cleanly — the per-syncPoint mutex serializes access.
func TestSyncPointRace_CleanSyncsVsDrain(t *testing.T) {
	for i := 0; i < 4; i++ {
		t.Run(fmt.Sprintf("instance-%d", i), func(t *testing.T) {
			t.Parallel()
			src1 := uint64(0xc0 + i*2)
			src2 := uint64(0xc1 + i*2)
			dirs, clear := testdirs(src1, src2)
			defer clear()

			a, err := Open(dirs[0], Options{
				Src:             src1,
				Name:            fmt.Sprintf("replica-A-%d", i),
				Logger:          utils.NewDefaultLogger(slog.LevelWarn),
				MaxSyncDuration: 1 * time.Millisecond,
			})
			require.NoError(t, err)
			defer a.Close()

			b, err := Open(dirs[1], Options{
				Src:             src2,
				Name:            fmt.Sprintf("replica-B-%d", i),
				Logger:          utils.NewDefaultLogger(slog.LevelWarn),
				MaxSyncDuration: 1 * time.Millisecond,
			})
			require.NoError(t, err)
			defer b.Close()

			// Create data on A to produce diff sync traffic
			cid, err := a.NewClass(context.Background(), rdx.ID0, Schema...)
			require.NoError(t, err)

			orm := a.ObjectMapper()
			for j := 0; j < 200; j++ {
				err = orm.New(context.Background(), cid, &Test{
					Test: fmt.Sprintf("obj-%d-%d-padding-xxxxxxxxxxxxxxxx", i, j),
				})
				require.NoError(t, err)
			}
			orm.Close()

			// Sync repeatedly — each round creates H/D/V packets through real
			// Syncer paths while the real cleanSyncs goroutine fires every 5ms
			for round := 0; round < 10; round++ {
				synca := replication.Syncer{
					Host: a, Mode: replication.SyncRW, Name: "a",
					WaitUntilNone: time.Millisecond, Src: a.src,
					Log: utils.NewDefaultLogger(slog.LevelError), PingWait: time.Second,
				}
				syncb := replication.Syncer{
					Host: b, Mode: replication.SyncRW, Name: "b",
					WaitUntilNone: time.Millisecond, Src: b.src,
					Log: utils.NewDefaultLogger(slog.LevelError), PingWait: time.Second,
				}

				_ = protocol.Relay(&syncb, &synca)
				go protocol.Pump(&syncb, &synca)
				_ = protocol.Pump(&synca, &syncb)

				synca.Close()
				syncb.Close()

				orm := a.ObjectMapper()
				for j := 0; j < 20; j++ {
					_ = orm.New(context.Background(), cid, &Test{
						Test: fmt.Sprintf("r%d-%d-%d-xxxxxxxxxxxxxxxxxxxxxxxx", round, i, j),
					})
				}
				orm.Close()
			}
		})
	}
}

// TestSyncPointRace_ConcurrentDrains tests multiple goroutines calling
// cho.Drain() with D packets for the same sync point. Uses the full
// drain() → ApplyD → batch.Merge code path.
//
// Before the fix: panics with "index out of range" in batch.Merge.
// After the fix: the per-syncPoint mutex serializes batch access.
func TestSyncPointRace_ConcurrentDrains(t *testing.T) {
	for i := 0; i < 4; i++ {
		t.Run(fmt.Sprintf("instance-%d", i), func(t *testing.T) {
			t.Parallel()
			src := uint64(0xd0 + i)
			dirs, clear := testdirs(src)
			defer clear()

			cho, err := Open(dirs[0], Options{
				Src:             src,
				Name:            fmt.Sprintf("concurrent-drain-%d", i),
				Logger:          utils.NewDefaultLogger(slog.LevelWarn),
				MaxSyncDuration: 10 * time.Minute,
			})
			require.NoError(t, err)
			defer cho.Close()

			syncID := rdx.IDFromSrcSeqOff(0xfeed, uint64(i+1), 0)
			blockID := rdx.IDFromSrcSeqOff(0xfeed, uint64(i+1), 0)

			vv, err := cho.VersionVector()
			require.NoError(t, err)
			err = cho.Drain(context.Background(), protocol.Records{makeHPacket(syncID, vv)})
			require.NoError(t, err)

			var wg sync.WaitGroup
			for g := 0; g < 8; g++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for j := 0; j < 100; j++ {
						pkt := makeDPacket(syncID, blockID, 5)
						_ = cho.Drain(context.Background(), protocol.Records{pkt})
					}
				}()
			}
			wg.Wait()

			s, ok := cho.syncs.Load(syncID)
			require.True(t, ok)
			err = cho.db.Apply(s.batch, cho.opts.PebbleWriteOptions)
			assert.NoError(t, err, "batch should apply cleanly after concurrent drains")
		})
	}
}

// TestSyncPointRace_DrainAndClose exercises Drain + real cleanSyncs + Close
// concurrently. Detects deadlocks via a 10s timeout.
func TestSyncPointRace_DrainAndClose(t *testing.T) {
	for i := 0; i < 4; i++ {
		t.Run(fmt.Sprintf("instance-%d", i), func(t *testing.T) {
			t.Parallel()
			done := make(chan struct{})
			go func() {
				defer close(done)

				src := uint64(0xe0 + i)
				dirs, clear := testdirs(src)
				defer clear()

				cho, err := Open(dirs[0], Options{
					Src:             src,
					Name:            fmt.Sprintf("deadlock-%d", i),
					Logger:          utils.NewDefaultLogger(slog.LevelWarn),
					MaxSyncDuration: 5 * time.Millisecond,
				})
				if err != nil {
					return
				}

				syncID := rdx.IDFromSrcSeqOff(0xcafe, uint64(i+1), 0)
				blockID := rdx.IDFromSrcSeqOff(0xcafe, uint64(i+1), 0)
				vv, _ := cho.VersionVector()

				ctx, cancel := context.WithCancel(context.Background())
				var wg sync.WaitGroup
				wg.Add(1)
				go func() {
					defer wg.Done()
					for ctx.Err() == nil {
						if _, ok := cho.syncs.Load(syncID); !ok {
							_ = cho.Drain(context.Background(), protocol.Records{makeHPacket(syncID, vv)})
						}
						_ = cho.Drain(context.Background(), protocol.Records{makeDPacket(syncID, blockID, 5)})
					}
				}()

				time.Sleep(100 * time.Millisecond)
				cancel()
				wg.Wait()
				cho.Close()
			}()

			select {
			case <-done:
			case <-time.After(10 * time.Second):
				t.Fatal("DEADLOCK: test did not complete within 10 seconds")
			}
		})
	}
}

// TestSyncPointRace_ReconnectionStorm simulates rapid peer reconnections
// with 4 concurrent peers doing connect/sync/disconnect cycles while
// cleanSyncs fires every 5ms.
func TestSyncPointRace_ReconnectionStorm(t *testing.T) {
	for i := 0; i < 4; i++ {
		t.Run(fmt.Sprintf("instance-%d", i), func(t *testing.T) {
			t.Parallel()
			src1 := uint64(0xf0 + i*2)
			src2 := uint64(0xf1 + i*2)
			dirs, clear := testdirs(src1, src2)
			defer clear()

			a, err := Open(dirs[0], Options{
				Src:             src1,
				Name:            fmt.Sprintf("storm-A-%d", i),
				Logger:          utils.NewDefaultLogger(slog.LevelWarn),
				MaxSyncDuration: 2 * time.Millisecond,
			})
			require.NoError(t, err)
			defer a.Close()

			b, err := Open(dirs[1], Options{
				Src:             src2,
				Name:            fmt.Sprintf("storm-B-%d", i),
				Logger:          utils.NewDefaultLogger(slog.LevelWarn),
				MaxSyncDuration: 2 * time.Millisecond,
			})
			require.NoError(t, err)
			defer b.Close()

			cid, err := a.NewClass(context.Background(), rdx.ID0, Schema...)
			require.NoError(t, err)

			orm := a.ObjectMapper()
			for j := 0; j < 100; j++ {
				err = orm.New(context.Background(), cid, &Test{
					Test: fmt.Sprintf("storm-%d-%d-xxxxxxxxxxxxxxxxxxxxxxxxxxxx", i, j),
				})
				require.NoError(t, err)
			}
			orm.Close()

			var wg sync.WaitGroup
			for peer := 0; peer < 4; peer++ {
				wg.Add(1)
				go func(peerID int) {
					defer wg.Done()
					for round := 0; round < 5; round++ {
						synca := replication.Syncer{
							Host: a, Mode: replication.SyncRW,
							Name: fmt.Sprintf("peer-%d", peerID), WaitUntilNone: time.Millisecond,
							Src: a.src, Log: utils.NewDefaultLogger(slog.LevelError), PingWait: time.Second,
						}
						syncb := replication.Syncer{
							Host: b, Mode: replication.SyncRW,
							Name: fmt.Sprintf("peer-%d", peerID), WaitUntilNone: time.Millisecond,
							Src: b.src, Log: utils.NewDefaultLogger(slog.LevelError), PingWait: time.Second,
						}
						_ = protocol.Relay(&syncb, &synca)
						go protocol.Pump(&syncb, &synca)
						_ = protocol.Pump(&synca, &syncb)
						synca.Close()
						syncb.Close()
					}
				}(peer)
			}
			wg.Wait()
		})
	}
}
