package replication

import (
	"bytes"
	"context"
	"crypto/sha1"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/drpcorg/chotki/chotki_errors"
	"github.com/drpcorg/chotki/host"
	"github.com/drpcorg/chotki/protocol"
	"github.com/drpcorg/chotki/rdx"
	"github.com/drpcorg/chotki/utils"
	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
)

const MaxParcelSize = 100_000_000

var version string = fmt.Sprintf("%d", time.Now().Unix())

type SyncHost interface {
	protocol.Drainer
	// DrainApplied is Drain plus the count of applied records (0 on error,
	// len(recs) on success, since draining is all-or-nothing).
	DrainApplied(ctx context.Context, recs protocol.Records) (int, error)
	// AbortSyncsVia closes and removes the pending diff-sync points
	// created by the given replication session (see Syncer.SessionId).
	AbortSyncsVia(ctx context.Context, sessionId string)
	Snapshot() pebble.Reader
	Broadcast(ctx context.Context, records protocol.Records, except string)
}

type sessionIdCtxKey struct{}

// WithSessionId marks ctx with the id of the replication session that is
// draining the records; the host binds the sync points (pending diff
// batches) created by those records to the session lifetime, so they can
// be aborted when the session ends (SyncHost.AbortSyncsVia).
func WithSessionId(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, sessionIdCtxKey{}, id)
}

// SessionIdFromCtx extracts the replication session id set by WithSessionId,
// or "" if none.
func SessionIdFromCtx(ctx context.Context) string {
	if v, ok := ctx.Value(sessionIdCtxKey{}).(string); ok {
		return v
	}
	return ""
}

type SyncMode byte

const (
	SyncRead   SyncMode = 1
	SyncWrite  SyncMode = 2
	SyncLive   SyncMode = 4
	SyncRW     SyncMode = SyncRead | SyncWrite
	SyncRL     SyncMode = SyncRead | SyncLive
	SyncRWLive SyncMode = SyncRead | SyncWrite | SyncLive
)

func (m *SyncMode) Zip() []byte {
	return rdx.ZipUint64(uint64(*m))
}

func (m *SyncMode) Unzip(raw []byte) error {
	parsed := rdx.UnzipUint64(raw)
	if parsed > 0b111 {
		return errors.New("invalid mode")
	}

	*m = SyncMode(parsed)
	return nil
}

const PingVal = "ping"
const PongVal = "pong"

type SyncState int

const (
	SendHandshake SyncState = iota
	SendDiff
	SendLive
	SendEOF
	SendNone
	SendPing
	SendPong
)

type PingState int

const (
	Inactive PingState = iota
	Ping
	Pong
	PingBroken
	WaitingForPing
)

func (s SyncState) String() string {
	return []string{"SendHandshake", "SendDiff", "SendLive", "SendEOF", "SendNone", "SendPing", "SendPong"}[s]
}

var SessionsStates = prometheus.NewGaugeVec(prometheus.GaugeOpts{
	Namespace: "chotki",
	Subsystem: "sync",
	Name:      "sessions",
}, []string{"id", "kind", "version"})
var OpenedSnapshots = prometheus.NewGaugeVec(prometheus.GaugeOpts{
	Namespace: "chotki",
	Subsystem: "sync",
	Name:      "opened_snapshots",
}, []string{"id", "version"})

var OpenedIterators = prometheus.NewGaugeVec(prometheus.GaugeOpts{
	Namespace: "chotki",
	Subsystem: "sync",
	Name:      "opened_iterators",
}, []string{"id", "version"})

const TraceSize = 10

type Syncer struct {
	Src           uint64
	Name          string
	Host          SyncHost
	Mode          SyncMode
	PingPeriod    time.Duration
	PingWait      time.Duration
	WaitUntilNone time.Duration

	Log        utils.Logger
	vvit, ffit *pebble.Iterator
	snap       pebble.Reader
	snaplast   rdx.ID
	feedState  SyncState
	drainState SyncState
	Oqueue     protocol.FeedCloser

	hostvv, peervv rdx.VV
	vpack          []byte
	reason         error
	myTraceId      atomic.Pointer[[TraceSize]byte]
	theirsTraceid  atomic.Pointer[[TraceSize]byte]

	lock      sync.Mutex
	cond      sync.Cond
	pingTimer *time.Timer
	pingStage atomic.Int32
	lctx      atomic.Pointer[context.Context]

	// guards snap, vvit and ffit: the feed loop and Close may touch the
	// snapshot and its iterators concurrently
	snapLock    sync.Mutex
	sessionOnce sync.Once
	sessionUid  string
}

// SessionId returns a unique identifier of this replication session; the
// host uses it to bind the sync points this session creates to its
// lifetime (SyncHost.AbortSyncsVia).
func (sync *Syncer) SessionId() string {
	sync.sessionOnce.Do(func() {
		if id, err := uuid.NewV7(); err == nil {
			sync.sessionUid = id.String()
		} else {
			sync.sessionUid = fmt.Sprintf("%s-%d", sync.Name, time.Now().UnixNano())
		}
	})
	return sync.sessionUid
}

func (sync *Syncer) withDefaultArgs(reset bool) context.Context {
	lctx := sync.lctx.Load()

	if lctx == nil || reset {
		nlctx := sync.Log.WithDefaultArgs(context.Background(), "name", sync.Name, "trace_id", sync.GetTraceId())
		if !reset {
			sync.lctx.CompareAndSwap(lctx, &nlctx)
		} else {
			sync.lctx.Store(&nlctx)
		}
		lctx = &nlctx
	}
	return *lctx
}

func (sync *Syncer) LogCtx(ctx context.Context) context.Context {
	return sync.Log.WithArgsFromCtx(ctx, sync.withDefaultArgs(false))
}

func (sync *Syncer) Close() error {
	sync.SetFeedState(context.Background(), SendNone)

	if sync.Host == nil {
		return utils.ErrClosed
	}

	// Abort sync points this session created: their 'D'/'V' travel through
	// this connection, so once it's gone the batch can't complete, and a 'V'
	// relayed later would apply the handshake VV without the data (see tla/).
	sync.Host.AbortSyncsVia(sync.LogCtx(context.Background()), sync.SessionId())

	sync.snapLock.Lock()
	defer sync.snapLock.Unlock()

	if sync.snap != nil {
		if err := sync.snap.Close(); err != nil {
			sync.Log.ErrorCtx(sync.LogCtx(context.Background()), "failed closing snapshot", "err", err.Error())
		} else {
			OpenedSnapshots.WithLabelValues(sync.Name, version).Set(0)
		}
		sync.snap = nil
	}

	closediterators := true

	if sync.ffit != nil {
		if err := sync.ffit.Close(); err != nil {
			closediterators = false
			sync.Log.ErrorCtx(sync.LogCtx(context.Background()), "failed closing ffit", "err", err)
		}
		sync.ffit = nil
	}

	if sync.vvit != nil {
		if err := sync.vvit.Close(); err != nil {
			closediterators = false
			sync.Log.ErrorCtx(sync.LogCtx(context.Background()), "failed closing vvit", "err", err)
		}
		sync.vvit = nil
	}
	if closediterators {
		OpenedIterators.WithLabelValues(sync.Name, version).Set(0)
	}

	sync.Log.InfoCtx(sync.LogCtx(context.Background()), fmt.Sprintf("sync: connection %s closed: %v\n", sync.Name, sync.reason))

	return nil
}
func (sync *Syncer) GetFeedState() SyncState {
	sync.lock.Lock()
	defer sync.lock.Unlock()
	return sync.feedState
}

func (sync *Syncer) pingTransition(ctx context.Context) {
	//nolint:exhaustive
	switch PingState(sync.pingStage.Load()) {
	case Ping:
		sync.SetFeedState(ctx, SendPing)
	case Pong:
		sync.SetFeedState(ctx, SendPong)
	case PingBroken:
		sync.SetFeedState(ctx, SendEOF)
	}
}

func (sync *Syncer) GetDrainState() SyncState {
	sync.lock.Lock()
	defer sync.lock.Unlock()
	return sync.drainState
}

func (sync *Syncer) Feed(ctx context.Context) (recs protocol.Records, err error) {
	SessionsStates.WithLabelValues(sync.Name, "feed", version).Set(float64(sync.GetFeedState()))
	// The peer's bye only means it has nothing more to send; it keeps
	// applying our records until the connection closes. Finish our own
	// handshake/diff phase first (cutting it short would leave the peer a
	// staged batch with no 'V', silently missing promised data), then wind down.
	if sync.GetDrainState() == SendNone {
		if fs := sync.GetFeedState(); fs != SendHandshake && fs != SendDiff {
			sync.SetFeedState(ctx, SendNone)
		}
	}
	switch sync.GetFeedState() {
	case SendHandshake:
		recs, err = sync.FeedHandshake()
		if err == nil {
			sync.SetFeedState(ctx, SendDiff)
		}

	case SendDiff:
		ctx, cancel := context.WithCancel(ctx)
		defer cancel()
		select {
		case <-time.After(sync.PingWait):
			sync.Log.ErrorCtx(sync.LogCtx(ctx), "sync: handshake took too long")
			sync.SetFeedState(ctx, SendEOF)
			return
		case <-sync.WaitDrainState(ctx, SendDiff):
		}
		recs, err = sync.FeedBlockDiff(ctx)
		if err == io.EOF {
			recs2, _ := sync.FeedDiffVV(ctx)
			recs = append(recs, recs2...)
			if (sync.Mode & SyncLive) != 0 {
				sync.SetFeedState(ctx, SendLive)
				sync.resetPingTimer()
			} else {
				sync.SetFeedState(ctx, SendEOF)
			}

			sync.snapLock.Lock()
			if sync.snap != nil {
				err = sync.snap.Close()
				if err != nil {
					sync.Log.ErrorCtx(sync.LogCtx(ctx), "sync: failed closing snapshot", "err", err)
				} else {
					OpenedSnapshots.WithLabelValues(sync.Name, version).Set(0)
				}
				sync.snap = nil
				err = nil
			}
			sync.snapLock.Unlock()
		}
	case SendPing:
		recs = protocol.Records{
			protocol.Record('P', rdx.Stlv(PingVal)),
		}
		sync.SetFeedState(ctx, SendLive)
		sync.pingStage.Store(int32(WaitingForPing))
		// pingTimer is shared with resetPingTimer (drain side), so it
		// must only be touched under the lock
		sync.lock.Lock()
		if sync.pingTimer != nil {
			sync.pingTimer.Stop()
		}
		sync.pingTimer = time.AfterFunc(sync.PingWait, func() {
			sync.pingStage.Store(int32(PingBroken))
			sync.Log.ErrorCtx(sync.LogCtx(ctx), "sync: peer did not respond to ping")
		})
		sync.lock.Unlock()
	case SendPong:
		recs = protocol.Records{
			protocol.Record('P', rdx.Stlv(PongVal)),
		}
		sync.pingStage.Store(int32(Inactive))
		sync.SetFeedState(ctx, SendLive)
	case SendLive:
		recs, err = sync.Oqueue.Feed(ctx)
		if err == utils.ErrClosed {
			sync.Log.InfoCtx(sync.LogCtx(ctx), "sync: queue closed")
			sync.SetFeedState(ctx, SendEOF)
			err = nil
		}
		sync.pingTransition(ctx)

	case SendEOF:
		reason := []byte("closing")
		if sync.reason != nil {
			reason = []byte(sync.reason.Error())
		}
		recs = protocol.Records{protocol.Record('B',
			protocol.TinyRecord('T', sync.snaplast.ZipBytes()),
			reason,
		)}
		sync.snapLock.Lock()
		if sync.snap != nil {
			err = sync.snap.Close()
			if err != nil {
				sync.Log.ErrorCtx(sync.LogCtx(ctx), "sync: failed closing snapshot", "error", err.Error())
			} else {
				OpenedSnapshots.WithLabelValues(sync.Name, version).Set(0)
			}
			sync.snap = nil
		}
		sync.snapLock.Unlock()
		sync.SetFeedState(ctx, SendNone)

	case SendNone:
		wait := sync.WaitUntilNone
		if wait == 0 {
			wait = time.Second
		}
		// give the peer up to WaitUntilNone to drain our 'B' and close
		// first, but bound the wait with a timeout instead of forcing
		// the drain state via a one-shot timer: a Drain racing with
		// Close could move the drain state backwards after that timer
		// had already fired, leaving this wait blocked forever
		nctx, cancel := context.WithTimeout(ctx, wait)
		<-sync.WaitDrainState(nctx, SendNone)
		cancel()
		err = io.EOF
	}

	return
}

func (sync *Syncer) FeedHandshake() (vv protocol.Records, err error) {
	sync.snapLock.Lock()
	defer sync.snapLock.Unlock()

	sync.snap = sync.Host.Snapshot()

	OpenedSnapshots.WithLabelValues(sync.Name, version).Set(1)
	sync.vvit, err = sync.snap.NewIter(&pebble.IterOptions{
		LowerBound: []byte{'V'},
		UpperBound: []byte{'W'},
	})
	if err != nil {
		return nil, err
	}
	sync.ffit, err = sync.snap.NewIter(&pebble.IterOptions{
		LowerBound: []byte{'O'},
		UpperBound: []byte{'P'},
	})
	if err != nil {
		return nil, err
	}

	OpenedIterators.WithLabelValues(sync.Name, version).Set(1)

	ok := sync.vvit.SeekGE(host.VKey0)
	if !ok || 0 != bytes.Compare(sync.vvit.Key(), host.VKey0) {
		return nil, rdx.ErrBadV0Record
	}
	sync.hostvv = make(rdx.VV)
	err = sync.hostvv.PutTLV(sync.vvit.Value())
	if err != nil {
		return nil, err
	}
	sync.snaplast = sync.hostvv.GetID(sync.Src)

	sync.vpack = make([]byte, 0, 4096)
	_, sync.vpack = protocol.OpenHeader(sync.vpack, 'V') // 5
	sync.vpack = append(sync.vpack, protocol.Record('T', sync.snaplast.ZipBytes())...)

	sync.lock.Lock()
	mode := sync.Mode.Zip()
	sync.lock.Unlock()
	uuid, err := uuid.NewV7()
	if err != nil {
		return nil, err
	}
	hash := sha1.Sum(uuid[:])
	tracePart := [TraceSize]byte(hash[:TraceSize])
	sync.myTraceId.Store(&tracePart)
	sync.withDefaultArgs(true)

	// handshake: H(T{pro,src} M(mode) V(V{p,s}+), T(trace_ids))
	hs := protocol.Record('H',
		protocol.TinyRecord('T', sync.snaplast.ZipBytes()),
		protocol.TinyRecord('M', mode),
		protocol.Record('V', sync.vvit.Value()),
		protocol.Record('S', tracePart[:]),
	)

	return protocol.Records{hs}, nil
}

func (sync *Syncer) getVVChanges() (hasChanges bool, sendvv rdx.VV, err error) {
	vv := make(rdx.VV)
	err = vv.PutTLV(sync.vvit.Value())
	if err != nil {
		return false, nil, rdx.ErrBadVRecord
	}
	sendvv = make(rdx.VV)
	// check for any changes
	hasChanges = false // fixme up & repeat
	for src, pro := range vv {
		peerpro, ok := sync.peervv[src]
		if !ok || pro > peerpro {
			sendvv[src] = peerpro
			hasChanges = true
		}
	}
	return
}

func (sync *Syncer) nextBlockDiff() (bool, rdx.VV, error) {
	if sync.ffit != nil {
		block := host.VKeyId(sync.vvit.Key()).ZeroOff()
		till := block.ProPlus(host.SyncBlockMask + 1)
		if sync.ffit.Valid() {
			id, _ := host.OKeyIdRdt(sync.ffit.Key())
			if id != rdx.BadId && id.Less(till) {
				_, sendvv, err := sync.getVVChanges()
				if err != nil {
					return false, nil, err
				}
				return true, sendvv, nil
			}
		}
	}
	if sync.vvit == nil || !sync.vvit.Next() {
		return false, nil, io.EOF
	}
	hasChanges, sendvv, err := sync.getVVChanges()
	if err != nil {
		return false, nil, err
	}
	if !hasChanges {
		return false, nil, nil
	}

	block := host.VKeyId(sync.vvit.Key()).ZeroOff()
	key := host.OKey(block, 0)
	sync.ffit.SeekGE(key)
	return true, sendvv, nil
}

func (sync *Syncer) FeedBlockDiff(ctx context.Context) (diff protocol.Records, err error) {
	sync.snapLock.Lock()
	defer sync.snapLock.Unlock()

	hasChanges, sendvv, cerr := sync.nextBlockDiff()
	if cerr != nil {
		return nil, cerr
	}
	if !hasChanges {
		return protocol.Records{}, nil
	}

	block := host.VKeyId(sync.vvit.Key()).ZeroOff()
	bmark, parcel := protocol.OpenHeader(nil, 'D')
	parcel = append(parcel, protocol.Record('T', sync.snaplast.ZipBytes())...)
	parcel = append(parcel, protocol.Record('R', block.ZipBytes())...)
	till := block.ProPlus(host.SyncBlockMask + 1)
	for ; sync.ffit.Valid(); sync.ffit.Next() {
		id, rdt := host.OKeyIdRdt(sync.ffit.Key())
		if id == rdx.BadId || till.Less(id) {
			break
		}
		if len(parcel) > MaxParcelSize {
			break
		}
		lim, ok := sendvv[id.Src()]
		if ok && (id.Pro() > lim || lim == 0) {
			parcel = append(parcel, protocol.Record('F', rdx.ZipUint64(uint64(id.Pro()-block.Pro())))...)
			val := sync.ffit.Value()
			parcel = append(parcel, protocol.Record(rdt, val)...)
			if len(val) > MaxParcelSize {
				sync.Log.WarnCtx(sync.LogCtx(ctx), "too big key size", "size", len(val))
			}
			continue
		}
		diff := rdx.Xdiff(rdt, sync.ffit.Value(), sendvv)
		if len(diff) != 0 {
			parcel = append(parcel, protocol.Record('F', rdx.ZipUint64(uint64(id.Pro()-block.Pro())))...)
			parcel = append(parcel, protocol.Record(rdt, diff)...)
			if len(diff) > MaxParcelSize {
				sync.Log.WarnCtx(sync.LogCtx(ctx), "too big diff size", "size", len(diff))
			}
		}
	}
	protocol.CloseHeader(parcel, bmark)
	v := protocol.Record('V',
		protocol.Record('R', block.ZipBytes()),
		sync.vvit.Value()) // todo brief
	sync.vpack = append(sync.vpack, v...)
	return protocol.Records{parcel}, err
}

func (sync *Syncer) FeedDiffVV(ctx context.Context) (vv protocol.Records, err error) {
	sync.snapLock.Lock()
	defer sync.snapLock.Unlock()

	protocol.CloseHeader(sync.vpack, 5)
	vv = append(vv, sync.vpack)
	sync.vpack = nil
	closediterators := true
	if sync.ffit != nil {
		err = sync.ffit.Close()
		if err != nil {
			closediterators = false
			sync.Log.ErrorCtx(sync.LogCtx(ctx), "failed closing ffit", "err", err)
		}
		sync.ffit = nil
	}
	if sync.vvit != nil {
		err = sync.vvit.Close()
		if err != nil {
			closediterators = false
			sync.Log.ErrorCtx(sync.LogCtx(ctx), "failed closing vvit", "err", err)
		}
		sync.vvit = nil
	}
	if closediterators {
		OpenedIterators.WithLabelValues(sync.Name, version).Set(0)
	}
	return
}

func (sync *Syncer) SetFeedState(ctx context.Context, state SyncState) {
	SessionsStates.WithLabelValues(sync.Name, "feed", version).Set(float64(state))
	sync.Log.InfoCtx(sync.LogCtx(ctx), "sync: feed state", "state", state.String())
	sync.lock.Lock()
	sync.feedState = state
	sync.lock.Unlock()
}

func (sync *Syncer) SetDrainState(ctx context.Context, state SyncState) {
	sync.Log.InfoCtx(sync.LogCtx(ctx), "sync: drain state", "state", state.String())
	SessionsStates.WithLabelValues(sync.Name, "drain", version).Set(float64(state))
	sync.lock.Lock()
	sync.drainState = state
	if sync.cond.L == nil {
		sync.cond.L = &sync.lock
	}
	sync.cond.Broadcast()
	sync.lock.Unlock()
}

func (sync *Syncer) WaitDrainState(ctx context.Context, state SyncState) chan SyncState {
	// buffered so the waiter goroutine never blocks (and leaks) when the
	// caller abandons the wait (e.g. on timeout)
	res := make(chan SyncState, 1)
	// derive a cancelable context so the watcher goroutine below always
	// terminates, even for non-cancellable parent contexts
	ctx, cancel := context.WithCancel(ctx)
	go func() {
		<-ctx.Done()
		// take the lock so the wakeup cannot slip between the waiter's
		// ctx check and its cond.Wait()
		sync.lock.Lock()
		sync.cond.Broadcast()
		sync.lock.Unlock()
	}()
	go func() {
		defer close(res)
		defer cancel()
		sync.lock.Lock()
		defer sync.lock.Unlock()
		if sync.cond.L == nil {
			sync.cond.L = &sync.lock
		}
		for sync.drainState < state {
			if ctx.Err() != nil {
				return
			}
			sync.cond.Wait()
		}
		ds := sync.drainState

		res <- ds
	}()
	return res
}

func LastLit(recs protocol.Records) byte {
	if len(recs) == 0 {
		return 0
	}
	return protocol.Lit(recs[len(recs)-1])
}

func (sync *Syncer) resetPingTimer() {
	sync.lock.Lock()
	defer sync.lock.Unlock()
	if sync.pingTimer != nil && sync.pingStage.Load() != int32(WaitingForPing) {
		sync.pingTimer.Reset(sync.PingPeriod)
		sync.pingStage.CompareAndSwap(int32(Ping), int32(Inactive))
	} else {
		if sync.pingTimer != nil {
			sync.pingTimer.Stop()
		}
		sync.pingTimer = time.AfterFunc(sync.PingPeriod, func() {
			sync.pingStage.Store(int32(Ping))
		})
		sync.pingStage.CompareAndSwap(int32(WaitingForPing), int32(Inactive))
	}
}

func (sync *Syncer) processPings(recs protocol.Records) protocol.Records {
	// filter 'P' records out in place: they're session-scoped and must not
	// reach the DB or be relayed.
	filtered := recs[:0]
	for _, rec := range recs {
		if protocol.Lit(rec) != 'P' {
			filtered = append(filtered, rec)
			continue
		}
		body, _ := protocol.Take('P', rec)
		switch rdx.Snative(body) {
		case PingVal:
			sync.Log.InfoCtx(sync.LogCtx(context.Background()), "ping received")
			// go to pong state next time
			sync.pingStage.Store(int32(Pong))
		case PongVal:
			sync.Log.InfoCtx(sync.LogCtx(context.Background()), "pong received")
		}
	}
	return filtered
}

// relayApplied rebroadcasts the applied records, minus a trailing session-scoped
// 'B' (bye). Draining is all-or-nothing, so applied is 0 (dropped — resync
// re-delivers) or len(recs); a persisted batch MUST relay, or downstream never
// gets it (live records aren't re-sent and diff syncs would skip them).
func (sync *Syncer) relayApplied(ctx context.Context, recs protocol.Records, applied int) {
	relay := recs[:applied]
	if LastLit(relay) == 'B' {
		relay = relay[:len(relay)-1]
	}
	if len(relay) > 0 {
		sync.Host.Broadcast(sync.LogCtx(ctx), relay, sync.Name)
	}
}

func (sync *Syncer) Drain(ctx context.Context, recs protocol.Records) (err error) {
	if len(recs) == 0 {
		return nil
	}
	SessionsStates.WithLabelValues(sync.Name, "drain", version).Set(float64(sync.GetDrainState()))

	recs = sync.processPings(recs)
	if len(recs) == 0 {
		// A ping-only batch is normal once live, but before the handshake it's a
		// protocol error: a ping-only peer would keep the session alive forever
		// without ever handshaking.
		if sync.GetDrainState() == SendHandshake {
			return chotki_errors.ErrBadHPacket
		}
		// the batch contained pings only; there is nothing to drain,
		// relay or change state upon
		if sync.Mode&SyncLive != 0 {
			sync.resetPingTimer()
		}
		return nil
	}

	// mark the records with this session's id so the sync points they
	// create die together with the session (AbortSyncsVia in Close)
	hctx := WithSessionId(sync.LogCtx(ctx), sync.SessionId())

	switch sync.GetDrainState() {
	case SendHandshake:
		err = sync.DrainHandshake(recs[0:1])
		if err == nil {
			err = sync.Host.Drain(hctx, recs[0:1])
		}
		if err != nil {
			return
		}
		sync.Host.Broadcast(sync.LogCtx(ctx), recs[0:1], sync.Name)
		recs = recs[1:]
		sync.SetDrainState(ctx, SendDiff)
		if len(recs) == 0 {
			break
		}
		fallthrough

	case SendDiff:
		lit := LastLit(recs)
		if lit != 'D' && lit != 'V' {
			if lit == 'B' {
				sync.SetDrainState(ctx, SendNone)
			} else {
				sync.SetDrainState(ctx, SendLive)
			}
		}
		if sync.Mode&SyncLive != 0 {
			sync.resetPingTimer()
		}
		var applied int
		applied, err = sync.Host.DrainApplied(hctx, recs)
		sync.relayApplied(ctx, recs, applied)

	case SendLive:
		sync.resetPingTimer()
		lit := LastLit(recs)
		if lit == 'B' {
			sync.SetDrainState(ctx, SendNone)
		}
		var applied int
		applied, err = sync.Host.DrainApplied(hctx, recs)
		sync.relayApplied(ctx, recs, applied)

	case SendPong, SendPing:
		panic("chotki: unacceptable sync-state")

	case SendEOF, SendNone:
		return chotki_errors.ErrClosed

	default:
		panic("chotki: unacceptable sync-state")
	}

	if err != nil { // todo send the error msg
		sync.Log.ErrorCtx(sync.LogCtx(ctx), "error happened while drain", "error", err.Error())
		sync.SetDrainState(ctx, SendEOF)
	}

	return
}

func (sync *Syncer) GetTraceId() string {
	theirsP := sync.theirsTraceid.Load()
	if theirsP == nil {
		theirsP = &[TraceSize]byte{}
	}
	theirs := hex.EncodeToString((*theirsP)[:])
	mineP := sync.myTraceId.Load()
	if mineP == nil {
		mineP = &[TraceSize]byte{}
	}
	mine := hex.EncodeToString((*mineP)[:])
	if strings.Compare(mine, theirs) >= 0 {
		return mine + "-" + theirs
	} else {
		return theirs + "-" + mine
	}
}

func (sync *Syncer) DrainHandshake(recs protocol.Records) (err error) {
	lit, _, _, body, e := ParsePacket(recs[0])
	if lit != 'H' || e != nil {
		return chotki_errors.ErrBadHPacket
	}
	var mode SyncMode
	var trace_id []byte
	mode, sync.peervv, trace_id, err = ParseHandshake(body)
	sync.lock.Lock()
	if trace_id != nil {
		if len(trace_id) != TraceSize {
			err = chotki_errors.ErrBadHPacket
		} else {
			traceId := [TraceSize]byte(trace_id)
			sync.theirsTraceid.Store(&traceId)
			sync.withDefaultArgs(true)
		}
	}
	sync.Mode &= mode
	sync.lock.Unlock()
	return
}
