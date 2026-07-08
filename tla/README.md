# TLA+ specification of the Chotki sync protocol

`ChotkiSync.tla` is a formal model of the replication protocol implemented in
`replication/sync.go`, `chotki.go` (`drain`/`Broadcast`/`CommitPacket`),
`packets.go` (`ApplyH`/`ApplyD`/`ApplyV`/`ApplyE`) and `network/peer.go`
(read-side record coalescing). See the header comment of the module for the
precise abstraction choices.

## What is modelled

- **Replica state**: the pebble DB is reduced to a set of applied ops,
  the global version vector (`VKey0`) and per-block version vectors
  (`VKey(block)`; one object == one sync block).
- **Local id allocation**: `cho.last` is modelled as the cached allocator
  state used by `CommitPacket`, with a small witness action for own-source
  records drained outside the local commit lock.
- **Session FSM**: the `Syncer` feed states (`SendHandshake → SendDiff →
  SendLive → SendEOF → SendNone`, plus ping/pong) and drain states, exactly
  as driven by `Feed()`/`Drain()` including the `LastLit` batch-tail logic.
- **Diff sync**: pebble snapshot at handshake, per-block change detection
  (`getVVChanges`: block VV vs peer global VV), diff parcels, the closing
  `'V'` packet, and the sync-point batch on the receiving side that is
  applied atomically when `'V'` arrives (`cho.syncs`).
- **Live sync and relaying**: `CommitPacket` broadcast, and the rebroadcast
  of drained `'H'`/`'D'`/`'V'`/live records to all other sessions, which is
  how a diff sync propagates through a tree of replicas.
- **The network**: per-direction ordered reliable channels; a `Drain` call
  consumes an arbitrary non-empty *prefix* of the channel, modelling
  `keepRead` coalescing several writes into one batch (this is essential —
  several protocol decisions look only at the last record of a batch).
- **Session termination and reconnects**: `'B'` (bye) records, connection
  teardown dropping whatever was still in flight or queued, and reconnects
  with a fresh handshake while stale sync points survive at the replicas.

Not modelled: sync-mode negotiation (chotki always runs `SyncRWLive`
sessions), `MaxParcelSize` truncation, `cleanSyncs` expiry (subsumed by the
session-death nondeterminism), byte-level TLV encoding, and queue overflow
(an overflow kills the session, which the model covers as session death).

## Checked properties

| Property | Meaning |
|---|---|
| `TypeOK` | domains of all state variables |
| `NoGaps` | **data safety**: a replica's global VV never claims an op that is not in its store. Because diff sync uses the peer's global VV as the resend floor, a violation is *permanent, silent data loss* — nobody will ever resend that op. |
| `VVBounded` | a VV never runs ahead of what the source actually committed |
| `LastCoversOwnVV` | the cached local allocator `cho.last` never lags behind the replica's own VV entry |
| `LastNotAhead` | ...nor runs *ahead* of the persisted own VV entry. `cho.last` lives only in memory and is rebuilt from the VV on restart, so an id it holds that the VV does not is reissued after a crash. With `LastCoversOwnVV` this pins `cho.last = gvv[r][r]`. |
| `FreshLocalIds` | each local own-source event receives a fresh seq; repeated seqs violate this |
| `BvvExact`, `BvvCovers` | block VVs are exact and complete w.r.t. the store; the sender-side `hasChanges` check depends on this |
| `SyncPointPerSrc` | at most one pending diff batch per origin replica (`"allow only 1 diff sync per src"`) |
| `QuiescentConverged` | when nothing is in flight or queued and all sessions are live, all replicas hold the same data (eventual consistency over a tree topology) |
| `NotConverged` | used *negated* as a reachability witness: TLC's "counterexample" is a complete happy-path trace ending in full convergence |

## Running

Get `tla2tools.jar` (TLA+ tools, TLC ≥ 2.15) and run from this directory:

```sh
java -cp tla2tools.jar tlc2.TLC -config MCFixed.cfg -workers auto -deadlock MCChotkiSync
```

| Config | Expectation |
|---|---|
| `MCFixed.cfg` | fixed protocol, chain `a–b–c`, both ends commit: **no violations** |
| `MCFixedChurn.cfg` | fixed protocol + session closes/reconnects: **no violations** |
| `MCFixedLast.cfg` | intended fixed `cho.last` allocator witness: **no violations** |
| `MCWitness.cfg` | `NotConverged` "violated": a full convergence trace |
| `MCPing.cfg` | ping/pong machinery enabled: no violations |
| `MCBuggyRelay.cfg` | pre-fix relay logic: **`NoGaps` violated** (bug 1 below) |
| `MCBuggyDisplace.cfg` | pre-fix displacement: **`SyncPointPerSrc` violated** (bug 2) |
| `MCBuggyStaleSync.cfg` | pre-fix sync-point lifetime: **`NoGaps` violated** (bug 3) |
| `MCBuggyLast.cfg` | pre-fix unsynchronized `cho.last`: **`FreshLocalIds` violated** (bug 4) |
| `MCBatchFixed.cfg` | fixed batched drain (`BatchMode`), chain `a–b–c`: **no violations** (batching preserves `NoGaps`/`QuiescentConverged`) |
| `MCBatchFixedLast.cfg` | fixed batched drain + own-source drain witness: **no violations** (`cho.last` stays in lock-step with the VV) |
| `MCBatchBuggy.cfg` | pre-fix batched drain (`BatchBuggy`): **`LastNotAhead` violated** (bug 5 below) |

(`-deadlock` disables deadlock reporting: behaviours legitimately terminate
once the commit/reconnect budgets are exhausted.)

## Bugs found while writing this spec

The original model-switchable sync bugs plus the added local allocator
witness:

1. **Lost rebroadcast of applied records** (`replication/sync.go Drain`,
   modelled by `BuggyRelay = TRUE`). When network batching coalesced records
   with the peer's closing `'B'` (bye) — e.g. `[E, B]` in live mode or
   `[D, V, B]` at the end of a non-live diff — the batch was applied locally
   but **not rebroadcast at all**; the same happened to the applied prefix of
   a batch that failed mid-way. Downstream replicas never receive those
   records: live records are not re-sent, and later diff syncs skip them
   because the middle replica already has them. Worse, at the next
   re-handshake the relayed `'H'` merges the origin's version vector
   downstream while the following (empty) diff carries no data, so the
   downstream VV gets poisoned and the loss becomes permanent and silent —
   TLC finds a 27-state trace ending with `gvv[c][a] = 1` and `store[c] = {}`.
   Fix: rebroadcast exactly the locally-applied prefix of every drained
   batch, minus the session-scoped trailing `'B'` (`relayApplied`, backed by
   `DrainApplied` which reports how many records of a batch were applied).

2. **Sync-point displacement never fired** (`chotki.go drain 'H'`, modelled
   by `DisplaceBySrc = FALSE`). The `"allow only 1 diff sync per src"`
   cleanup compared sync-point keys against `cho.src` — the *local* replica
   id — but sync-point keys are the *origins'* snapshot ids, and a replica
   never drains its own handshake, so the cleanup deleted nothing. Stale
   sync points (with their pebble batches) from an origin's previous,
   interrupted session survived until the `cleanSyncs` timeout. Fix: compare
   against the source of the incoming handshake id.

3. **Sync points outlived the session that fed them** (modelled by
   `DropSyncsOnClose = FALSE`). This one was found *by TLC*, in a
   first-draft `MCFixedChurn` run with bugs 1–2 already fixed: `cho.syncs`
   is keyed only by the origin's snapshot id, so a pending diff batch
   survives connection churn. TLC's 30-state counterexample: while `a—b`
   diff-syncs, `b` relays `H(a)` to `c` (sync point at `c`); the `b—c`
   link then gracefully reconnects, and the relayed `D(a)` dies in the
   old connection's queue; `b` (whose `a`-session is still live) relays
   the closing `V(a)` into the *new* `b—c` connection; `c` still holds
   the stale sync point and applies it — the staged handshake VV lands in
   `VKey0` with no data (`gvv[c][a] = 1`, `store[c] = {}`). In production
   terms: any network blip on a downstream link while an upstream diff
   sync is being relayed through it can permanently and silently lose
   data downstream. Fix: every sync point remembers the replication
   session that created it (`Syncer.SessionId()`, carried in the drain
   context) and `Syncer.Close` aborts its sessions' sync points
   (`Chotki.AbortSyncsVia`) — a late `V` on a newer connection then gets
   `ErrSyncUnknown`, and the session restarts with a clean re-handshake.

4. **`cho.last` could lag behind own-source records** (modelled by
   `ProtectLast = FALSE` and `EnableOwnSourceDrain = TRUE`). `CommitPacket`
   stamps a new local mutation from the cached `cho.last` value, while
   `drain` can also apply records whose id source is the local replica and
   advance the replica's own VV.  Without a synchronization edge around that
   cache, the next commit can observe stale `cho.last` and reuse an already
   issued seq.  The `MCBuggyLast.cfg` trace is deliberately tiny: an
   own-source drain applies seq 1 but leaves `last = 0`, then `CommitPacket`
   also emits seq 1, violating `FreshLocalIds`.  In the Go code the two
   paths that advance `cho.last` really do not share a lock: local commits
   hold `commitMutex`, while replication sessions drain own-source records
   (our own history synced back after a restore from an older snapshot)
   under `cho.lock.RLock` only — and `rdx.ID` is two uint64s, so a torn
   read is possible on top of the lost update.  Fixed by giving the
   allocator cache its own mutex (`lastLock`) around the commit allocation
   (`nextLast`), the own-source advance in `drain`, `Last()` and the
   `Close()` reset; `TestCommitAllocatorSyncedWithOwnSourceDrain` races the
   two paths under `-race` and fails against the pre-fix code.

5. **Batched drain could advance `cho.last` past the persisted VV** (modelled
   by `BatchMode = TRUE` with `BatchBuggy = TRUE`). `chotki.go drain()` was
   changed to accumulate the `Y/C/O/E` records of a batch into one pebble
   `pb` applied once after the loop (one write per drain), instead of
   per-packet. The first draft advanced `cho.last` to an own-source record's
   id *inside the loop* — before that durable write — and dropped `pb`
   entirely on a mid-batch error (a bad packet, `ErrSyncUnknown` after a sync
   point timed out, an unsupported type). The record's version-vector bump
   was dropped with `pb`, but `cho.last` stayed advanced: `cho.last` now
   *leads* the persisted VV. Since `cho.last` is rebuilt from the VV on
   restart (`cho.last = vv.GetID(cho.src)` in `Open`), a crash before a
   resync regresses it, and the next `CommitPacket` reissues an id a peer that
   received the drained/relayed record already holds — divergence. The
   `MCBatchBuggy.cfg` trace is one step: a dropped own-source drain advances
   `last` to 1 while `gvv` stays 0, violating `LastNotAhead`. Fixed by (a)
   flushing the applied prefix even on error, so a drained own-source record
   is always durable before it is relayed and `cho.last` never leads the VV
   for long, and (b) advancing `cho.last` (under `lastLock`) only *after* that
   durable write. `TestDrainErrorDoesNotOverrunAllocator` drives a mixed
   `[own-source C, unknown-sync V]` batch and fails against the pre-fix code.
   `MCBatchFixed`/`MCBatchFixedLast` re-check the whole property set with
   `BatchMode = TRUE`.

Bugs found in the same code while studying it for the model (also fixed, not
modelled at the byte/timer level):

3. `processPings` removed `'P'` records while ranging over the same slice,
   skipping the record that followed a removed one (a coalesced `[ping, X]`
   batch left `X` unprocessed by the ping logic and could relay a stray
   record); rewritten as an in-place filter.
4. `WaitDrainState` leaked one goroutine per call whenever the context could
   not be cancelled (`Feed`'s `SendNone` path passes `context.Background()`,
   so every closed session leaked a goroutine blocked on `ctx.Done()`), and
   another goroutine whenever the caller abandoned the wait on timeout (the
   result channel was unbuffered); fixed with an internal cancel and a
   buffered channel.
5. `Feed`'s `SendPing` case mutated `sync.pingTimer` without holding the
   lock, racing `resetPingTimer` on the drain side; now locked.
6. `ApplyD`/`ApplyV` could spin forever on a truncated inner TLV record
   (`protocol.Take` returns the input unchanged on incomplete data) and
   `ApplyV` could overwrite an earlier `ErrBadVPacket` with a later
   successful merge; both now fail fast. A malformed relayed handshake could
   also make `ApplyH` merge a nil VV into `VKey0`.
7. `drain`'s `'H'` case stored the sync point (and previously leaked the
   displaced batch) even when `ApplyH` failed; the batch is now closed and
   the sync point only registered on success.
8. A batch that became empty after ping filtering went through the
   `LastLit`-based state transition with `LastLit == 0` and could flip the
   drain FSM to `SendLive` prematurely (or panic on `recs[0:1]` in the
   handshake state); such batches now return early.
9. `Syncer.Close` closed the pebble snapshot and its iterators while a
   concurrent `Feed` (`FeedBlockDiff`) could still be using them — a data
   race that `TestSyncPointRace_ReconnectionStorm -race` turns into a
   `mergingIter` panic; snapshot/iterator lifetime is now guarded by a
   dedicated mutex (`snapLock`).
10. `Feed`'s `SendNone` case waited for the drain state to reach
   `SendNone`, unblocking itself via a one-shot timer that forced the
   state — but a `Drain` racing with `Close` (e.g. a late handshake)
   could move the drain state backwards *after* the timer had fired,
   leaving the feed goroutine blocked forever (the reconnection-storm
   test hangs on this both before and after the other fixes). The
   lingering wait is now bounded by a context timeout instead.
11. Draining a peer's `'B'` (bye) set the drain state to `SendNone`, and
   `Feed`'s next call surrendered to that state *immediately* — even in
   the middle of its own diff. In a bidirectional non-live sync the
   faster side's bye could therefore cut the slower side's diff short of
   its `'V'` packet, stranding a staged diff batch at the peer that
   silently misses the promised data (`TestChotki_Sync3` flakes on
   exactly this). A bye only means the peer has nothing more to *send*;
   its drain side keeps working until the connection closes, so `Feed`
   now finishes its handshake/diff phase before winding down.
12. `examples/plain_object_test.go` never closed its third DB handle, so
   pebble's background compactions raced the deferred `os.RemoveAll` and
   failed the test after it had passed; `chotki_index_test.go` waited
   only 5s for the background reindex worker, which a loaded `-race` run
   can exceed (now 30s; the helper polls, so fast machines stay fast).
