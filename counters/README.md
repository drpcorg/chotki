# AtomicCounter & AtomicCounterManager

`AtomicCounter` is a **lock-free** CRDT counter (Natural increment-only or ZCounter
two-way). `Get`/`Increment` touch only in-memory atomics; an `AtomicCounterManager`
background goroutine periodically persists the local contribution and reloads other
replicas' contributions.

## Model

- Each field tracks `mine` (this replica's contribution, `atomic.Int64`) and `theirs`
  (the last-loaded sum of all other replicas, `atomic.Int64`).
- `Get()` returns `mine + theirs`.
- `Increment(v)` adds to `mine` (Natural rejects `v < 0`).
- The background goroutine, every `Options.CounterSyncPeriod`, **flushes** all changed fields
  in **batched commits** — up to 1024 fields per Pebble batch + broadcast — and **reloads**
  `theirs` for fields touched since the last tick (idle fields cost nothing). A flush is a
  read-modify-write under the host's sequential-write bracket: it reads the current own-src
  slot and commits slot + unflushed delta (never the absolute cached `mine`), so a concurrent
  writer to the same slot (e.g. an ORM counter "set") is not clobbered and the merge cannot
  silently drop either write.

Local increments are visible immediately via `Get`. Other replicas' increments become
visible after the next reload. Obtain a counter with `cho.Counter(rid, offset)`.

## Durability tradeoff

Increments live in memory between flushes. A **graceful** `Close()` flushes everything; a
**hard crash** (panic / kill -9 / power loss) loses increments since the last flush. This is
the deliberate tradeoff for a lock-free hot path. Absent a crash, **no event is missed**:
the unflushed delta (`mine - lastSynced`) survives a skipped, coalesced or failed flush and
is carried into the next one.

## Forcing a cycle

`cho.SyncCounters(ctx)` runs one flush + full reload synchronously (used in tests and when a
caller wants an immediate, complete sync). The background ticker uses the same machinery but
only reloads fields touched since the previous tick (and retries any that failed to load).

## Example

```go
c := cho.Counter(objectID, fieldOffset)
c.Increment(ctx, 5)   // in-memory; no DB write
v, _ := c.Get(ctx)    // 5, immediately
// ... background goroutine flushes within CounterSyncPeriod ...
```
