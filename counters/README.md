# AtomicCounter Architecture

AtomicCounter provides atomic increment operations in distributed environments while
optimizing performance through intelligent caching. It supports Natural (increment-only)
and ZCounter (two-way) types with CRDT merge semantics.

## Core Design Principle

The counter trades CPU usage for data freshness, but **only for data from other replicas**.
All writes to the current replica are immediately reflected in the counter value.
Caching only affects how frequently we read data that arrived via synchronization.

## How It Works

The counter uses a lazy loading pattern with time-based caching. When data is requested:

1. **Cache Check**: If cached data hasn't expired, return it immediately
2. **Database Load**: Otherwise, load fresh data from the LSM database
3. **Parse & Cache**: Parse TLV data into internal structures and cache with expiration

For increments, the process is:

1. **Load Data**: Get current counter state (cached or from DB)
2. **Atomic Update**: Use Go's atomic primitives to update the value
3. **Generate TLV**: Create TLV records for persistence
4. **Commit**: Write changes to database with CRDT merge semantics

## Internal Structure

The counter maintains two internal representations:

- **atomicNcounter**: For Natural counters, uses atomic.Uint64 for thread-safe increments
- **atomicZCounter**: For ZCounter, uses atomic.Pointer with revision tracking for conflict resolution

## Performance Trade-offs

The design trades CPU usage for freshness of **synchronized data from other replicas**.
With updatePeriod > 0, the counter caches data to avoid expensive database reads,
but may return slightly stale values from other replicas. Local writes are always
immediately visible. With updatePeriod = 0, it always reads fresh synchronized data.

## Thread Safety

Operations are atomic when using a single instance. Multiple instances may have
race conditions due to the distributed nature of the system.

## Example: Cache vs Local Writes

```go
counter := NewAtomicCounter(db, objectID, fieldOffset, 1*time.Second)

// Local write - immediately visible
counter.Increment(ctx, 5)  // Value: 5
value, _ := counter.Get(ctx)  // Returns 5 immediately

After sync from other replica (value: 10)
With cache: may still return 5 until cache expires
Without cache: immediately returns 15 (5 + 10)
```
