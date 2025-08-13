# Architecture overview

Chotki is a CRDT-based replicated database. Chotki has no master
server and no consensus algorithm. All Chotki objects consist of
fields, and fields are CRDTs. That means any edits to any field
are mergeable. `git` is a good analogy here: merging, no master!

One change to one field is called an _op_. An op is emitted by a
_source_, which is a writeable _replica_. A bunch of ops emitted
and processed atomically is called a _packet_.

The key to understanding Chotki is its use of 128-bit identifiers
able to reference any replica, object, field, op, or packet in
the system. Chotki ID bit layout, most significant bit to least significant bit:

1.  source aka `src`, 64 bits, the ID of the replica,
2.  sequence number aka `seq`, 50 bits, all replica's packets
    are numbered sequentially,
3.  offset aka `off` is either the op's 12-bit offset in a
    packet or, if we address within an object, the "offset" of a
    field contains:
    1.  field number (1, 2, 3...) of 7 bits and
    2.  field type ('C', 'S'...) of 5 bits.

IDs are serialized in hex, like `8e-82f0-32` (src-seq-off).

Each op has its _own_ 64-bit ID as well as a _reference_ to the
location it is being applied to (which is also a 128-bit ID).
References are very essential and powerful building blocks of
CRDTs. They allow for very precise and fully deterministic
application of changes in the data graph. No IDs - no CRDTs.
While a typical CRDT architecture employs Lamport timestamps for
IDs, Chotki IDs are a bit cheaper than that. Their only function
is addressing; they have no logical-timestamp or monotony
properties. CRDTs that depend on logical time implement it on
their own.

Note the difference with all the classic databases, like
PostgreSQL, LevelDB, MySQL, or you-name-it. They all use
plain sequential numbering to refer to a transaction, as they
are NOT distributed. They use one linear op log. Distributed
systems, like Cassandra, often use UUIDs for identifiers and
also timestamps for conflict resolution. That allows the use of
per-replica op logs with no order guarantees whatsoever. The
nice term for that is _eventual consistency_. Chotki maintains
**causal consistency**, and that allows for useful same-source
sequential numbering. Replica op logs are _partially ordered_ —
that is a formal term! Chotki replica sync protocol relies on
that rather heavily. Chotki _can_ survive a breakdown of the
causal order, although that would not be an efficient mode of
operation.

# Data model

There are 3 basic entities in Chotki:

1. Replica object — immutable object that defines some properties of the current replica like name
2. Class — a kind of objects, defines which fields an object should have and their properties (like indexes)
3. Object — an instance of a class

All object values are RDX values.

# RDX

Can be easily described as mergeable JSON. For all of the types there is a merge operation defined,
meaning that applying all of the updates in random order will yield the same result every time.

**FIRST-types**
Float, Integer, Reference, String, and Term. We can also call them last-write-wins,
meaning that they are not CRDT values in a sense that updates to them are commutative, but they still have
automatic conflict resolution via revisions.
Reference is a special type that we also call rdx.ID. Term is a stub for many different strings.

**ELM-types**
Eulerian (set), Linear (list), Map. These are structures that can host FIRST or CRDT values.
They are mergeable, though again, updates are not commutative. In case of conflicts they are resolved in the
same way as FIRST values.

**CRDT-types (NZ)**
Currently N-counters (grow-only) and Z-counters (arbitrary operations).
These are real CRDT values for which updates should be partially ordered (meaning that there should be order for each replica's updates).
One important things that those type of counters can be very efficiently updated in concurrent-safe fashion.
For more details look at [../counters](../counters)

**System types (OYCV)**
O — system type, zero offset of each object is O field that contains reference to the class this object represents
Y — replica object, unused
C — class object, this is actually an append-only log of terms
V — version vector, it's a list of rdx.IDs

Each type has a number of helpers

```go
    // produce a text form (for REPL mostly)
    func Xstring(tlv []byte) (txt string)
    // parse a text form into a TLV value
    func Xparse(txt string) (tlv []byte)
    // convert plain golang value into a TLV form
    func Xtlv(i plain) (tlv []byte)
    // convert a TLV value to a plain golang value
    func Xplain(tlv []byte) plain
    // merge TLV values
    func Xmerge(tlvs [][]byte) (tlv []byte)
    // produce an op that turns the old value into the new one
    func Xdelta(old_tlv, new_tlv []byte) (tlv_delta []byte)
    // checks a TLV value for validity (format violations)
    func Xvalid(tlv []byte) bool
```

The most important is merge operator, this is the core functionality of RDX it allows us to
deterministically merge any number of RDX objects of the same type.

More in [../rdx](../rdx).

# Protocol

Chotki employs a very simple TLV protocol called ToyTLV: each
record is prepended with its type `[A-Z]` and length (1 or 4
bytes). A record's body can contain nested records.

It's important to understand that ops and data types are separate things.

Each op has _ID_ and _ref_ (both are 64-bit IDs). Ops are always
shipped in packets. There are a number of optimizations to
abbreviate the metadata (IDs, refs).

There are more detailed explanations in [../protocol](../protocol)

We now describe the currently implemented types of ops.

**O operation**

Create object operation.

`{O I R [RDX]+}`
I — ID
R — reference
RDX — an arbitrary amount of RDX structures which represent object fields

**C operation**

Create or update class operation.

`{C I R [T]+ }`

I — ID
R — reference
It is followed by an arbitrary number of terms which represent class fields. Each class field is a T; in its revision field, the index is stored and the value is a field name — a simple byte string.

**H operation**

Diff sync handshake operation.

`{H T M V S}`

T — it's the last seen ID of a snapshot for which we perform diff sync with another replica, used to retrieve sync point
M — unused field
V — global version vector
S — trace ID, part of UUID that is used in logs so we can find all of the logs for both replicas about the sync session

**D operation**

Diff sync parcel.

`{D T R [F RDX]+}`

T — it's the last seen ID of a snapshot for which we perform diff sync with another replica, used to retrieve sync point
R — it's an offset of a block that allows us to save some space on rdx.ID of values
F — it's an offset for a value, so to get value rdx.ID we need to do R + F
RDX — value itself that we're syncing

**V operation**

End of diff sync operation that passes sync block version vectors.

`{V T [V R]+}`
T — it's the last seen ID of a snapshot for which we perform diff sync with another replica, used to retrieve sync point
V — block version vector
R — block ID

**E operation**

Edit field operation.

`{E I R [F RDX]+}`

I — ID
R — reference
F — offset of the field we're editing
RDX — rdx value itself

# Replication protocol

The protocol operates through a finite state machine with the following states:

    SendHandshake → SendDiff → SendLive → SendEOF → SendNone
                        ↓         ↕
                      SendPing ← → SendPong

State descriptions:

- SendHandshake: Initial connection setup and capability negotiation
- SendDiff: Bulk synchronization of historical data differences
- SendLive: Real-time streaming of new changes as they occur
- SendPing/SendPong: Keep-alive mechanism during live sync
- SendEOF: Graceful connection termination
- SendNone: Connection closed state

More details in [../replication](../replication).

# Networking

Chotki implements a high-performance, callback-driven networking layer optimized for continuous bidirectional streaming between replicas.

Currently there is an important limitations each replica can connect to only replicas which have lower source id.

## Key Features

- **Multiple Protocols**: TCP, TLS, and planned QUIC support
- **Automatic Reconnection**: Exponential backoff retry (500ms → 60s max) with persistent connection pools
- **Intelligent Buffering**: Adaptive batching to minimize syscalls and maximize throughput
- **Full Duplex**: Independent read/write goroutines for concurrent bidirectional communication
- **Connection Resilience**: Automatic failure detection and transparent reconnection

## Architecture

Each connection is managed by a `Peer` instance that handles:

- **Buffered I/O**: MTU-aligned (1500 bytes) buffer growth with configurable thresholds
- **Protocol Integration**: Bridges network transport with TLV protocol processing via `Feed()`/`Drain()` callbacks
- **Flow Control**: Read accumulation until `bufferMinToProcess` threshold or `readAccumTimeLimit` timeout
- **Performance Metrics**: Lock-free atomic operations for thread-safe monitoring

## Connection Management

**Outbound**: `Connect("tcp://host:port")` spawns persistent goroutines with automatic retry logic
**Inbound**: `Listen("tcp://:port")` accepts connections and wraps them in identical `Peer` processing

The network layer automatically:

- Creates protocol handlers per connection via install callbacks
- Manages connection lifecycles with graceful cleanup
- Provides connection pooling to prevent duplicates
- Handles TLS termination when configured

More details in [../network](../network).

# Physical storage

Chotki uses [Pebble](https://github.com/cockroachdb/pebble), a high-performance LSM-tree based key-value store. This choice is strategic because LSM-trees excel at write-heavy workloads and support custom merge operators, which are essential for CRDT operations.

## Merge Operator Integration

The core innovation is implementing RDX merge semantics as Pebble's merge operator. Instead of read-modify-write operations, all updates become merge operations:

- **Write path**: Every operation (object creation, field updates, sync) becomes a `batch.Merge()` call
- **Compaction**: During LSM compaction, Pebble automatically applies RDX merge rules to combine values
- **Conflict resolution**: CRDT merge semantics handle concurrent updates deterministically
- **Performance**: Leverages LSM-tree's optimized merge infrastructure for high write throughput

The merge operator detects RDX type from the key's last byte and delegates to the appropriate `Xmerge()` function.

# Physical layout

Data is stored as key-value pairs where keys encode both identity and type information, enabling efficient range scans and type-specific operations.

## Key Structure

All keys follow a consistent 18-byte structure:

```
[prefix:1][src:8][pro:8][type:1]
```

- **Prefix**: Single byte indicating entity type (`'O'` for objects, `'V'` for version vectors, `'I'` for indexes)
- **Source**: 8-byte big-endian replica ID
- **Progress**: 8-byte big-endian sequence + offset
- **Type**: RDX type byte (`'F'`, `'I'`, `'S'`, etc.)

This design enables:

- **Locality**: Related data (same object fields) stored together
- **Ordering**: Natural sort order for efficient range scans
- **Type safety**: Type byte prevents merge conflicts between different RDX types

## Objects layout

Each object occupies a contiguous key range determined by its ID. Object fields are stored as separate key-value pairs:

```
O[src][seq][0]O  → class_id           // Object header (type 'O')
O[src][seq][1]F  → field1_value       // Field 1 (Float)
O[src][seq][2]S  → field2_value       // Field 2 (String)
O[src][seq][3]N  → field3_value       // Field 3 (N-counter)
...
```

- **Field 0**: Always type `'O'`, contains reference to the object's class
- **Field N**: Application fields ordered by index, typed by RDX type
- **Range scans**: Object prefix `O[src][seq]` enables efficient field enumeration

## Classes layout

Classes are special append-only objects storing field definitions:

```
O[class_src][class_seq][0]C  → field_definitions
```

The value is an append-only log of Terms (`T` records), where each term represents a field definition with:

- **Index**: Stored in the term's revision field
- **Name**: Field name as UTF-8 byte string
- **Immutability**: Fields cannot be deleted, only added (append-only semantics)

## Version Vectors

Version tracking uses two key patterns:

- **Global VV**: `V[0][0][0]V` → maps all replica IDs to latest seen sequence numbers
- **Block VV**: `V[block_src][block_seq]V` → tracks sync block progress for differential sync

## Indexes

The IndexManager maintains additional key ranges for query optimization:

- **Full-scan index**: `IF[class_id][object_id]T` → chronological list of class instances
- **Hash index**: `IH[class_id][field_id][hash]E` → field value → object ID mapping for O(1) lookups
- **Reindex tasks**: `IT[class_id]M` → tracks background reindexing state

All index updates are transactional with object changes using Pebble batches, ensuring consistency.

More details in [../indexes](../indexes).
