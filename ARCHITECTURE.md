# Chotki: a CRDT database

## Formal model

The smallest unit of change is an operation (_op_). It is a
serialized change to a _field_ of a given CRDT type. Operations
are batched into _packets_. Packets get applied atomically (all
ops or no op). There are several varieties of packets: object
creation, object edit, multi-object edit, etc. These varieties
are completely orthogonal to op/field types. The canonic
serialization for ops and packets is ToyTLV based. One packet
can be up to a page in size (4096 bytes) hosting up to 4096 ops
(nominally).

The system employs 64-bit IDs that can address objects, fields,
packets and ops within a packet. ID bit layout is `src-seq-off`
where

1.  `src` is the replica id (first 20 bits),
2.  `seq` is a packet's sequence number (32 bits),
3.  `off` is the op/field offset within a packet/object (12 bits).

Packets issued by each replica get numbered sequentially. IDs
are rendered in hex, like `2a8-5e2041-0`.

Fields are mutable values of certain CRDT types. An _object_
contains a number of fields of certain types, in accordance with
its _class_. A _class_ is a meta-object containing a number of
field descriptions (types and names). Classes get mutated by
ops, same as other objects. (Normally, only the author of a
class can mutate it.) When mapping the Chotki model onto
relational model, a class becomes a relational table. When
mapping to struct/objects, a class is a type/class.

Packets form a partially ordered log. The causal DAG order of
that log is defined by A-packets which contain cross-references
between replicas. For example, A-packet `2a8-5e2005` references
`7b-49c2` thus forming a causal link between replicas `2a8` and
`7b`. De-facto order of packets in logs of different replicas
may differ while obeying the causal order. In other words, if
two packets are causally linked, they go in the same order
everywhere. (Causal link is a chain of A-references leading from
later one to the earlier one, see Minkowski.)

The database reads ops from the log and merges them into
respective fields in a key-value store. When a user requests an
object, its fields are scanned and a struct is produced.

## Performance

When employing an LSM state storage (leveldb, RocksDB,
pebbledb), the database is optimized for write-intensive
workloads, which may proceed at a rate close to hardware
bandwidth. Meanwhile, reads should normally top at 5-10K per
second per thread.

Namely, a Chotki database can pump new packets into the LSM
state without ever reading back. That means no read-amend-write
round-trips (see merge operators).

## (Replicated) Data types

Each object has a number of fields in accordance with its class
definition. Each field is a CRDT value. That means, each field
value converges irrespectively of the order of the ops. Broadly,
once the same set of ops was applied to two replicas, their
state will be identical.

_Trivial types_ supported by Chotki are uint64, int64, float64,
string, id64) _Field types_ (CRDTs) supported by Chotki are:

1.  last-write-wins fields of trivial types (LWW)
2.  arrays of trivials (CT/RGA)
3.  maps of trivials keyed by trivials (LWW maps)
4.  counters (LWW is no good for concurrent counters)

Within an LSM state storage, one field is one key-value pair.
The key is an ID. Writes are implemented as type-dependent merge
operators applying ops to field values.

## Syncing modes

The simplest form of syncing is snapshot duplication which is
basically copying database files over. All the other forms boil
down to feeding packet logs one-another. Packets are always fed
with respect to the causal relations. That means:

1.  no packet `a-b` is accepted if `a-c` is missing (a is a
    replica, b, c are sequence numbers, c=b-1, b is not 0)
2.  no A packet is accepted if its referenced packet is missing

Log-syncing can be batched or real-time. It can affect the full
log or be limited by a version vector to retrieve a specific
snapshot/subset of the data. Mode of the syncing gets conveyed
in the connection handshake.

## Versioning

By default, the head state reflects all the ops from the local
log. Still, it is possible to produce snapshots defined by a
version vector. It is also possible to limit the head state by a
version vector to ignore some of the data. That makes it
possible to run multiple heads as well. Snapshots can be
storage-efficient in many cases if the LSM database can share
sst files among them (rocksdb can).

## CRDTs, their storage format and merge operators

Chotki supports a variety of CRDT types. Each type is denoted by
a liter `[A-Z]`. That letter appears in the field declaration,
log ops, state keys and values. To implement a type one has to
implement the following functions (where X is the type's liter):

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

These functions require some obvious correctness invariants. For
example, `Xdelta` must round-trip with `Xmerge`. Note that
`Xplain` and `Xtlv` signatures are type-specific, e.g. for
signed counters that is `Ctlv(i int64) []byte` Also, various
types may benefit from type-specific functions, e.g. `Cadd(inc
int64) []byte` to generate an increase of a counter without
looking at the current value.

That is mostly enough to implement log, state and REPL. Here is
a standalone usage example involving an inner state `old_tlv`
and a trivial value `x`:

```go
    obj_field := ParseID("b82-5047")
    old_tlv := db.GetFieldValue(obj_field)
    x := Xplain(old_tlv) // 1
    x = 2
    new_tlv := Itlv(x)
    delta := Idelta(old_tlv, new_tlv)
    err = db.CommitEditPacket(
    	EditOp(obj_field, delta)
	)
```

### Last-write wins

LWW is the simplest type that qualifies as a CRDT. All writes
are "timestamped", the later write wins. (Cassandra is an
example of almost exclusively LWW database.)

ToyTLV storage format:

1.  int64: `I{t i}`, where `t` is 32-bit timestamp and `i` is a
    zig-zag and zip packed int64
2.  float64: `F{t f}`, where `f` is a zipped 64-bit float
3.  string: `S{t s}`, where `s` is an UTF-8 string
4.  id64: `D{t d}`, where `d` is a pair-zipped id64 (see the
    code)

### Arrays

Arrays are the most complicated CRDT types as they require a
tree-like inner structure. The underlying algorithm is Causal
Trees also known as Replicated Growable Array.

### Maps

Maps are essentially vectorized LWWs. Map value is a sorted list
of triplets `(key, value, time)`.

The storage format for `map[int64]int64` is like `M{t Ik Iv}*`,
where `p` is a zig-zagged pair-zipped key and value.
For `map[string]string` that would be `M{t Sk Sv}*`, etc

### Counters

Counters as simple as LWWs or even simpler. The database
guarantees idempotence, so the merge operator is relieved of any
work but adding numbers up.

The storage format for counters is `Cv`, where `v` is the
accumulated value.
