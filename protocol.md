##  Chotki data model and protocol

Chotki is a CRDT based replicated database.
Chotki has no master server and no consensus algo.
All Chotki objects consist of fields, and fields
are CRDTs. That means, any edits to any field are
mergeable. `git` is a good analogy here: merging,
no master!

One change to one field is called an *op*. An op is
emitted by a *source*, which is a writeable *replica*.
A bunch of ops emitted and processed atomically is 
called a *packet*. 

The key to understanding Chotki
is its use of 64-bit identifiers able to reference
any replica, object, field, op or packet in the system.
Chotki id bit layout, m.s.b. to l.s.b.:

 1. source aka `src`, 20 bits, the id of the replica,
 2. sequence number aka `seq`, 32 bits, all replica's
    packets are numbered sequentially,
 3. offset aka `off` is either the op's 12-bit offset in a 
    packet or, if we address within an object, the
    "offset" of a field contains:
     1. field number (1, 2, 3...) of 7 bits and
     2. field type ('C', 'S'...) of 5 bits.

Ids are serialized in hex, like `8e-82f0-32` (src-seq-off).
    
Each op has its *own* 64-bit id as well as a *reference*
of the location it is being applied to (which is also a
64-bit id). References are very essential and powerful
building blocks of CRDTs. They allow for very precise and
fully deterministic application of changes in the data
graph. No ids - no CRDTs. While a typical CRDT architecture
employs Lamport timestamps for ids, Chotki ids are a bit
cheaper than that. Their only function is addressing, they
have no logical-timestamp or monotony properties. 
CRDTs that depend on logical time implement it on their own.

Note the difference with all the classic databases, like
PostgreSQL or LevelDB or MySQL or you-name-it. They all use
plain sequential numbering to refer to a transaction, as
they are NOT distributed. They use one linear op log. Distributed 
systems, like Cassandra, often use UUIDs for identifiers and
also timestamps for conflict resolution. That allows the use of
per-replica op logs with no order guarantees whatsoever.
The nice term for that is *eventual consistency*.
Chotki maintains **causal consistency**, and that allows for useful
same-source sequential numbering. Replica op logs are *partially
ordered* -- that is a formal term! Chotki replica sync protocol
relies on that rather heavily. Chotki can survive a breakdown
of the causal order, although that would not be an efficient mode of
operation.

##  Serialization and the protocol

Chotki employs a very simple TLV protocol called ToyTLV:
each record is prepended with its type `[A-Z]` and length (1 or 4 bytes).
A record's body can contain nested records.

Each op has *id* and *ref* (both 64 bit its).
Ops are always shipped in packets.
There is some number of optimizations to abbreviate
the metadata (ids, refs).

Packets may have different internal structure. 
They can be:

 1. new object creation packets (an op per a field),
 2. multiple-object edit packets (an op per field change),
 3. object state snapshots (may have zero or many ops per a field),
 4. diffs / changesets. 

All the particular packet layouts are better seen in the code.
