# Protocol Overview

The Chotki replication protocol enables peer-to-peer synchronization of CRDT-based
databases between replicas without a master server or consensus algorithm. The protocol
maintains causal consistency and supports efficient differential synchronization.

# Synchronization States

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

# Synchronization Modes

The protocol supports different synchronization modes via bitmask flags:

    SyncRead   (1): Can receive data from peer
    SyncWrite  (2): Can send data to peer
    SyncLive   (4): Supports real-time live synchronization

Common combinations:

- SyncRW = SyncRead | SyncWrite (bidirectional batch sync)
- SyncRL = SyncRead | SyncLive (read + live updates)
- SyncRWLive = SyncRead | SyncWrite | SyncLive (full bidirectional)

# Protocol Flow

## 1. Handshake Phase

The synchronization begins with a handshake message:

    H(T{snapshot_id} M{mode} V{version_vector} S{trace_id})

Where:

- H: Handshake packet type
- T: Snapshot timestamp ID (current replica state)
- M: Sync mode bitmask (read/write/live capabilities)
- V: Version vector (TLV-encoded replica states)
- S: Session trace ID (for logging and debugging)

The handshake establishes:

- Peer capabilities and sync mode
- Version vectors for differential calculation
- Session trace IDs for request tracking

## 2. Diffsync Phase

After handshake, the protocol sends block-based diffs:

    D(T{snapshot_id} R{block_range} F{offset}+)

Where:

- D: Diff packet type
- T: Reference snapshot ID
- R: Block range being synchronized
- F: Frame offset within block followed by operation data

The diff phase:

- Compares version vectors to identify missing data
- Sends data in blocks with maximum size of 100MB (MaxParcelSize)
- Uses CRDT-specific diff algorithms (rdx.Xdiff) for efficiency
- Processes blocks sequentially until all differences are sent

## 3. Version Vector Exchange

After diff completion, version vectors are exchanged:

    V(R{block_range} version_vector_data)

This finalizes the differential sync by confirming the new state.

## 4. Live Sync Phase (Optional)

If SyncLive mode is enabled, the protocol enters real-time sync:

- Streams new operations as they occur via the outbound queue
- Maintains connection with periodic ping/pong messages
- Ping interval and timeout are configurable per connection

Ping/Pong messages:

    P("ping")  - Keep-alive ping
    P("pong")  - Keep-alive response

## 5. Termination

Connections end with a bye message containing the reason:

    B(T{final_snapshot_id} reason_text)

# Versioning

Key component of the protocol is the version vector. Each time we change data on any replica, this change
is stamped by an rdx.ID (autoincrementing sequence number + source replica id). This rdx.ID will then help us to
determine which things we need to sync.

Version vector is maintained in VKey0, so every time we see change from the same replica or from other replica,
we update VKey0
Versions vector or (VV) layout is basically a map: src_id -> last seen rdx.ID. So, we always no which latest event we saw from each replica.

There is a special src_id = 0, which is a bunch of objects created in Log0, so we could have some common system objects
on each replica. So when those objects are updated they we also store them like: 0 -> last seen rdx.ID in the VV.

# Handshake

This is the first thing happens when we connect to a new replica. We init the sync session.
But most importantly we send our VV (also we send trace_id for logging) and we also create pebble snapshot, that
we will use during diffsync. When we just connected, we start to accumulate live events, until we completed diffsync.
We will apply them later, while we do not guarantee that created snapshot do not contain any live events from the queue,
we do not care as all operations in chotki are idempotent.

# Diff sync

As described above, when replicas first connect after handshake they enter diff sync state. The idea is that
before we start live sync, we need to equalize the state between 2 snapshots we made on previous step.
In diff sync we sync data using blocks. Block is basically a range of rdx.ID, so when we update our VV,
we then take this rdx.ID and apply SyncBlockMask to it (basically cutting first N bits from it) and consider it a block.
Then for each block we will also maintain a VV for all updates associated with this block, meaning if DB update occurs,
we update 2 VVs:

- global VV (VKey0)
- block VV (VKey(block_id))

The algorithm of diff sync is as follows:

1. We take a block and look at its VV, then we look at other replica global VV
2. If other replica has not seen something from this block, we start syncing of this block
3. We start iterating all objects inside this block (with rdx.IDs that conform to this block range)
4. If other replica has not seen this object at all (meaning its VV for the src of this object is non existent or smaller than this object rdx.ID), then we just send this object
5. Ohterwise, we use XDiff, however in a majority of cases it will just send whole object anyway for simplicity. It may change in future.
6. When we sync all blocks we then exchange VVs of blocks we synced, so other replica can update them.
7. Each replica accumulate updates in pebble.Batch, and when it receives V packet in the end, it applies it to the DB.\

Important note that during diff sync we also broadcast all 'D' and 'H' packets to all other replicas.
Imagine there are 3 replicas: A <-> B <-> C.

- Let's say A, B are live syncing and C is just connected to B.
- If we do not broadcast 'D' packets, then B and C will sync, however if there is something in C, that A haven't seen it will not be synced, until diff sync between A and B.
- But if we broadcast 'D' and 'H' packets, we basically open syncing session between all upstream replicas and C, so
  they will sync all the data from C

Unfortunattely, it can create a lot of excess traffic, especially when we rollout new version and there are a lot of diffsyncs happenings.
But it simplified protocol a lot.

# Live sync

After diff sync, we now can process all updates that were accumulated in the queue and continue process them as we go.
When we receive a bunch of records during live sync, we apply them to the DB immediately and also broadcast then to all other replicas.
Due to the fact that currently chotki only allows tree replication structures, we know that we can safely send events to all connected replicas without fear of loops.
