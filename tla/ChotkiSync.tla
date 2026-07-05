---------------------------- MODULE ChotkiSync ----------------------------
(***************************************************************************)
(* A TLA+ specification of the Chotki replication (sync) protocol, as     *)
(* implemented in:                                                         *)
(*                                                                         *)
(*   replication/sync.go   -- Syncer feed/drain state machines            *)
(*   chotki.go             -- Chotki.drain / Broadcast / CommitPacket     *)
(*   packets.go            -- ApplyH / ApplyD / ApplyV / ApplyE           *)
(*   network/peer.go       -- read-side batching (record coalescing)      *)
(*                                                                         *)
(* -------------------------- ABSTRACTIONS ------------------------------ *)
(*                                                                         *)
(* Replica DB state.  A replica's pebble DB is abstracted to:              *)
(*   store[r] : the set of operations (op = [src, seq, obj]) applied;     *)
(*   gvv[r]   : the global version vector VKey0 (src -> max seq seen);    *)
(*   bvv[r]   : per-object version vector, VKey(block).  A sync "block"   *)
(*              is modelled as a single object (1 block == 1 object);     *)
(*              this keeps the block/global VV distinction, which is      *)
(*              what the diff algorithm actually depends on.              *)
(*                                                                         *)
(* Operations.  All mutation packets ('O','E','C','Y', ...) behave        *)
(* identically w.r.t. the sync protocol: they are stamped with an id      *)
(* (src, seq), update the object they touch, and update both VVs.  They   *)
(* are modelled as a single record type "E" (a live op).                  *)
(*                                                                         *)
(* Diff filtering.  FeedBlockDiff sends, for a block with changes, either *)
(* the full object value (when the peer has not seen the object's        *)
(* creation) or rdx.Xdiff(value, sendvv).  Both cases amount to "all      *)
(* op-stamps in the block newer than the peer's global VV"; the model     *)
(* sends exactly the set {op : op.seq > peervv[op.src]} for the object.   *)
(*                                                                         *)
(* Network.  Each established connection is a pair of directed, ordered,  *)
(* reliable channels.  network/peer.go accumulates bytes and hands the    *)
(* Syncer *batches* of records (keepRead), so a Drain call receives an    *)
(* arbitrary non-empty prefix of the channel -- this coalescing is        *)
(* essential: several protocol decisions (LastLit) look only at the last  *)
(* record of a batch.                                                      *)
(*                                                                         *)
(* Sync modes.  chotki always runs SyncRWLive sessions (chotki.go,        *)
(* Open()); mode negotiation is therefore not modelled.                    *)
(*                                                                         *)
(* Timers.  Ping/pong timers, cleanSyncs() expiry and handshake timeouts  *)
(* are modelled as nondeterministic action enablement (a timer that "may  *)
(* fire").                                                                 *)
(*                                                                         *)
(* ------------------------------ BUGS ---------------------------------- *)
(*                                                                         *)
(* Three switchable constants model defects found in the Go code while    *)
(* writing this spec (see tla/README.md for TLC configs demonstrating     *)
(* them, plus the non-modelled fixes that came out of the same work):     *)
(*                                                                         *)
(*   BuggyRelay = TRUE models replication/sync.go Drain() before the fix: *)
(*     a drained batch whose last record is 'B' (bye) is NOT broadcast    *)
(*     at all, and a batch that fails mid-way relays nothing even though  *)
(*     a prefix was locally applied.  Downstream replicas permanently     *)
(*     miss the applied records (violates NoGaps / QuiescentConverged).   *)
(*   BuggyRelay = FALSE models the fixed behaviour: relay exactly the     *)
(*     locally-applied prefix, minus the session-scoped trailing 'B'.     *)
(*                                                                         *)
(*   DisplaceBySrc = FALSE models chotki.go drain() 'H' before the fix:   *)
(*     the "allow only 1 diff sync per src" cleanup compared sync-point   *)
(*     keys against cho.src (self), which never matches, so stale sync    *)
(*     points from an origin's previous session are never displaced       *)
(*     (violates SyncPointPerSrc).                                         *)
(*   DisplaceBySrc = TRUE models the fixed cleanup (compare against the   *)
(*     incoming handshake's source).                                       *)
(*                                                                         *)
(*   DropSyncsOnClose = FALSE models cho.syncs before the fix: a sync     *)
(*     point (pending diff batch) survives the session whose 'H' created  *)
(*     it, waiting only for the cleanSyncs timeout.  TLC finds (this bug  *)
(*     was discovered BY this model): a relayed 'H' creates a sync point  *)
(*     downstream; the relayed 'D' records die in the queue/channel of a  *)
(*     reconnecting downstream link; the relaying replica then feeds the  *)
(*     relayed 'V' into the NEW connection, which applies the staged      *)
(*     handshake VV without the data — permanent silent data loss         *)
(*     (violates NoGaps).                                                  *)
(*   DropSyncsOnClose = TRUE models the fix: sync points are bound to     *)
(*     the session that created them (Syncer.SessionId / AbortSyncsVia)   *)
(*     and are aborted when it ends.                                       *)
(***************************************************************************)

EXTENDS Naturals, Sequences, FiniteSets

CONSTANTS
    Replicas,       \* set of replica ids, e.g. {"a", "b", "c"}
    Edges,          \* tree topology: set of unordered pairs {x, y}
    Objects,        \* set of CRDT objects; 1 object == 1 sync block
    CommitBudget,   \* [Replicas -> Nat]: how many ops each replica commits
    GenBudget,      \* max number of sessions (connects) per edge
    PingBudget,     \* max pings initiated per endpoint per session
    BuggyRelay,       \* TRUE: model the pre-fix broadcast-relay behaviour
    DisplaceBySrc,    \* TRUE: model the fixed sync-point displacement
    DropSyncsOnClose, \* TRUE: sync points die with the session (fixed)
    EnablePing        \* include the ping/pong keep-alive machinery

ASSUME
    /\ \A p \in Edges : p \subseteq Replicas /\ Cardinality(p) = 2
    /\ CommitBudget \in [Replicas -> Nat]
    /\ GenBudget \in Nat
    /\ PingBudget \in Nat
    /\ BuggyRelay \in BOOLEAN
    /\ DisplaceBySrc \in BOOLEAN
    /\ DropSyncsOnClose \in BOOLEAN
    /\ EnablePing \in BOOLEAN

(***************************************************************************)
(* An endpoint <<x, y>> is the Syncer living at replica x serving the     *)
(* connection to replica y.  Its outbound records go to chan[<<x, y>>];   *)
(* it drains chan[<<y, x>>].                                               *)
(***************************************************************************)
Endpoints == {e \in Replicas \X Replicas : {e[1], e[2]} \in Edges}
Rev(e)    == <<e[2], e[1]>>
EdgeOf(e) == {e[1], e[2]}

Max(a, b) == IF a >= b THEN a ELSE b

ZeroVV        == [s \in Replicas |-> 0]
VVMerge(u, w) == [s \in Replicas |-> Max(u[s], w[s])]

(***************************************************************************)
(* Records (protocol packets).                                             *)
(*   H : handshake     -- H(T{snaplast} M{mode} V{vv} S{trace})           *)
(*   D : diff parcel   -- D(T{snaplast} R{block} F+{ops})                 *)
(*   V : diff-final VV -- V(T{snaplast} (V(R{block} vv))* )               *)
(*   B : bye           -- B(T{snaplast} reason)                           *)
(*   E : any mutation packet applied/relayed during live sync             *)
(*   P : ping/pong                                                        *)
(* A session key k == <<src, seq>> abstracts snaplast (the origin's own   *)
(* last id inside its snapshot): FeedHandshake sets                        *)
(* snaplast = hostvv.GetID(sync.Src).                                      *)
(***************************************************************************)
HRec(k, vv)     == [t |-> "H", k |-> k, vv |-> vv]
DRec(k, o, ops) == [t |-> "D", k |-> k, o |-> o, ops |-> ops]
VRec(k, bv)     == [t |-> "V", k |-> k, bv |-> bv]
BRec(k)         == [t |-> "B", k |-> k]
ERec(op)        == [t |-> "E", op |-> op]
PingRec         == [t |-> "P", v |-> "ping"]
PongRec         == [t |-> "P", v |-> "pong"]

VARIABLES
    \* ------------------------- replica (DB) state ----------------------
    store,   \* [Replicas -> SUBSET Op]              applied operations
    gvv,     \* [Replicas -> VV]                     global VV (VKey0)
    bvv,     \* [Replicas -> [Objects -> VV]]        per-block VVs
    syncs,   \* [Replicas -> SUBSET SyncPoint]       cho.syncs: pending
             \*   diff batches; sp = [k, hvv, ops]; sp.hvv is the origin's
             \*   handshake VV staged in the batch (ApplyH), applied
             \*   atomically together with sp.ops when 'V' arrives.
    \* ------------------------- per-endpoint state ----------------------
    feed,    \* [Endpoints -> {"hs","diff","live","eof","none"}]
    drain,   \* [Endpoints -> {"hs","diff","live","none"}]
    outq,    \* [Endpoints -> Seq(Record)]  broadcast queue (FDQueue)
    chan,    \* [Endpoints -> Seq(Record)]  records in flight e[1] -> e[2]
    peervv,  \* [Endpoints -> VV]           peer's handshake VV
    snap,    \* [Endpoints -> [key, bvv, ops, pend, vset]] pebble snapshot
             \*   taken by FeedHandshake; pend = blocks not yet examined,
             \*   vset = blocks with changes (they go into the 'V' packet)
    ping,    \* [Endpoints -> {"idle","wait_pong","want_pong"}]
    pingcnt, \* [Endpoints -> Nat] pings initiated (bounding only)
    gen      \* [Edges -> Nat] sessions started on this edge so far

vars == <<store, gvv, bvv, syncs, feed, drain, outq, chan, peervv,
          snap, ping, pingcnt, gen>>

NoSnap(x) == [key  |-> <<x, 0>>,
              bvv  |-> [o \in Objects |-> ZeroVV],
              ops  |-> {},
              pend |-> {},
              vset |-> {}]

(***************************************************************************)
(* Type invariant.                                                         *)
(***************************************************************************)
IsVV(v)  == v \in [Replicas -> Nat]
IsOp(op) == /\ DOMAIN op = {"src", "seq", "obj"}
            /\ op.src \in Replicas /\ op.seq \in Nat \ {0} /\ op.obj \in Objects
IsKey(k) == k \in Replicas \X Nat

IsRec(m) ==
    \/ /\ m.t = "H" /\ DOMAIN m = {"t", "k", "vv"}
       /\ IsKey(m.k) /\ IsVV(m.vv)
    \/ /\ m.t = "D" /\ DOMAIN m = {"t", "k", "o", "ops"}
       /\ IsKey(m.k) /\ m.o \in Objects /\ \A op \in m.ops : IsOp(op)
    \/ /\ m.t = "V" /\ DOMAIN m = {"t", "k", "bv"}
       /\ IsKey(m.k) /\ DOMAIN m.bv \subseteq Objects
       /\ \A o \in DOMAIN m.bv : IsVV(m.bv[o])
    \/ /\ m.t = "B" /\ DOMAIN m = {"t", "k"} /\ IsKey(m.k)
    \/ /\ m.t = "E" /\ DOMAIN m = {"t", "op"} /\ IsOp(m.op)
    \/ /\ m.t = "P" /\ DOMAIN m = {"t", "v"} /\ m.v \in {"ping", "pong"}

IsSyncPoint(sp) == /\ DOMAIN sp = {"k", "hvv", "ops", "via"}
                   /\ IsKey(sp.k) /\ IsVV(sp.hvv) /\ \A op \in sp.ops : IsOp(op)
                   /\ sp.via \in Endpoints

TypeOK ==
    /\ \A r \in Replicas :
         /\ \A op \in store[r] : IsOp(op)
         /\ IsVV(gvv[r])
         /\ \A o \in Objects : IsVV(bvv[r][o])
         /\ \A sp \in syncs[r] : IsSyncPoint(sp)
    /\ \A e \in Endpoints :
         /\ feed[e] \in {"hs", "diff", "live", "eof", "none"}
         /\ drain[e] \in {"hs", "diff", "live", "none"}
         /\ \A i \in 1..Len(outq[e]) : IsRec(outq[e][i])
         /\ \A i \in 1..Len(chan[e]) : IsRec(chan[e][i])
         /\ IsVV(peervv[e])
         /\ IsKey(snap[e].key)
         /\ snap[e].pend \subseteq Objects /\ snap[e].vset \subseteq Objects
         /\ ping[e] \in {"idle", "wait_pong", "want_pong"}
         /\ pingcnt[e] \in Nat
    /\ \A p \in Edges : gen[p] \in Nat

(***************************************************************************)
(* Broadcast (chotki.go Broadcast): append records to the outbound queue  *)
(* of every other live session at replica x ("except" semantics).  A      *)
(* queue exists for the lifetime of the connection.                        *)
(***************************************************************************)
QueueOpen(d) == feed[d] # "none" /\ drain[d] # "none"

BcastTargets(x, except) ==
    {d \in Endpoints : d[1] = x /\ d # except /\ QueueOpen(d)}

SeqToApp(q, recs) == q \o recs

(***************************************************************************)
(* Chotki.drain applied record-by-record over a batch (chotki.go).        *)
(* The fold state st carries the replica's DB view plus:                   *)
(*   err : a record failed (ErrSyncUnknown & co) -- processing stops;     *)
(*   n   : how many records of the batch were fully applied.               *)
(***************************************************************************)
ApplyOne(st, m) ==
    CASE m.t = "H" ->
           \* chotki.go 'H': displace stale sync points, then create a
           \* fresh one keyed by the handshake id (snaplast).  ApplyH
           \* stages the origin's global VV inside the batch.  The sync
           \* point remembers the session (endpoint) that delivered the
           \* handshake (syncPoint.via).
           LET kept == IF DisplaceBySrc
                       THEN {sp \in st.syn : sp.k[1] # m.k[1]}
                       ELSE {sp \in st.syn : sp.k # m.k}
                            \* pre-fix code: cho.syncs.Store overwrites the
                            \* same key; same-src keys are NOT displaced
           IN  [st EXCEPT !.syn = kept \cup
                   {[k |-> m.k, hvv |-> m.vv, ops |-> {}, via |-> st.via]}]
      [] m.t = "D" ->
           \* chotki.go 'D': stage ops into the sync-point batch;
           \* unknown sync point => ErrSyncUnknown.
           IF \E sp \in st.syn : sp.k = m.k
           THEN LET sp == CHOOSE sp \in st.syn : sp.k = m.k
                IN  [st EXCEPT !.syn = (@ \ {sp}) \cup
                                       {[sp EXCEPT !.ops = @ \cup m.ops]}]
           ELSE [st EXCEPT !.err = TRUE]
      [] m.t = "V" ->
           \* chotki.go 'V': atomically apply the whole batch (staged data
           \* ops + staged handshake VV) and the block VVs carried by 'V'.
           IF \E sp \in st.syn : sp.k = m.k
           THEN LET sp == CHOOSE sp \in st.syn : sp.k = m.k
                IN  [st EXCEPT
                      !.sto = @ \cup sp.ops,
                      !.gv  = VVMerge(@, sp.hvv),
                      !.bv  = [o \in Objects |->
                                 IF o \in DOMAIN m.bv
                                 THEN VVMerge(st.bv[o], m.bv[o])
                                 ELSE st.bv[o]],
                      !.syn = @ \ {sp}]
           ELSE [st EXCEPT !.err = TRUE]
      [] m.t = "B" ->
           \* chotki.go 'B': drop the origin's pending diff batch, if any.
           [st EXCEPT !.syn = {sp \in @ : sp.k # m.k}]
      [] m.t = "E" ->
           \* chotki.go 'E'/'O'/...: apply immediately; UpdateVTree bumps
           \* both the global VV and the object's block VV.
           [st EXCEPT !.sto = @ \cup {m.op},
                      !.gv  = [@ EXCEPT ![m.op.src] = Max(@, m.op.seq)],
                      !.bv  = [@ EXCEPT ![m.op.obj][m.op.src] = Max(@, m.op.seq)]]

RECURSIVE ApplyBatch(_, _)
ApplyBatch(st, recs) ==
    IF recs = <<>> \/ st.err THEN st
    ELSE LET st1 == ApplyOne(st, Head(recs))
         IN  ApplyBatch(IF st1.err THEN st1 ELSE [st1 EXCEPT !.n = @ + 1],
                        Tail(recs))

(***************************************************************************)
(* Ping records are removed by Syncer.processPings before the batch       *)
(* reaches Chotki.drain and are never relayed.                             *)
(***************************************************************************)
RECURSIVE FilterP(_)
FilterP(s) ==
    IF s = <<>> THEN <<>>
    ELSE IF Head(s).t = "P" THEN FilterP(Tail(s))
         ELSE <<Head(s)>> \o FilterP(Tail(s))

HasPing(s) == \E i \in 1..Len(s) : s[i].t = "P" /\ s[i].v = "ping"

(***************************************************************************)
(* What Syncer.Drain rebroadcasts to the other sessions.                   *)
(*                                                                         *)
(* BuggyRelay = TRUE  (replication/sync.go before the fix):               *)
(*   - the handshake head is always relayed once accepted;                *)
(*   - the rest of the batch is relayed only if the whole batch applied   *)
(*     cleanly AND its last record is not 'B'.                             *)
(* BuggyRelay = FALSE (fixed):                                             *)
(*   - relay exactly the locally-applied prefix, minus a trailing 'B'     *)
(*     (a bye is session-scoped and must not leak downstream).             *)
(***************************************************************************)
RelayOf(batch, st, wasHs) ==
    IF BuggyRelay
    THEN LET rest      == IF wasHs THEN Tail(batch) ELSE batch
             headRelay == IF wasHs /\ st.n >= 1 THEN <<Head(batch)>> ELSE <<>>
             tailB     == rest # <<>> /\ rest[Len(rest)].t = "B"
         IN  IF st.err \/ tailB THEN headRelay ELSE headRelay \o rest
    ELSE LET pre == SubSeq(batch, 1, st.n)
         IN  IF pre # <<>> /\ pre[Len(pre)].t = "B"
             THEN SubSeq(pre, 1, Len(pre) - 1)
             ELSE pre

(***************************************************************************)
(* Syncer.Drain state transition, driven by the last record of the batch  *)
(* (LastLit).                                                              *)
(***************************************************************************)
NextDrainState(cur, batch) ==
    LET lastT == batch[Len(batch)].t
    IN  CASE cur = "hs" ->
               IF Len(batch) = 1 THEN "diff"      \* just the handshake
               ELSE IF lastT \in {"D", "V"} THEN "diff"
               ELSE IF lastT = "B" THEN "none" ELSE "live"
          [] cur = "diff" ->
               IF lastT \in {"D", "V"} THEN "diff"
               ELSE IF lastT = "B" THEN "none" ELSE "live"
          [] cur = "live" ->
               IF lastT = "B" THEN "none" ELSE "live"

--------------------------------------------------------------------------

Init ==
    /\ store   = [r \in Replicas |-> {}]
    /\ gvv     = [r \in Replicas |-> ZeroVV]
    /\ bvv     = [r \in Replicas |-> [o \in Objects |-> ZeroVV]]
    /\ syncs   = [r \in Replicas |-> {}]
    /\ feed    = [e \in Endpoints |-> "none"]
    /\ drain   = [e \in Endpoints |-> "none"]
    /\ outq    = [e \in Endpoints |-> <<>>]
    /\ chan    = [e \in Endpoints |-> <<>>]
    /\ peervv  = [e \in Endpoints |-> ZeroVV]
    /\ snap    = [e \in Endpoints |-> NoSnap(e[1])]
    /\ ping    = [e \in Endpoints |-> "idle"]
    /\ pingcnt = [e \in Endpoints |-> 0]
    /\ gen     = [p \in Edges |-> 0]

(***************************************************************************)
(* Connect / reconnect an edge (network.Net dial + accept).  Both         *)
(* endpoints start a fresh Syncer.  Anything still in flight or queued    *)
(* from the previous session is dropped with the old connection.  Sync    *)
(* points at either replica survive: they live in cho.syncs, not in the   *)
(* Syncer.                                                                 *)
(***************************************************************************)
\* Sync points are bound to the session that created them and die with it
\* (Syncer.Close -> AbortSyncsVia); pre-fix they lingered until the
\* cleanSyncs timeout.
DropVia(S, p) == IF DropSyncsOnClose
                 THEN {sp \in S : EdgeOf(sp.via) # p}
                 ELSE S

Connect(p) ==
    /\ gen[p] < GenBudget
    /\ \A e \in Endpoints : EdgeOf(e) = p => feed[e] = "none"
    /\ syncs'  = [r \in Replicas |->
                    IF r \in p THEN DropVia(syncs[r], p) ELSE syncs[r]]
    /\ gen'    = [gen EXCEPT ![p] = @ + 1]
    /\ feed'   = [e \in Endpoints |-> IF EdgeOf(e) = p THEN "hs" ELSE feed[e]]
    /\ drain'  = [e \in Endpoints |-> IF EdgeOf(e) = p THEN "hs" ELSE drain[e]]
    /\ chan'   = [e \in Endpoints |-> IF EdgeOf(e) = p THEN <<>> ELSE chan[e]]
    /\ outq'   = [e \in Endpoints |-> IF EdgeOf(e) = p THEN <<>> ELSE outq[e]]
    /\ peervv' = [e \in Endpoints |-> IF EdgeOf(e) = p THEN ZeroVV ELSE peervv[e]]
    /\ snap'   = [e \in Endpoints |-> IF EdgeOf(e) = p THEN NoSnap(e[1]) ELSE snap[e]]
    /\ ping'   = [e \in Endpoints |-> IF EdgeOf(e) = p THEN "idle" ELSE ping[e]]
    /\ pingcnt' = [e \in Endpoints |-> IF EdgeOf(e) = p THEN 0 ELSE pingcnt[e]]
    /\ UNCHANGED <<store, gvv, bvv>>

(***************************************************************************)
(* CommitPacket (chotki.go): a replica commits a local op, applies it to  *)
(* its own DB and broadcasts it to every live session (except "").        *)
(***************************************************************************)
Commit(r, o) ==
    /\ gvv[r][r] < CommitBudget[r]
    /\ LET q  == gvv[r][r] + 1
           op == [src |-> r, seq |-> q, obj |-> o]
       IN /\ store' = [store EXCEPT ![r] = @ \cup {op}]
          /\ gvv'   = [gvv EXCEPT ![r][r] = q]
          /\ bvv'   = [bvv EXCEPT ![r][o][r] = q]
          /\ outq'  = [d \in Endpoints |->
                         IF d \in BcastTargets(r, <<r, r>>)
                         THEN SeqToApp(outq[d], <<ERec(op)>>)
                         ELSE outq[d]]
    /\ UNCHANGED <<syncs, feed, drain, chan, peervv, snap, ping, pingcnt, gen>>

(***************************************************************************)
(* Feed side (replication/sync.go Feed()).                                 *)
(***************************************************************************)

\* FeedHandshake: take a pebble snapshot, send H(snaplast, mode, vv).
FeedHandshake(e) ==
    /\ feed[e] = "hs"
    /\ drain[e] # "none"
    /\ LET x == e[1]
           k == <<x, gvv[x][x]>>
       IN /\ chan' = [chan EXCEPT ![e] = Append(@, HRec(k, gvv[x]))]
          /\ snap' = [snap EXCEPT ![e] = [key  |-> k,
                                          bvv  |-> bvv[x],
                                          ops  |-> store[x],
                                          pend |-> Objects,
                                          vset |-> {}]]
          /\ feed' = [feed EXCEPT ![e] = "diff"]
    /\ UNCHANGED <<store, gvv, bvv, syncs, drain, outq, peervv, ping, pingcnt, gen>>

\* FeedBlockDiff: examine the next block of the snapshot.  Gated on the
\* peer's handshake having been drained (WaitDrainState(SendDiff)).  A
\* block is sent iff its block VV is ahead of the peer's global VV
\* (getVVChanges); the parcel carries the ops the peer lacks.
FeedBlockDiff(e) ==
    /\ feed[e] = "diff"
    /\ drain[e] \in {"diff", "live"}
    /\ snap[e].pend # {}
    /\ \E o \in snap[e].pend :
         LET hasChanges == \E s \in Replicas : snap[e].bvv[o][s] > peervv[e][s]
             ops        == {op \in snap[e].ops :
                              op.obj = o /\ op.seq > peervv[e][op.src]}
         IN IF hasChanges
            THEN /\ chan' = [chan EXCEPT ![e] = Append(@, DRec(snap[e].key, o, ops))]
                 /\ snap' = [snap EXCEPT ![e].pend = @ \ {o},
                                         ![e].vset = @ \cup {o}]
            ELSE /\ chan' = chan
                 /\ snap' = [snap EXCEPT ![e].pend = @ \ {o}]
    /\ UNCHANGED <<store, gvv, bvv, syncs, feed, drain, outq, peervv, ping, pingcnt, gen>>

\* FeedDiffVV: all blocks examined; send the 'V' packet carrying the
\* snapshot block VVs of every changed block, then go live (SyncLive is
\* always set, so the non-live SendEOF branch is not taken).
FeedDiffVV(e) ==
    /\ feed[e] = "diff"
    /\ drain[e] \in {"diff", "live"}
    /\ snap[e].pend = {}
    /\ chan' = [chan EXCEPT ![e] =
                  Append(@, VRec(snap[e].key,
                                 [o \in snap[e].vset |-> snap[e].bvv[o]]))]
    /\ feed' = [feed EXCEPT ![e] = "live"]
    /\ UNCHANGED <<store, gvv, bvv, syncs, drain, outq, peervv, snap, ping, pingcnt, gen>>

\* FeedLive: ship the accumulated broadcast queue (Oqueue.Feed).
FeedLive(e) ==
    /\ feed[e] = "live"
    /\ drain[e] \in {"diff", "live"}
    /\ outq[e] # <<>>
    /\ chan' = [chan EXCEPT ![e] = @ \o outq[e]]
    /\ outq' = [outq EXCEPT ![e] = <<>>]
    /\ UNCHANGED <<store, gvv, bvv, syncs, feed, drain, peervv, snap, ping, pingcnt, gen>>

\* The session's outbound queue is closed (displacement by a new
\* connection, queue overflow, shutdown) or the ping timeout fired:
\* Feed switches to SendEOF.  Bounded by the reconnect budget so that
\* TLC does not have to explore pointless final disconnects.
FeedClose(e) ==
    /\ feed[e] = "live"
    /\ outq[e] = <<>>
    /\ gen[EdgeOf(e)] < GenBudget
    /\ feed' = [feed EXCEPT ![e] = "eof"]
    /\ UNCHANGED <<store, gvv, bvv, syncs, drain, outq, chan, peervv, snap, ping, pingcnt, gen>>

\* SendEOF: emit B(snaplast, reason) and stop feeding.
FeedEOF(e) ==
    /\ feed[e] = "eof"
    /\ drain[e] # "none"
    /\ chan' = [chan EXCEPT ![e] = Append(@, BRec(snap[e].key))]
    /\ feed' = [feed EXCEPT ![e] = "none"]
    /\ UNCHANGED <<store, gvv, bvv, syncs, drain, outq, peervv, snap, ping, pingcnt, gen>>

\* Feed() checks GetDrainState() == SendNone on every call and winds the
\* feed down -- but only once its own handshake/diff phase is complete: a
\* peer's bye means it has nothing more to send, while its drain side
\* keeps applying our records until the connection closes, so cutting our
\* diff short would strand a staged diff batch at the peer without its
\* 'V' (the peer would silently miss the promised data).
FeedDrainNone(e) ==
    /\ drain[e] = "none"
    /\ feed[e] \in {"live", "eof"}
    /\ feed' = [feed EXCEPT ![e] = "none"]
    /\ UNCHANGED <<store, gvv, bvv, syncs, drain, outq, chan, peervv, snap, ping, pingcnt, gen>>

(***************************************************************************)
(* Ping/pong (replication/sync.go pingTransition / processPings).          *)
(***************************************************************************)
FeedPing(e) ==
    /\ EnablePing
    /\ feed[e] = "live" /\ ping[e] = "idle" /\ pingcnt[e] < PingBudget
    /\ chan'    = [chan EXCEPT ![e] = Append(@, PingRec)]
    /\ ping'    = [ping EXCEPT ![e] = "wait_pong"]
    /\ pingcnt' = [pingcnt EXCEPT ![e] = @ + 1]
    /\ UNCHANGED <<store, gvv, bvv, syncs, feed, drain, outq, peervv, snap, gen>>

FeedPong(e) ==
    /\ EnablePing
    /\ feed[e] = "live" /\ ping[e] = "want_pong"
    /\ chan' = [chan EXCEPT ![e] = Append(@, PongRec)]
    /\ ping' = [ping EXCEPT ![e] = "idle"]
    /\ UNCHANGED <<store, gvv, bvv, syncs, feed, drain, outq, peervv, snap, pingcnt, gen>>

\* The PingWait timer fires with no pong (PingBroken): the session ends.
PingBroken(e) ==
    /\ EnablePing
    /\ feed[e] = "live" /\ ping[e] = "wait_pong"
    /\ gen[EdgeOf(e)] < GenBudget
    /\ feed' = [feed EXCEPT ![e] = "eof"]
    /\ UNCHANGED <<store, gvv, bvv, syncs, drain, outq, chan, peervv, snap, ping, pingcnt, gen>>

(***************************************************************************)
(* Drain side (replication/sync.go Drain + chotki.go drain).               *)
(*                                                                         *)
(* One Drain call processes a coalesced batch: an arbitrary non-empty     *)
(* prefix of the incoming channel (network/peer.go keepRead).              *)
(***************************************************************************)

\* An unrecoverable drain error (bad handshake, ErrSyncUnknown, ...):
\* Drain returns the error, the network layer tears the connection down,
\* and everything in flight or queued dies with it, including the sync
\* points this session had created (syn0 is the [Replicas -> syncs] base
\* to start from, so the caller can fold in partial batch application).
KillEdge(e, syn0) ==
    LET p == EdgeOf(e)
    IN /\ feed'  = [d \in Endpoints |-> IF EdgeOf(d) = p THEN "none" ELSE feed[d]]
       /\ drain' = [d \in Endpoints |-> IF EdgeOf(d) = p THEN "none" ELSE drain[d]]
       /\ chan'  = [d \in Endpoints |-> IF EdgeOf(d) = p THEN <<>> ELSE chan[d]]
       /\ syncs' = [r \in Replicas |->
                      IF r \in p THEN DropVia(syn0[r], p) ELSE syn0[r]]

Drain(e) ==
    /\ drain[e] \in {"hs", "diff", "live"}
    /\ chan[Rev(e)] # <<>>
    /\ \E n \in 1..Len(chan[Rev(e)]) :
         LET x     == e[1]
             raw   == SubSeq(chan[Rev(e)], 1, n)
             rest  == SubSeq(chan[Rev(e)], n + 1, Len(chan[Rev(e)]))
             batch == FilterP(raw)
             \* processPings: a ping means we owe a pong; any traffic
             \* resets a pending WaitingForPing (resetPingTimer).
             ping1 == IF HasPing(raw) THEN "want_pong"
                      ELSE IF ping[e] = "wait_pong" THEN "idle" ELSE ping[e]
             badHs == drain[e] = "hs" /\ (batch = <<>> \/ batch[1].t # "H")
         IN
         IF badHs
         THEN \* ErrBadHPacket: session dies.
              /\ KillEdge(e, syncs)
              /\ ping' = [ping EXCEPT ![e] = ping1]
              /\ UNCHANGED <<store, gvv, bvv, outq, peervv, snap, pingcnt, gen>>
         ELSE IF batch = <<>>
         THEN \* batch was pings only: nothing reaches Chotki.drain.
              /\ chan' = [chan EXCEPT ![Rev(e)] = rest]
              /\ ping' = [ping EXCEPT ![e] = ping1]
              /\ UNCHANGED <<store, gvv, bvv, syncs, feed, drain, outq, peervv, snap, pingcnt, gen>>
         ELSE
           LET st0 == [sto |-> store[x], gv |-> gvv[x], bv |-> bvv[x],
                       syn |-> syncs[x], via |-> e, err |-> FALSE, n |-> 0]
               stF == ApplyBatch(st0, batch)
               rly == RelayOf(batch, stF, drain[e] = "hs")
               tgt == BcastTargets(x, e)
               nds == NextDrainState(drain[e], batch)
           IN /\ store' = [store EXCEPT ![x] = stF.sto]
              /\ gvv'   = [gvv EXCEPT ![x] = stF.gv]
              /\ bvv'   = [bvv EXCEPT ![x] = stF.bv]
              /\ outq'  = [d \in Endpoints |->
                             IF d \in tgt /\ rly # <<>>
                             THEN SeqToApp(outq[d], rly)
                             ELSE outq[d]]
              /\ peervv' = IF drain[e] = "hs"
                           THEN [peervv EXCEPT ![e] = batch[1].vv]
                           ELSE peervv
              /\ ping' = [ping EXCEPT ![e] = ping1]
              /\ IF stF.err
                 THEN \* Drain returns the error; the connection dies
                      KillEdge(e, [syncs EXCEPT ![x] = stF.syn])
                 ELSE /\ syncs' = [syncs EXCEPT ![x] = stF.syn]
                      /\ drain' = [drain EXCEPT ![e] = nds]
                      /\ chan'  = [chan EXCEPT ![Rev(e)] = rest]
                      /\ feed'  = feed
              /\ UNCHANGED <<snap, pingcnt, gen>>

--------------------------------------------------------------------------

Next ==
    \/ \E p \in Edges : Connect(p)
    \/ \E r \in Replicas, o \in Objects : Commit(r, o)
    \/ \E e \in Endpoints :
         \/ FeedHandshake(e) \/ FeedBlockDiff(e) \/ FeedDiffVV(e)
         \/ FeedLive(e) \/ FeedClose(e) \/ FeedEOF(e) \/ FeedDrainNone(e)
         \/ FeedPing(e) \/ FeedPong(e) \/ PingBroken(e)
         \/ Drain(e)

Spec == Init /\ [][Next]_vars

--------------------------------------------------------------------------
(***************************************************************************)
(* Safety properties.                                                      *)
(***************************************************************************)

\* Every op ever committed by replica s with seq q (ops are committed with
\* consecutive seqs, so "q <= gvv[s][s]" iff it was committed).
Committed(s, q) == q <= gvv[s][s]

(***************************************************************************)
(* NoGaps -- the central data-safety property of the protocol.  A         *)
(* replica's global VV must never claim knowledge of an op it does not    *)
(* actually store: diff sync uses the peer's global VV as the resend      *)
(* floor (getVVChanges/sendvv), so an op below that floor which is not    *)
(* in the store will never be re-sent by anyone -- permanent data loss.   *)
(***************************************************************************)
NoGaps ==
    \A r \in Replicas, s \in Replicas :
        \A q \in 1..gvv[r][s] :
            \E op \in store[r] : op.src = s /\ op.seq = q

\* A replica never claims to have seen more from s than s ever committed.
VVBounded ==
    \A r \in Replicas, s \in Replicas : gvv[r][s] <= gvv[s][s]

\* Block VVs are exact: a non-zero bvv entry names a stored op stamp...
BvvExact ==
    \A r \in Replicas, o \in Objects, s \in Replicas :
        bvv[r][o][s] = 0 \/
            \E op \in store[r] :
                op.src = s /\ op.seq = bvv[r][o][s] /\ op.obj = o

\* ...and complete: they cover every stored op, otherwise the sender-side
\* hasChanges check (block VV vs peer global VV) would under-send.
BvvCovers ==
    \A r \in Replicas : \A op \in store[r] :
        op.seq <= bvv[r][op.obj][op.src]

(***************************************************************************)
(* "Allow only 1 diff sync per src" (chotki.go drain 'H'): at any moment  *)
(* a replica holds at most one pending diff batch per origin replica.     *)
(***************************************************************************)
SyncPointPerSrc ==
    \A r \in Replicas :
        \A sp1, sp2 \in syncs[r] : sp1.k[1] = sp2.k[1] => sp1 = sp2

(***************************************************************************)
(* Convergence.  When nothing is in flight, no broadcast is queued and    *)
(* every session has finished its diff phase (all feeds live), all        *)
(* replicas of the (tree-shaped) cluster must hold the same data.         *)
(***************************************************************************)
TransQuiescent ==
    \A e \in Endpoints :
        chan[e] = <<>> /\ outq[e] = <<>> /\ feed[e] = "live"

Converged == \A r1, r2 \in Replicas : store[r1] = store[r2]

QuiescentConverged == TransQuiescent => Converged

\* Used with "INVARIANT NotConverged" as a reachability witness: TLC's
\* counterexample proves full convergence is actually reachable.
NotConverged ==
    ~(Converged /\ TransQuiescent /\ \A r \in Replicas : gvv[r][r] = CommitBudget[r])

=============================================================================
