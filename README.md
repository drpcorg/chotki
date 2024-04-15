#   ◌ Chotki: fast syncable store 

[![godoc](http://img.shields.io/badge/godoc-reference-blue.svg?style=flat)](https://godoc.org/github.com/drpcorg/chotki)
[![MIT License](http://img.shields.io/badge/license-MIT-red.svg?style=flat)](https://raw.githubusercontent.com/drpcorg/chotki/main/LICENSE)
[![Build Status](https://github.com/drpcorg/chotki/actions/workflows/test.yml/badge.svg?branch=main)](https://github.com/drpcorg/chotki/actions/workflows/test.yml)

<img align="right" width="30%" src="chotki.jpg"> 

Chotki is a syncable store with really fast counters.
Internally, it is [pebble db][p] running CRDT natively, using
the [Replicated Data Interchange][r] format (RDX). Chotki is
sync-centric and causally consistent. That means, Chotki
replicas can sync master-master, either incrementally in
real-time or stay offline to diff-sync periodically.
Chotki API is REST/object based, no SQL yet.

[p]: https://github.com/cockroachdb/pebble

##  Use

Chotki's data model has three key entities:
 1. a class is a list of named fields,
 2. an object is an instance of a class,
 3. a field is a value of a Replicated Data Type (RDT).

Available RDTs are basic FIRST types: 
 1. Float, 
 2. Integer, 
 3. Reference,
 4. String and
 5. Term.

There are also collection ELM types: 
 1. Eulerian (set) is an unordered collection of FIRST values,
 2. Linear (array) is an ordered collection of FIRST values, and
 3. Mapping is a collection of key-value pairs (both FIRST).

There are also two counter types:
 1. N are "natural" increment-only uint64 counters and
 2. Z are signed int64 increment/decrement counters.

Objects can Reference each other thus forming object graphs.
As an example, an object of class `Team` has a field `Players`
which is a E-set of references to objects of class `Player`.
Each object of class `Player` has an Integer field `Number`,
a String field `Name`, and a counter `NetWorth`.

Chotki does not support any filter queries yet (of `select ...
where` type). It can fetch an object by its ID or recursively
by references (a reference is an ID).

### REPL

Chotki REPL supports a number of commands to manage databases,
network activities and the data itself.
Each command takes an argument in the text version of RDX, which
is very close to JSON, e.g. 
````
    ◌ connect "replica:1234"
    ◌ cat b0b-28f 
    ◌ new {_ref:Player, Name: "Lionel Messi", Number: 10}
````
Each command returns an ID and/or an error.

Text RDX is different from JSON in several aspects: it has the
ID type, arbitrary literals apart from `true`, `false` and
`null`, also set collections and some other minor differences.
On the human readability side, it is pretty much the same thing.

### HTTP

Chotki HTTP interface is your typical REST API that accepts and
delivers RDX or JSON.

### Native API

Chotki aims at implementing protoc-like compilation step
producing code for objects, including database store/load, RDX
serialization/parsing, etc.

##  Replicas

The main superpower of Chotki is syncing. Replicas may work
offline, reconnect and resync, or they may sync continuously in
real time. Chotki so far only supports a spanning-tree overlay
network topology. Each replica has a 20-bit number (aka
*source*); a replica can only connect to replicas with lesser
src number to prevent loops.
E.g. `a1ece` can connect to `b0b`, but not the other way around
(replica numbers are given in hex).

Implementations of client replicas working on mobile devices or
in a browser are planned.

##  Microbenchmarks

Chotki is based on pebble db, which is an LSM database.
A superpower of LSM is "blind writes", i.e. writes with no
preceding read necessary. On a Lenovo Yoga laptop, a Chotki
replica can do about 1mln blind increments of a counter in about
3 seconds single-thread, be it connected to other replicas or not:
````
    ◌ sinc {fid:b0b-6-2,count:10000000,ms:0}
    b0b-6-2
    inc storm: 10000000 incs complete for b0b-6-2, elapsed 1m4.506868865s, a1ece-5debd1..a1ece-f68251
````

##  Installation

```bash
go get -u github.com/drpcorg/chotki
```

##  Inner workings

Internally, Chotki is [pebble db][p] using [RDX][r] merge operators.
See the RDX doc for further details on its serialization format
(type-length-value) and a very predictable choice of CRDTs.

[r]: https://github.com/learn-decentralized-systems/Chotki/tree/main/rdx

##  Comparison to other projects

Overall, RDX is the final version of RON (Replicated Object
Notation), a research project that lasted from 2017 till 2022.
One may check the [first 2017 RON/RDX talk][c] manifesting the
project's goals. In that regard, we may compare RON/RDX to
[Automerge][a], which is a project of exactly the same age. Both
projects started with a columnar-like coding of operations,
which Automerge is using to this day, while RDX followed the
Einstein's maxim: "Everything should be made as simple as
possible, but not simpler". After spending quite some time to
[cram][s] columnar-encoded CRDT into exising databases, RDX was
greatly simplified and now all the RDX CRDT logic fits into a
merge operator. That greatly improved the performance.
Effectively, that simplicity allows to use a commodity LSM
storage engine to *natively* store arbitrary CRDT.

We can also compare Chotki to a number of JavaScript-centric
CRDT databases, such as [RxDB][x], [TinyBase][b] or [SyncedStore][z].
Historically RON/RDX also has it roots in the JavaScript world.
[Swarm.js][j] was likely the first CRDT sync lib in history
(2013-2018); although it was distilled from the earlier
[Citrea][t] project (2011-2012). Chotki/RDX has an
objective of creating a production-ready scalable CRDT store,
which JavaScript does not really allow. Still, we will be
extremely happy if some of the JavaScript libs would consider
supporting RDX as a unifying format. (Ping us any time!)

[j]: https://github.com/gritzko/swarm
[a]: https://automerge.org/
[b]: https://tinybase.org/
[c]: https://www.youtube.com/watch?v=0Xx9kkTMi10
[s]: https://www.youtube.com/live/M8RRZakZgiI?si=yQVT0Le7FlnpfWXw&t=32187
[t]: https://github.com/gritzko/citrea-model
[x]: https://github.com/pubkey/rxdb
[z]: https://syncedstore.org/docs/

##  The original Chotki project summary

The mission of the system is to keep and update real-time statistics, such as
quotas, counters, billing and suchlike. Update propagation time is expected to
be close to the theoretic minimum: the one-way delay. We expect to be able to
process data at bulk transfer speeds (i.e. as much as we can move by the
network we can process in real-time). It is preferred if the database is
embedded into the application to minimize response times. The data should
(preferably) fit in memory. Consistency guarantees are: causally consistent
(strong EC). The data model is hierarchical, JSON like, with an emphasis on
numeric values. There are no aggregations or queries on the data, only point
lookups and subtree scans. The inner workings of the database is a combination
of a self-orginizing overlay network and an LSM like storage engine. A node is
not connected to every other node: the topology is closer to a spanning tree.
That is to minimize network traffic. The API is object-handle based; the entire
object is not raised into the memory; instead, once the user is reading a
field, we do a lookup. That wav we minimize reads, de-serializations and GC
busywork. Updates are lamport-timestamped, there is a re-sync protocol for
newly joining and re-joining replicas. Overall, we take every shortcut to make
the store lightweight and fast while focusing on our specific usecase
(distributed counters, mainly).

