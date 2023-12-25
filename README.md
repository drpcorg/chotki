# Fast distributed cache (CRDT based)

<img align="right" src="chotki.jpg">
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

