// Package indexes provides the index management subsystem for Chotki.
//
// # Overview
//
// IndexManager keeps two kinds of indexes for class data:
//
//  1. Fullscan index (implicit per class)
//     A chronological list of all object IDs that belong to a class. It is
//     optimized for scanning a whole class. No range or value queries; O(n).
//
//  2. Hashtable index (opt-in per field)
//     A hash from a field value to the object ID that holds it. Lookups are
//     O(1) on average. Only one object per class+field value is allowed;
//     inserting a duplicate returns ErrHashIndexUinqueConstraintViolation.
//
// # Key layout in Pebble
//
// All keys start with 'I'.
//
//   - Fullscan index:  "IF" + class_id + object_id + 'T' -> empty value
//
//   - Hashtable index: "IH" + class_id + field_id(u32, BE) + hash(u64, BE) +
//     'E' -> TLV-encoded set of object RIDs. We still enforce uniqueness, so at
//     most one RID remains for a value.
//
//   - Reindex tasks:   "IT" + class_id + 'M' -> TLV-encoded map
//     Keys are field indices (u32). Values store task state and last update.
//
// # Integration with writes
//
// IndexManager runs on the write path. Index updates are written in the same
// Pebble batch as the object change. A write either commits object+index
// together or not at all. This keeps data and index consistent, even on
// crashes. Changes can arrive in two ways:
//
// Realtime synchronization (live)
//
//   - Object create/update events arrive in order on an already
//     initialized replica.
//   - AddFullScanIndex appends a class membership entry for a new object.
//   - OnFieldUpdate checks if a field is indexed. If yes, it hashes the FIRST
//     payload and merges the mapping into the hashtable index. Both changes are
//     in the same batch, so the index matches the object data.
//
// Diff synchronization (bootstrap/catch-up)
//
//   - During bootstrap or a large diff, classes and objects may arrive in the
//     same window.
//   - Early in sync, ClassFields may not be readable yet. Then OnFieldUpdate
//     cannot tell if a field is indexed, so it cannot update the hashtable
//     safely.
//   - In that case we enqueue a reindex task for class+field (in the same
//     batch as the object write) and skip the hashtable write. Fullscan entries
//     are still added for new objects.
//   - After sync completes, CheckReindexTasks runs the task. It scans objects
//     via fullscan and rebuilds the hashtable. Indexes catch up without ever
//     writing partial entries.

// Consistency guarantee
//
//   - Realtime: object data and indexes commit in one batch.
//   - Diff sync: if inline indexing is not possible, we only enqueue a reindex
//     task (in the same batch) and defer index writes. The background reindex
//     rebuilds from a snapshot.
//
// The index never contradicts committed object data.
//
// Query helpers
//
//   - SeekClass iterates object IDs that belong to a class using the fullscan
//     index.
//
//   - GetByHash resolves a class+field+value (FIRST payload) to the object ID
//     using the hashtable index. A small in-memory LRU cache accelerates
//     repeat lookups.
//
// # Reindexing lifecycle
//
// Index definitions are part of class definitions. When a class is created or
// updated (e.g., an index is added or removed for a field), HandleClassUpdate
// emits reindex tasks that are persisted under the task key. A background
// scanner (CheckReindexTasks) monitors tasks and runs them via runReindexTask.
//
// Task states are stored as bytes and surfaced via Prometheus metrics. The
// lifecycle is:
//
//   - Pending:     task is scheduled and will be picked up
//   - InProgress:  task is running
//   - Done:        task finished successfully
//   - Remove:      index was deleted; task stays in this state and is ignored
//
// A reindex pass operates on a consistent snapshot and performs two phases:
//
//  1. Repair missing entries: for every object in the class (via fullscan),
//     compute the hash for the field and ensure the corresponding hashtable
//     entry exists.
//
//  2. Remove stale entries: scan the index keys for the class+field and
//     drop entries that point to non-existent objects, to non-FIRST values, or
//     to values whose hash no longer matches the object field.
//
// When an index is removed from a field definition, the manager deletes the
// corresponding IH range and then sets the task to Remove. We keep the task
// (do not delete it) and simply ignore it later. "Done" tasks may be
// periodically rescheduled for self-healing; "Remove" tasks are not.
//
// # Caching and concurrency
//
// The manager keeps small caches:
//   - classCache: object_id -> class_id
//   - hashIndexCache: (class_id, field_id, value) -> object_id
//
// Writes to the same class field index are serialized with a per-field mutex.
//
// # Metrics
//
// Prometheus metrics report task counts, states, durations, and results.
package indexes
