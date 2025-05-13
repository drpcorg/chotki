# Index Management System Specification

## Index Structure

### Fullscan Index

The fullscan index maintains a list of all objects for a field in their creation order. This allows for efficient iteration over all objects with a particular field.
They are implicitly created for each class.

#### Storage Format

```
Key: IF{class_id}{object_id}
Value: empty
```

Where:

- class_id: rdx.ID
- object_id: rdx.ID

The key is constructed as:

1. First byte: 'I' for index
2. Second byte: 'F' for fullscan
3. Next 8 bytes: class_id (big-endian)
4. Next 8 bytes: object_id (big-endian)

Example:

```
IF0-0-10-0-2 -> ""
IF0-0-10-0-3 -> ""
IF0-0-10-0-4 -> ""
```

The actual values are stored in the object fields themselves.

#### Operations

- Insert: O(1) - append to the end
- Delete: O(1) - remove entry
- Full Scan: O(n) - iterate over all entries
- No range queries or exact matches supported

#### Updating index

- Live mode. New object created via event 'O', we just add index entry
- Diff mode. When we diff sync we send 'O' entries all the same, so we can detect and add index entry

### Hashtable Index

The hashtable index provides O(1) lookups for exact value matches.

#### Storage Format

```
Key: IH{class_id}{field_id}{hash(value)}
Value: Eulerian set of object IDs
```

Where:

- class_id: rdx.ID
- field_id: rdx.ID
- hash(value): hash of the field value

The key is constructed as:

1. First byte: 'I' for index
2. Second byte: 'H' for hashtable
3. Next 8 bytes: class_id (big-endian)
4. Next 8 bytes: field_id (big-endian)
5. Next bytes: hash of the field value

The value is stored as an Eulerian set ('E') containing object IDs:

```
E{R{object_id1}R{object_id2}...}
```

Example:

```
IH0-0-10-0-20-0-3alice_hash -> E{R{1-2}R{1-3}R{1-4}}
IH0-0-10-0-20-0-3bob_hash -> E{R{1-5}R{1-6}}
IH0-0-10-0-20-0-3charlie_hash -> E{R{1-7}}
```

#### Operations

- Insert: O(1) - hash and insert
- Delete: O(1) - hash and remove
- Exact Match: O(1)
- No range queries supported

### Index Definition Storage

Index definitions are stored as part of class definitions:

```
Key: O{class_id}C
Value: Class definition including index specifications
```

Example class definition with indexes:

```go
type Field struct {
    Offset     int64
    Name       string
    RdxType    byte
    RdxTypeExt []byte
}

// A class is just a slice of fields
type Class []Field

// Example usage:
class := Class{
    {
        Name: "name",
        RdxType: 'S',
        Index: HashIndex
    },
    {
        Name: "age",
        RdxType: 'N',
    },
}
```

### Index Mapping Storage

For quick lookup of which indexes exist for a class:

```
Key: M{class_id}
Value: List of index IDs for this class
```

Example:

```
M123 -> ["name_idx", "age_idx"]
```

## Interface

```go
type IndexManager interface {
    // Handle updates to indexes when data changes
    HandleUpdate(ctx context.Context, fid rdx.ID, lit byte, value []byte, batch *pebble.Batch) error

    // Query operations
    QueryByIndex(ctx context.Context, indexName string, value []byte) ([]rdx.ID, error)

    // Reindexing operations
    ReindexClass(ctx context.Context, classID rdx.ID) error
    ReindexField(ctx context.Context, classID rdx.ID, fieldType byte) error
}
```

## Integration with Chotki

### Object Creation

When a new object is created:

1. The object's fields are stored in the database
2. For each field with a hashtable index:
   - The field value is hashed
   - A new entry is added to the hashtable index
3. The object is added to the class's fullscan index
4. All index updates are part of the same atomic batch as the object creation

### Object Updates

When an object is updated:

1. The old field value is retrieved
2. For each changed field with a hashtable index:
   - The old value is removed from the index
   - The new value is added to the index
3. All index updates are part of the same atomic batch as the object update

### Object Deletion

When an object is deleted:

1. For each field with a hashtable index:
   - The field value is removed from the index
2. The object is removed from the class's fullscan index
3. All index updates are part of the same atomic batch as the object deletion

### Synchronization

Index updates are synchronized between replicas as part of the normal packet synchronization:

1. When a packet containing object changes is received
2. The index updates are applied as part of the same atomic batch
3. This ensures that indexes stay consistent across all replicas

## Reindexing Process

### Task Management

- Each reindexing operation is tracked as a task
- Tasks are stored in the database
- Only one reindexing task per index can run at a time
- Task status and progress are tracked

### Task States

- pending: Task created but not started
- in_progress: Task is currently running
- completed: Task finished successfully
- failed: Task encountered an error
- cancelled: Task was cancelled
