Protocol format is based on ToyTLV (MIT licence) written by Victor Grishchenko in 2024
Original project: https://github.com/learn-decentralized-systems/toytlv

# TLV Record Format

The protocol supports three encoding formats with automatic format selection based on record size:

1.  Tiny Format (1 byte header) - for records 0-9 bytes:
    [('0' + body_length)]
    Example: 3-byte body → ['3']

    - Most compact encoding
    - Type information is lost (normalized to '0')
    - Only available with lowercase record types

2.  Short Format (2 bytes header) - for records up to 255 bytes:
    [lowercase_type, body_length]
    Example: type 'A', 100 bytes → ['a', 100]

    - Medium efficiency
    - Type preserved in lowercase form
    - 1-byte length field

3.  Long Format (5 bytes header) - for records up to 2GB:
    [uppercase_type, length_as_4byte_little_endian]
    Example: type 'A', 1000 bytes → ['A', 0xE8, 0x03, 0x00, 0x00]
    - Full capacity encoding
    - Type preserved in uppercase form
    - 4-byte little-endian length field

# Record Types

Record types are restricted to uppercase letters A-Z. The case of the type parameter
in encoding functions affects format selection:

- Lowercase ('a'-'z'): enables tiny format optimization for small records
- Uppercase ('A'-'Z'): forces explicit encoding, no tiny format

# Format Selection Logic

The encoding format is automatically selected based on:

- Body size (0-9 → tiny, 10-255 → short, >255 → long)
- Type case (lowercase enables tiny, uppercase forces explicit)
- Tiny format requires both: body_size ≤ 9 AND lowercase type

# Parsing and Safety

The package provides two levels of parsing functions:

- Safe functions (Take, TakeAny): for trusted data sources, use nil returns for errors
- Wary functions (TakeWary, TakeAnyWary): for untrusted data, return explicit errors

# Streaming Support

For large or dynamically-sized records, use the streaming API:

    bookmark, buf := OpenHeader(buf, 'X')  // start record with placeholder length
    buf = append(buf, data...)             // add body data incrementally
    CloseHeader(buf, bookmark)             // finalize length field

Example Usage

    // Create a simple record
    record := Record('M', []byte("Hello"))

    // Parse records from buffer
    data := bytes.NewBuffer(networkData)
    records, err := Split(data)

    // Extract specific record type
    body, rest := Take('M', records[0])

# Performance Considerations

- Use Concat() instead of Join() for better memory efficiency
- Prefer lowercase types for small frequent records (tiny format)
- Use streaming API for large records to avoid intermediate allocations
