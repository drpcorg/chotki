// Implements a compact TLV (Type-Length-Value) encoding format optimized for efficiency.
package protocol

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
)

const CaseBit uint8 = 'a' - 'A'

var (
	ErrIncomplete = errors.New("incomplete data")
	ErrBadRecord  = errors.New("bad TLV record format")
)

// ProbeHeader analyzes a TLV record header and extracts type and size information.
//
// Returns:
//   - lit: record type ('A'-'Z', '0' for tiny, '-' for error, 0 for incomplete)
//   - hdrlen: header length (1, 2, or 5 bytes)
//   - bodylen: body length in bytes
func ProbeHeader(data []byte) (lit byte, hdrlen, bodylen int) {
	if len(data) == 0 {
		return 0, 0, 0
	}
	dlit := data[0]
	if dlit >= '0' && dlit <= '9' { // tiny
		lit = '0'
		bodylen = int(dlit - '0')
		hdrlen = 1
	} else if dlit >= 'a' && dlit <= 'z' { // short
		if len(data) < 2 {
			return
		}
		lit = dlit - CaseBit
		hdrlen = 2
		bodylen = int(data[1])
	} else if dlit >= 'A' && dlit <= 'Z' { // long
		if len(data) < 5 {
			return
		}
		bl := binary.LittleEndian.Uint32(data[1:5])
		if bl > 0x7fffffff {
			lit = '-'
			return
		}
		lit = dlit
		bodylen = int(bl)
		hdrlen = 5
	} else {
		lit = '-'
	}
	return
}

// Split parses a buffer containing multiple TLV records.
// Modifies the buffer by consuming successfully parsed records.
//
// Returns:
//   - recs: slice of complete TLV records (header + body)
//   - err: ErrBadRecord or ErrIncomplete
func Split(data *bytes.Buffer) (recs Records, err error) {
	for data.Len() > 0 {
		lit, hlen, blen := ProbeHeader(data.Bytes())
		if lit == '-' { // bad format
			if len(recs) == 0 {
				err = ErrBadRecord
			}
			return
		}
		if lit == 0 { // incomplete header
			return
		}
		if hlen+blen > data.Len() { // incomplete package received
			err = errors.Join(ErrIncomplete, fmt.Errorf("packet size %d, len %d", hlen+blen, data.Len()))
			return
		}

		record := make([]byte, hlen+blen)
		if n, err := data.Read(record); err != nil {
			return recs, err
		} else if n != hlen+blen {
			panic("impossible buffer reading")
		}

		recs = append(recs, record)
	}

	return
}

// AppendHeader constructs and appends a TLV record header.
// Automatically selects format based on body length and case.
// Lowercase lit enables tiny format optimization for small bodies.
func AppendHeader(into []byte, lit byte, bodylen int) (ret []byte) {
	biglit := lit &^ CaseBit
	if biglit < 'A' || biglit > 'Z' {
		panic("ToyTLV record type is A..Z")
	}
	if bodylen < 10 && (lit&CaseBit) != 0 {
		ret = append(into, byte('0'+bodylen))
	} else if bodylen > 0xff {
		if bodylen > 0x7fffffff {
			panic("oversized TLV record")
		}
		ret = append(into, biglit)
		ret = binary.LittleEndian.AppendUint32(ret, uint32(bodylen))
	} else {
		ret = append(into, lit|CaseBit, byte(bodylen))
	}
	return ret
}

// Take extracts a TLV record from trusted data. Uses nil returns for errors.
//
// Returns:
//   - body: record body content, nil if error
//   - rest: remaining data, original data if incomplete
func Take(lit byte, data []byte) (body, rest []byte) {
	flit, hdrlen, bodylen := ProbeHeader(data)
	if flit == 0 || hdrlen+bodylen > len(data) {
		return nil, data // Incomplete
	}
	if flit != lit && flit != '0' {
		return nil, nil // BadRecord
	}
	body = data[hdrlen : hdrlen+bodylen]
	rest = data[hdrlen+bodylen:]
	return
}

// TakeAny extracts any TLV record from trusted data without type restrictions.
//
// Returns:
//   - lit: record type found ('A'-'Z'), 0 if no data
//   - body: record body content, nil if error
//   - rest: remaining data, nil if error
func TakeAny(data []byte) (lit byte, body, rest []byte) {
	if len(data) == 0 {
		return 0, nil, nil
	}
	lit = data[0] & ^CaseBit
	body, rest = Take(lit, data)
	return
}

// TakeWary extracts a TLV record from untrusted data with explicit error handling.
//
// Returns:
//   - body: record body content, nil on error
//   - rest: remaining data, original data if incomplete
//   - err: ErrIncomplete or ErrBadRecord
func TakeWary(lit byte, data []byte) (body, rest []byte, err error) {
	flit, hdrlen, bodylen := ProbeHeader(data)
	if flit == 0 || hdrlen+bodylen > len(data) {
		return nil, data, ErrIncomplete
	}
	if flit != lit && flit != '0' {
		return nil, nil, ErrBadRecord
	}
	body = data[hdrlen : hdrlen+bodylen]
	rest = data[hdrlen+bodylen:]
	return
}

// TakeAnyWary extracts any TLV record from untrusted data with error handling.
//
// Returns:
//   - lit: record type found ('A'-'Z'), 0 on error
//   - body: record body content, nil on error
//   - rest: remaining data, nil on error
//   - err: ErrIncomplete for empty/insufficient data
func TakeAnyWary(data []byte) (lit byte, body, rest []byte, err error) {
	if len(data) == 0 {
		return 0, nil, nil, ErrIncomplete
	}
	lit = data[0] & ^CaseBit
	body, rest = Take(lit, data)
	return
}

// TotalLen calculates the total length of multiple byte slices.
func TotalLen(inputs [][]byte) (sum int) {
	for _, input := range inputs {
		sum += len(input)
	}
	return
}

// Lit extracts the canonical record type from a TLV record's first byte.
// Returns ('A'-'Z', '0' for tiny format, or '-' for invalid).
func Lit(rec []byte) byte {
	b := rec[0]
	if b >= 'a' && b <= 'z' {
		return b - CaseBit
	} else if b >= 'A' && b <= 'Z' {
		return b
	} else if b >= '0' && b <= '9' {
		return '0'
	} else {
		return '-'
	}
}

// Append constructs a complete TLV record and appends it to the buffer.
// Lowercase lit enables tiny format optimization.
func Append(into []byte, lit byte, body ...[]byte) (res []byte) {
	total := TotalLen(body)
	res = AppendHeader(into, lit, total)
	for _, b := range body {
		res = append(res, b...)
	}
	return res
}

// Record creates a complete TLV record with pre-allocated capacity.
// Use Append() to add to existing buffer.
func Record(lit byte, body ...[]byte) []byte {
	total := TotalLen(body)
	ret := make([]byte, 0, total+5)
	ret = AppendHeader(ret, lit, total)
	for _, b := range body {
		ret = append(ret, b...)
	}
	return ret
}

// TinyRecord creates a TLV record optimized for tiny format.
// Equivalent to Record() with lowercase lit.
func TinyRecord(lit byte, body []byte) (tiny []byte) {
	// Convert to lowercase to enable tiny format optimization in AppendHeader
	lowercaseLit := (lit &^ CaseBit) | CaseBit
	return Record(lowercaseLit, body)
}

// Join concatenates multiple TLV records into a single byte slice.
// Useful for creating compound messages or batching records.
func Join(records ...[]byte) (ret []byte) {
	for _, rec := range records {
		ret = append(ret, rec...)
	}
	return
}

// Concat efficiently concatenates multiple byte slices with pre-allocation.
// More efficient than Join() for performance-critical code.
func Concat(msg ...[]byte) []byte {
	total := TotalLen(msg)
	ret := make([]byte, 0, total)
	for _, b := range msg {
		ret = append(ret, b...)
	}
	return ret
}

// OpenHeader begins a streamed TLV record for incremental construction.
// Must be paired with CloseHeader(). Use for large or dynamic records.
//
// This function starts a TLV record with a placeholder for the body length,
// allowing the body to be built incrementally using append() operations.
// Must be paired with CloseHeader() to finalize the length field.
//
// Use this pattern for large or dynamically-sized records where the final
// body size is not known in advance.
//
// Parameters:
//   - buf: buffer to append the record header to
//   - lit: record type ('A'-'Z'), automatically converted to uppercase
//
// Return values:
//   - bookmark: position marker needed for CloseHeader() call
//   - res: buffer with the header appended (lit + 4 zero bytes for length)
//
// Usage pattern:
//
//	bookmark, buf := OpenHeader(buf, 'X')
//	buf = append(buf, bodyData...)  // add body incrementally
//	CloseHeader(buf, bookmark)      // finalize the length
//
// The function always uses long format (5-byte header) for simplicity.
func OpenHeader(buf []byte, lit byte) (bookmark int, res []byte) {
	lit &= ^CaseBit
	if lit < 'A' || lit > 'Z' {
		panic("TLV liters are uppercase A-Z")
	}
	res = append(buf, lit)
	blanclen := []byte{0, 0, 0, 0}
	res = append(res, blanclen...)
	return len(res), res
}

// CloseHeader finalizes a streamed TLV record by writing the actual body length.
//
// This function completes a TLV record started with OpenHeader() by calculating
// the actual body size and writing it into the length field placeholder.
// Must be called after all body data has been appended to the buffer.
//
// Parameters:
//   - buf: buffer containing the TLV record with body data appended
//   - bookmark: position marker returned by OpenHeader()
//
// The function:
// 1. Validates the bookmark position (must be ≥5 and ≤ buffer length)
// 2. Calculates body length as: len(buf) - bookmark
// 3. Writes the length as 4-byte little-endian uint32 at bookmark-4 position
//
// Panics if bookmark is invalid, indicating incorrect API usage.
// Always pair with OpenHeader() - never call independently.
func CloseHeader(buf []byte, bookmark int) {
	if bookmark < 5 || len(buf) < bookmark {
		panic("check the API docs")
	}
	binary.LittleEndian.PutUint32(buf[bookmark-4:bookmark], uint32(len(buf)-bookmark))
}
