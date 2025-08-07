package protocol

// Records (a batch of) as a very universal primitive, especially
// for database/network op/packet processing. Batching allows
// for writev() and other performance optimizations. Also, if
// you have cryptography, blobs are way handier than structs.
// Records converts easily to net.Buffers.
type Records [][]byte

func (recs Records) TotalLen() (total int64) {
	for _, r := range recs {
		total += int64(len(r))
	}
	return
}
