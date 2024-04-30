package protocol

// Records (a batch of) as a very universal primitive, especially
// for database/network op/packet processing. Batching allows
// for writev() and other performance optimizations. Also, if
// you have cryptography, blobs are way handier than structs.
// Records converts easily to net.Buffers.
type Records [][]byte

func (recs Records) recrem(total int64) (prelen int, prerem int64) {
	for len(recs) > prelen && int64(len(recs[prelen])) <= total {
		total -= int64(len(recs[prelen]))
		prelen++
	}
	prerem = total
	return
}

func (recs Records) WholeRecordPrefix(limit int64) (prefix Records, remainder int64) {
	prelen, remainder := recs.recrem(limit)
	prefix = recs[:prelen]
	return
}

func (recs Records) ExactSuffix(total int64) (suffix Records) {
	prelen, prerem := recs.recrem(total)
	suffix = recs[prelen:]
	if prerem != 0 { // damages the original, hence copy
		edited := make(Records, 1, len(suffix))
		edited[0] = suffix[0][prerem:]
		suffix = append(edited, suffix[1:]...)
	}
	return
}

func (recs Records) TotalLen() (total int64) {
	for _, r := range recs {
		total += int64(len(r))
	}
	return
}
