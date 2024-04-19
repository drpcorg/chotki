package toytlv

import (
	"github.com/drpcorg/chotki/toyqueue"
	"io"
)

// Feeder reads TLV records from an io.Reader stream.
// Note that Feeder is buffered, i.e. it reads ahead.
// When doing Seek() on a file, recreate Feeder, that is cheap.
type Reader2Feeder struct {
	pre    []byte
	Reader io.Reader
}

type ReadSeeker2FeedSeeker struct {
	pre    []byte
	Reader io.ReadSeeker
}

type ReadCloser2FeedCloser struct {
	pre    []byte
	Reader io.ReadCloser
}

type ReadSeekCloser2FeedSeekCloser struct {
	pre    []byte
	Reader io.ReadSeekCloser
}

const DefaultPreBufLength = 4096
const MinRecommendedRead = 512
const MinRecommendedWrite = 400

func (fs *ReadSeeker2FeedSeeker) Seek(offset int64, whence int) (int64, error) {
	fs.pre = nil
	return fs.Reader.Seek(offset, whence)
}

func (fs *ReadSeekCloser2FeedSeekCloser) Seek(offset int64, whence int) (int64, error) {
	fs.pre = nil
	return fs.Reader.Seek(offset, whence)
}

func (fs *ReadCloser2FeedCloser) Close() error {
	fs.pre = nil
	return fs.Reader.Close()
}

func (fs *ReadSeekCloser2FeedSeekCloser) Close() error {
	fs.pre = nil
	return fs.Reader.Close()
}

func (fs *Reader2Feeder) Feed() (recs toyqueue.Records, err error) {
	fs.pre, recs, err = feed(fs.pre, fs.Reader)
	return
}

func (fs *ReadSeeker2FeedSeeker) Feed() (recs toyqueue.Records, err error) {
	fs.pre, recs, err = feed(fs.pre, fs.Reader)
	return
}

func (fs *ReadCloser2FeedCloser) Feed() (recs toyqueue.Records, err error) {
	fs.pre, recs, err = feed(fs.pre, fs.Reader)
	return
}

func (fs *ReadSeekCloser2FeedSeekCloser) Feed() (recs toyqueue.Records, err error) {
	fs.pre, recs, err = feed(fs.pre, fs.Reader)
	return
}

func fill(past []byte, tolen int, reader io.Reader) (data []byte, err error) {
	data = past
	l := len(data)
	c := cap(data)
	if c-l < MinRecommendedRead || c < tolen {
		newcap := DefaultPreBufLength
		if newcap < tolen {
			newcap = tolen
		}
		newpre := make([]byte, newcap)
		copy(newpre, data)
		newpre = newpre[:l]
		data = newpre
		l = len(data)
		c = cap(data)
	}
	for len(data) < tolen {
		vac := data[l:c]
		var n int
		n, err = reader.Read(vac)
		if err != nil {
			break
		}
		data = data[0 : l+n]
	}
	return
}

func feed(past []byte, reader io.Reader) (rest []byte, tlv toyqueue.Records, err error) {
	rest = past
	var hdrlen, bodylen int
	var lit byte
	lit, hdrlen, bodylen = ProbeHeader(rest)
	for lit == 0 || hdrlen+bodylen > len(rest) {
		tolen := len(rest) + 1
		if lit != 0 {
			tolen = hdrlen + bodylen
		}
		rest, err = fill(rest, tolen, reader)
		if err != nil {
			return
		}
		lit, hdrlen, bodylen = ProbeHeader(rest)
	}
	for lit >= 'A' && lit <= 'Z' && hdrlen+bodylen <= len(rest) {
		tlv = append(tlv, rest[0:hdrlen+bodylen])
		rest = rest[hdrlen+bodylen:]
		lit, hdrlen, bodylen = ProbeHeader(rest)
	}
	if lit == '-' {
		err = ErrBadRecord
	}
	return
}

type Writer2Drainer struct {
	Writer io.Writer
}

type WritCloser2DrainCloser struct {
	Writer io.WriteCloser
}

func next(rest []byte, more toyqueue.Records) (cur []byte, left toyqueue.Records) {
	cur, left = rest, more
	if len(cur) >= MinRecommendedWrite {
		return
	}
	for len(cur) < MinRecommendedWrite && len(left) > 0 {
		cur = append(cur, left[0]...)
		left = left[1:]
	}
	return
}

// Having no writev() we do the next best thing: bundle writes
func (d *Writer2Drainer) Drain(recs toyqueue.Records) error {
	var cur []byte
	for len(cur) > 0 || len(recs) > 0 {
		cur, recs = next(cur, recs)
		n, err := d.Writer.Write(cur)
		if err != nil {
			return err
		}
		cur = cur[n:]
	}
	return nil
}

// Having no writev() we do the next best thing: bundle writes
func (d *WritCloser2DrainCloser) Drain(recs toyqueue.Records) error {
	var cur []byte
	for len(cur) > 0 || len(recs) > 0 {
		cur, recs = next(cur, recs)
		n, err := d.Writer.Write(cur)
		if err != nil {
			return err
		}
		cur = cur[n:]
	}
	return nil
}

func (dc *WritCloser2DrainCloser) Close() error {
	return dc.Writer.Close()
}
