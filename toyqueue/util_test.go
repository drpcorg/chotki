package toyqueue

import (
	"github.com/stretchr/testify/assert"
	"io"
	"testing"
)

type sliceFeedDrainer struct {
	data []byte
	res  []byte
}

func (fd *sliceFeedDrainer) Close() error {
	fd.res = append(fd.res, '(')
	fd.res = append(fd.res, fd.data...)
	fd.res = append(fd.res, ')')
	return nil
}

func (fd *sliceFeedDrainer) Drain(recs Records) error {
	for _, rec := range recs {
		fd.data = append(fd.data, rec...)
	}
	return nil
}

func (fd *sliceFeedDrainer) Feed() (recs Records, err error) {
	for i := 0; i < 3 && len(fd.data) > 0; i++ {
		recs = append(recs, fd.data[0:1])
		fd.data = fd.data[1:]
	}
	if len(fd.data) == 0 {
		err = io.EOF
	}
	return
}

func TestPump(t *testing.T) {
	sfd := sliceFeedDrainer{
		data: []byte("Hello world"),
	}
	err := PumpN(&sfd, &sfd, 2)
	assert.Nil(t, err)
	assert.Equal(t, sfd.data, []byte("worldHello "))

	fro := sliceFeedDrainer{
		data: []byte("Hello world"),
	}
	to := sliceFeedDrainer{}
	err = PumpThenClose(&fro, &to)
	assert.Equal(t, err, io.EOF)
	assert.Equal(t, []byte("(Hello world)"), to.res)
}
