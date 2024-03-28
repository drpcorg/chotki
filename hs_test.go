package chotki

/*
import (
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
	"time"
)


func TestHandshake(t *testing.T) {
	_ = os.RemoveAll("cho2")
	_ = os.RemoveAll("cho3")

	example := Example{
		Name:  "test",
		Score: 123,
	}

	var chotki2 Chotki
	chotki2.opts.RelaxedOrder = true
	err := chotki2.Open(2)
	assert.Nil(t, err)

	var chotki3 Chotki
	chotki3.opts.RelaxedOrder = true
	err = chotki3.Open(3)
	assert.Nil(t, err)

	state, _ := example.Store(nil) // fixme
	objid, err := chotki2.CommitPacket('O', ID0, state)
	assert.Nil(t, err)

	address := "localhost:1234"

	err = chotki2.tcp.Listen(address)
	assert.Nil(t, err)

	err = chotki3.tcp.Connect(address)
	assert.Nil(t, err)

	time.Sleep(time.Minute * 10) // FIXME :)

	it := chotki3.ObjectIterator(objid)
	var example2 Example
	err = example2.Load(it)
	assert.Nil(t, err)
	assert.Equal(t, example2, example)

	err = chotki2.Close()
	assert.Nil(t, err)

	err = chotki3.Close()
	assert.Nil(t, err)

	_ = os.RemoveAll("cho2")
	_ = os.RemoveAll("cho3")
}
*/
