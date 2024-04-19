package toyqueue

import (
	"sync"
	"testing"
)

type counterFeed struct {
	Counter int
}

func (c *counterFeed) Feed() (Records, error) {
	if c.Counter == 0 {
		return nil, ErrClosed
	} else {
		//fmt.Printf("feed to send: %d\n", c.Counter)
		c.Counter--
		return Records{[]byte{'C'}}, nil
	}
}

func (c *counterFeed) Close() error {
	c.Counter = 0
	//fmt.Printf("feed closed\n")
	return nil
}

type counterDrain struct {
	counter int
	closed  bool
	group   *sync.WaitGroup
}

func (c *counterDrain) Drain(records Records) error {
	c.counter += len(records)
	//fmt.Printf("drain received: %d\n", c.counter)
	return nil
}

func (c *counterDrain) Close() error {
	c.closed = true
	if c.group != nil {
		c.group.Add(-1)
	}
	//fmt.Printf("drain closed\n")
	return nil
}

func TestFanout(t *testing.T) {
	var f2d Fanout
	f := counterFeed{Counter: 5}
	f2d.feeder = &f
	wait := sync.WaitGroup{}
	wait.Add(3)
	c1 := counterDrain{group: &wait}
	c2 := counterDrain{group: &wait}
	c3 := counterDrain{group: &wait}
	f2d.AddDrain(&c1)
	f2d.AddDrain(&c2)
	go f2d.Run()
	f2d.AddDrain(&c3)
	wait.Wait()
}
