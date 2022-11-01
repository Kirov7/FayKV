package utils

import "sync"

type Closer struct {
	waiting     sync.WaitGroup
	CloseSignal chan struct{}
}

func NewCloser() *Closer {
	return &Closer{
		waiting:     sync.WaitGroup{},
		CloseSignal: make(chan struct{}),
	}
}

func (c *Closer) Close() {
	close(c.CloseSignal)
	c.waiting.Wait()
}

func (c *Closer) Done() {
	c.waiting.Done()
}

func (c *Closer) Add(n int) {
	c.waiting.Add(n)
}
