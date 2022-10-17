package bloomfilter

import (
	"math/rand"
	"time"
)

const (
	cmDepth = 4
)

type cmSketch struct {
	rows [cmDepth]cmRow
	seed [cmDepth]uint64
	mask uint64
}

func newCmSketch(numCounters int64) *cmSketch {
	if numCounters == 0 {
		panic("cmSketch: invalid numCounters")
	}
	// numCounters 一定是二次米, 也就是1后面有n个0
	numCounters = next2Power(numCounters)
	// mask 一定是0111...111
	sketch := &cmSketch{mask: uint64(numCounters - 1)}
	source := rand.New(rand.NewSource(time.Now().UnixNano()))

	// 初始化4行
	// 0000,0000|0000,0000|0000,0000
	// 0000,0000|0000,0000|0000,0000
	// 0000,0000|0000,0000|0000,0000
	// 0000,0000|0000,0000|0000,0000
	for i := 0; i < cmDepth; i++ {
		sketch.seed[i] = source.Uint64()
		sketch.rows[i] = newCmRow(numCounters)
	}
	return sketch
}

func (s *cmSketch) Increment(hashed uint64) {
	// 对于每行进行相同操作
	for i := range s.rows {
		s.rows[i].increment((hashed ^ s.seed[i]) & s.mask)
	}
}

func (s *cmSketch) Estimate(hashed uint64) int64 {
	min := byte(255)
	for i := range s.rows {
		val := s.rows[i].get((hashed ^ s.seed[i]) & s.mask)
		if val < min {
			min = val
		}
	}
	return int64(min)
}

// Reset halves all counter values.
func (s *cmSketch) Reset() {
	for _, r := range s.rows {
		r.reset()
	}
}

// Clear zeroes all counters.
func (s *cmSketch) Clear() {
	for _, r := range s.rows {
		r.clear()
	}
}

// 快速计算大于 X，且最接近 X 的二次幂
func next2Power(x int64) int64 {
	x--
	x |= x >> 1
	x |= x >> 2
	x |= x >> 4
	x |= x >> 8
	x |= x >> 16
	x |= x >> 32
	x++
	return x
}
