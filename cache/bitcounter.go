package cache

import "fmt"

type cmRow []byte

func newCmRow(numCounters int64) cmRow {
	return make(cmRow, numCounters/2)
}

func (r cmRow) get(n uint64) byte {
	return r[n/2] >> ((n & 1) * 4) & 0x0f
}

func (r cmRow) increment(n uint64) {
	i := n / 2
	s := (n & 1) * 4
	v := (r[i] >> s) & 0x0f
	if v < 15 {
		r[i] += 1 << s
	}
}

func (r cmRow) reset() {
	for i := range r {
		r[i] = (r[i] >> 1) & 0x77
	}
}

func (r cmRow) clear() {
	for i := range r {
		r[i] = 0
	}
}

func (r cmRow) String() (s string) {
	for i := uint64(0); i < uint64(len(r)*2); i++ {
		s += fmt.Sprintf("%02d ", (r[(i/2)]>>((i&1)*4))&0x0f)
	}
	s = s[:len(s)-1]
	return s
}
