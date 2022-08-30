package bloomfilter

import (
	"crypto/sha256"
	"math"
)

type Filter struct {
	filter []int
}

func (f *Filter) BitsPerKey(numEntries int, fp float64) int {
	size := -1 * float64(numEntries) * math.Log(fp) / math.Pow(math.Ln2, 2)
	locs := math.Ceil(size / float64(numEntries))
	return int(locs)
}

func (f *Filter) CalcHashNum(bitsPerKey int) (k uint32) {
	k = uint32(float64(bitsPerKey) * math.Ln2)
	if k < 1 {
		k = 1
	}
	if k > 30 {
		k = 30
	}
	return
}

func (f *Filter) appendFilter(keys []uint32, bitsPerKey int) []int {
	if bitsPerKey < 0 {
		bitsPerKey = 0
	}
	k := f.CalcHashNum(bitsPerKey)

	nBits := len(keys) * bitsPerKey

	filter := make([]int, nBits)

	for _, h := range keys {
		delta := h>>17 | h<<15
		for j := uint32(0); j < k; j++ {
			bitPos := h % uint32(nBits)
			filter[bitPos] = 1
			h += delta
		}
	}
	return filter
}

func (f *Filter) MayContainsKey(key []byte) bool {
	sha256.Sum256(key)
	return f.MayContain(key)
}

func (f *Filter) MayContain(key []byte) bool {
	panic(key)
}
