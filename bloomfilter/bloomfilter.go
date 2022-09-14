package bloomfilter

import (
	"math"
)

type Filter struct {
	// todo make it be a bitmap
	filter  []bool
	hashNum int
}

// NewFilter returns a new Bloom filter that encodes a set of []byte keys with
// the given number of bits per key, approximately.
//
// A good bitsPerKey value is 10, which yields a filter with ~ 1% false
// positive rate.
func NewFilter(keys []uint32, numEntries int, fp float64) *Filter {
	f := &Filter{}
	f.hashNum = f.bitsPerKey(numEntries, fp)
	f.filter = f.appendFilter(keys, f.hashNum)
	return f
}

func NewFilterDefault(keys []uint32) *Filter {
	f := &Filter{}
	f.hashNum = 10
	f.filter = f.appendFilter(keys, f.hashNum)
	return f
}

// BitsPerKey 指定假阳性率和数据量的情况下计算数组长度大小
func (f *Filter) bitsPerKey(numEntries int, fp float64) int {
	size := -1 * float64(numEntries) * math.Log(fp) / math.Pow(math.Ln2, 2)
	locs := math.Ceil(size / float64(numEntries))
	return int(locs)
}

// CalcHashNum 通过指定的数组长度计算最合适的hash数量
func (f *Filter) calcHashNum(bitsPerKey int) (k uint32) {
	k = uint32(float64(bitsPerKey) * math.Ln2)
	if k < 1 {
		k = 1
	}
	if k > 30 {
		k = 30
	}
	return
}

// appendFilter 将多个key放入bloomFilter中
func (f *Filter) appendFilter(keys []uint32, bitsPerKey int) []bool {
	if bitsPerKey < 0 {
		bitsPerKey = 0
	}
	k := f.calcHashNum(bitsPerKey)

	nBits := len(keys) * bitsPerKey

	filter := make([]bool, nBits)

	// 同一个hash函数做了k次的重复计算
	for _, h := range keys {
		delta := h>>17 | h<<15
		for j := uint32(0); j < k; j++ {
			bitPos := h % uint32(nBits)
			filter[bitPos] = true
			h += delta
		}
	}
	return filter
}

// MayContainsKey 通过过滤器进行过滤
func (f *Filter) MayContainsKey(key []byte) bool {
	return f.mayContain(Hash(key))
}

func (f *Filter) mayContain(key uint32) bool {
	if len(f.filter) < 2 {
		return false
	}
	k := f.calcHashNum(f.hashNum)

	nBits := uint32(len(f.filter) - 1)
	delta := key>>17 | key<<15
	for j := uint32(0); j < k; j++ {
		bitPos := key % nBits
		if f.filter[bitPos] {
			return false
		}
		key += delta
	}
	return true
}

// Hash 采用类murmur Hash进行高效的哈希计算
func Hash(b []byte) uint32 {
	const (
		seed = 0xbc9f1d34
		m    = 0xc6a4a793
	)
	h := uint32(seed) ^ uint32(len(b))*m
	for ; len(b) >= 4; b = b[4:] {
		h += uint32(b[0]) | uint32(b[1])<<8 | uint32(b[2])<<16 | uint32(b[3])<<24
		h *= m
		h ^= h >> 16
	}
	switch len(b) {
	case 3:
		h += uint32(b[2]) << 16
		fallthrough
	case 2:
		h += uint32(b[1]) << 8
		fallthrough
	case 1:
		h += uint32(b[0])
		h *= m
		h ^= h >> 24
	}
	return h
}
