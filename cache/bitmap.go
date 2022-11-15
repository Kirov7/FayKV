package cache

type Filter []byte

func MakeBitmapWithByteSize(nBytes int) Filter {
	filter := make([]byte, nBytes)
	return filter
}

func MakeBitmapWithBitSize(nBits int) Filter {
	if nBits < 64 {
		nBits = 64
	}
	return MakeBitmapWithByteSize((nBits + 7) / 8)
}

func (f Filter) SetHashNum(hashNum uint8) {
	f[len(f)-1] = byte(hashNum)
}

// Insert Change the bitPos th bit to 1
func (f Filter) Insert(bitPos uint32) {
	f[bitPos/8] |= 1 << (bitPos % 8)
}

// Delete Change the bitPos th bit to 0
func (f Filter) Delete(bitPos uint32) {
	f[bitPos/8] &= ^(1 << (bitPos % 8))
}

func (f Filter) Contains(bitPos uint32) bool {
	return f[bitPos/8]&(1<<(bitPos%8)) != 0
}

func (f Filter) BlContains(key []byte) bool {
	h := Hash(key)
	if len(f) < 2 {
		return false
	}
	k := f[len(f)-1]
	if k > 30 {
		// This is reserved for potentially new encodings for short Bloom filters.
		// Consider it a match.
		return true
	}
	nBits := uint32(8 * (len(f) - 1))
	delta := h>>17 | h<<15
	for j := uint8(0); j < k; j++ {
		bitPos := h % nBits
		if f[bitPos/8]&(1<<(bitPos%8)) == 0 {
			return false
		}
		h += delta
	}
	return true
}

func (f Filter) Reset() {
	for i := range f {
		f[i] = 0
	}
}

func (f Filter) ByteSize(bitPos uint32) int {
	return len(f)
}

func (f Filter) BitSize(bitPos uint32) int {
	return len(f) * 8
}
