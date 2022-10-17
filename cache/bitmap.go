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
