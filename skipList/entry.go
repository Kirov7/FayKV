package skipList

import (
	"encoding/binary"
	"time"
)

type Entry struct {
	Key       []byte
	Value     []byte
	ExpiresAt uint64

	Meta         byte
	Version      uint64
	Offset       uint32
	Hlen         int // Length of the header.
	ValThreshold int64
}

func NewEntry(key, value []byte) *Entry {
	return &Entry{
		Key:   key,
		Value: value,
	}
}

// WithTTL 为Entry添加自动过期时间
func (e *Entry) WithTTL(dur time.Duration) *Entry {
	e.ExpiresAt = uint64(time.Now().Add(dur).Unix())
	return e
}

func (e *Entry) Size() int64 {
	return int64(len(e.Key) + len(e.Value))
}

func (e *Entry) Entry() *Entry {
	return e
}

type ValueStruct struct {
	Meta      byte
	Value     []byte
	ExpiresAt uint64

	Version uint64 // This field is not serialized. Only for internal usage.
}

// value只持久化具体的value值和过期时间
func (vs *ValueStruct) EncodedSize() uint32 {
	sz := len(vs.Value) + 1 // meta
	enc := sizeVarint(vs.ExpiresAt)
	return uint32(sz + enc)
}

// DecodeValue
func (vs *ValueStruct) DecodeValue(buf []byte) {
	vs.Meta = buf[0]
	var sz int
	vs.ExpiresAt, sz = binary.Uvarint(buf[1:])
	vs.Value = buf[1+sz:]
}

//对value进行编码，并将编码后的字节写入byte
//这里将过期时间和value的值一起编码
func (vs *ValueStruct) EncodeValue(b []byte) uint32 {
	b[0] = vs.Meta
	sz := binary.PutUvarint(b[1:], vs.ExpiresAt)
	n := copy(b[1+sz:], vs.Value)
	return uint32(1 + sz + n)
}

func sizeVarint(x uint64) (n int) {
	for {
		n++
		x >>= 7
		if x == 0 {
			break
		}
	}
	return n
}
