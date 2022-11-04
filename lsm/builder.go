package lsm

import (
	"fmt"
	"github.com/Kirov7/FayKV/cache"
	"github.com/Kirov7/FayKV/inmemory"
	"github.com/Kirov7/FayKV/utils"
	"github.com/pkg/errors"
	"math"
	"unsafe"
)

type tableBuilder struct {
	sstSize       int64
	curBlock      *block
	opt           *Options
	blockList     []*block
	keyCount      uint32
	keyHashes     []uint32
	maxVersion    uint64
	baseKey       []byte
	staleDataSize int
	estimateSz    int64
}

type buildData struct {
	blockList []*block
	index     []byte
	checksum  []byte
	size      int
}

type block struct {
	offset            int //the offset of the current block
	checksum          []byte
	entriesIndexStart int
	chkLen            int
	data              []byte
	baseKey           []byte
	entryOffsets      []uint32
	end               int
	estimateSz        int64
}

type header struct {
	overlap uint16 // Overlap with base key.
	diff    uint16 // Length of the diff.
}

const headerSize = uint16(unsafe.Sizeof(header{}))

func (h *header) decode(buf []byte) {
	copy((*[headerSize]byte)(unsafe.Pointer(h))[:], buf[:headerSize])
}

func (h header) encode() []byte {
	var b [4]byte
	*(*header)(unsafe.Pointer(&b[0])) = h
	return b[:]
}

func newTableBuilder(opt *Options) *tableBuilder {
	return &tableBuilder{
		opt:     opt,
		sstSize: opt.SSTableMaxSize,
	}
}

func (tb *tableBuilder) add(entry *inmemory.Entry, isStale bool) {
	key := entry.Key
	val := inmemory.ValueStruct{
		Meta:      entry.Meta,
		Value:     entry.Value,
		ExpiresAt: entry.ExpiresAt,
	}
	// Check if new blocks are needed
	if tb.tryFinishBlock(entry) {
		if isStale {
			tb.staleDataSize += len(key) + 4 + 4
		}
		tb.finishBlock()
		// create new block and start writing
		tb.curBlock = &block{data: make([]byte, tb.opt.BlockSize)}
	}
	tb.keyHashes = append(tb.keyHashes, cache.Hash(inmemory.ParseKey(key)))
	if version := inmemory.ParseTs(key); version > tb.maxVersion {
		tb.maxVersion = version
	}

	var diffKey []byte
	if len(tb.curBlock.baseKey) == 0 {
		tb.curBlock.baseKey = append(tb.curBlock.baseKey[:0], key...)
		diffKey = key
	} else {
		diffKey = tb.keyDiff(key)
	}
	utils.CondPanic(!(len(key)-len(diffKey) <= math.MaxUint16), fmt.Errorf("tableBuilder.add: len(key)-len(diffKey) <= math.MaxUint16"))
	utils.CondPanic(!(len(diffKey) <= math.MaxUint16), fmt.Errorf("tableBuilder.add: len(diffKey) <= math.MaxUint16"))

	h := header{
		overlap: uint16(len(key) - len(diffKey)),
		diff:    uint16(len(diffKey)),
	}

	tb.curBlock.entryOffsets = append(tb.curBlock.entryOffsets, uint32(tb.curBlock.end))

	tb.append(h.encode())
	tb.append(diffKey)

	dst := tb.allocate(int(val.EncodedSize()))
	val.EncodeValue(dst)
}

func (tb *tableBuilder) tryFinishBlock(entry *inmemory.Entry) bool {
	panic("todo")
}

func (tb *tableBuilder) finishBlock() bool {
	panic("todo")
}

// append appends to curBlock.data
func (tb *tableBuilder) append(data []byte) {
	dst := tb.allocate(len(data))
	utils.CondPanic(len(data) != copy(dst, data), errors.New("tableBuilder.append data"))
}

func (tb tableBuilder) allocate(need int) []byte {
	bb := tb.curBlock
	if len(bb.data[bb.end:]) < need {
		sz := 2 * len(bb.data)
		if bb.end+need > sz {
			sz = bb.end + need
		}
		tmp := make([]byte, sz)
		copy(tmp, bb.data)
		bb.data = tmp
	}
	bb.end += need
	return bb.data[bb.end-need : bb.end]
}

func (tb *tableBuilder) keyDiff(newKey []byte) []byte {
	var i int
	for i = 0; i < len(newKey) && i < len(tb.curBlock.baseKey); i++ {
		if newKey[i] != tb.curBlock.baseKey[i] {
			break
		}
	}
	return newKey[i:]
}
