package lsm

import (
	"fmt"
	faycache "github.com/Kirov7/FayKV/cache"
	"github.com/Kirov7/FayKV/inmemory"
	"github.com/Kirov7/FayKV/pb"
	"github.com/Kirov7/FayKV/persistent"
	"github.com/Kirov7/FayKV/utils"
	"github.com/pkg/errors"
	"math"
	"os"
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
	data              []byte   // the real kv_data
	baseKey           []byte   // the first key be written of the block
	entryOffsets      []uint32 // the offset of each key
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

func (tb *tableBuilder) flush(lm *levelManager, tableName string) (t *table, err error) {
	bd := tb.done()
	t = &table{lm: lm, fid: utils.FID(tableName)}
	t.sst = persistent.OpenSSTable(&persistent.Options{
		FileName: tableName,
		Dir:      lm.opt.WorkDir,
		Flag:     os.O_CREATE | os.O_RDWR,
		MaxSize:  bd.size,
	})
	buf := make([]byte, bd.size)
	written := bd.Copy(buf)
	utils.CondPanic(written != len(buf), fmt.Errorf("tableBuilder.flush written != len(buf)"))
	dst, err := t.sst.Bytes(0, bd.size)
	if err != nil {
		return nil, err
	}
	// copy to the mmap buf
	copy(dst, buf)
	return t, nil
}

func (tb *tableBuilder) done() buildData {
	// finish the current active block
	tb.finishBlock()
	if len(tb.blockList) == 0 {
		return buildData{}
	}
	bd := buildData{
		blockList: tb.blockList,
	}

	var f faycache.Filter
	if tb.opt.BloomFalsePositive > 0 {
		f = faycache.BuildBloomFilter(tb.keyHashes, tb.opt.BloomFalsePositive)
	}

	// when all the block are finish, then build the index of the SST
	index, dataSize := tb.buildIndex(f)
	checksum := tb.calculateChecksum(index)
	bd.index = index
	bd.checksum = checksum
	bd.size = int(dataSize) + len(index) + len(checksum) + 4 + 4
	return bd
}

// add encode a kv instance into the sst file
// Write blocks layer by layer from the sst file
func (tb *tableBuilder) add(entry *utils.Entry, isStale bool) {
	key := entry.Key
	val := utils.ValueStruct{
		Meta:      entry.Meta,
		Value:     entry.Value,
		ExpiresAt: entry.ExpiresAt,
	}
	// Check if new blocks are needed
	if tb.tryFinishBlock(entry) {
		// check if cold block
		if isStale {
			tb.staleDataSize += len(key) + 4 + 4
		}
		tb.finishBlock()
		// create new block and start writing
		tb.curBlock = &block{data: make([]byte, tb.opt.BlockSize)}
	}
	// Parse delete ts, add to the hash list, all the key will save a hash value
	tb.keyHashes = append(tb.keyHashes, faycache.Hash(inmemory.ParseKey(key)))
	if version := inmemory.ParseTs(key); version > tb.maxVersion {
		tb.maxVersion = version
	}

	var diffKey []byte
	if len(tb.curBlock.baseKey) == 0 {
		// first time to write
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

func (tb *tableBuilder) tryFinishBlock(entry *utils.Entry) bool {
	if tb.curBlock == nil {
		return true
	}

	if len(tb.curBlock.entryOffsets) <= 0 {
		return false
	}
	// len(tb.curBlock.entryOffsets))+1: The number of kv in the original block + the new kv we want to add (1)
	// *4: Each entry consumes 4 long bytes
	// +4: offset_len uint32
	// +8: checksum uint64
	// +4: checksum_len uint32
	utils.CondPanic(!((uint32(len(tb.curBlock.entryOffsets))+1)*4+4+8+4 < math.MaxUint32), errors.New("Integer overflow"))
	entriesOffsetsSize := int64((len(tb.curBlock.entryOffsets)+1)*4 +
		4 + // size of list
		8 + // Sum64 in checksum proto
		4) // checksum length
	tb.curBlock.estimateSz = int64(tb.curBlock.end) + int64(6 /*header size for entry*/) +
		int64(len(entry.Key)) + int64(entry.EncodedSize()) + entriesOffsetsSize

	// Integer overflow check for table size.
	utils.CondPanic(!(uint64(tb.curBlock.end)+uint64(tb.curBlock.estimateSz) < math.MaxUint32), errors.New("Integer overflow"))

	return tb.curBlock.estimateSz > int64(tb.opt.BlockSize)
}

func (tb *tableBuilder) finishBlock() {
	if tb.curBlock == nil || len(tb.curBlock.entryOffsets) == 0 {
		return
	}
	tb.append(utils.U32SliceToBytes(tb.curBlock.entryOffsets))
	tb.append(utils.U32ToBytes(uint32(len(tb.curBlock.entryOffsets))))

	checksum := tb.calculateChecksum(tb.curBlock.data[:tb.curBlock.end])

	// Append the block checksum and its length.
	tb.append(checksum)
	tb.append(utils.U32ToBytes(uint32(len(checksum))))
	tb.estimateSz += tb.curBlock.estimateSz
	tb.blockList = append(tb.blockList, tb.curBlock)
	// add the key's num for statistic meta
	tb.keyCount += uint32(len(tb.curBlock.entryOffsets))
	tb.curBlock = nil // Indicates that the current block has been serialized to memory
	return
}

func (tb tableBuilder) buildIndex(bloom []byte) ([]byte, uint32) {
	tableIndex := &pb.TableIndex{}
	if len(bloom) > 0 {
		tableIndex.BloomFilter = bloom
	}
	tableIndex.KeyCount = tb.keyCount
	tableIndex.MaxVersion = tb.maxVersion
	tableIndex.Offsets = tb.writeBlockOffsets(tableIndex)
	var dataSize uint32
	for i := range tb.blockList {
		dataSize += uint32(tb.blockList[i].end)
	}
	data, err := tableIndex.Marshal()
	utils.Panic(err)
	return data, dataSize
}

func (tb *tableBuilder) writeBlockOffsets(tableIndex *pb.TableIndex) []*pb.BlockOffset {
	var startOffset uint32
	var offsets []*pb.BlockOffset
	for _, bl := range tb.blockList {
		offset := tb.writeBlockOffset(bl, startOffset)
		offsets = append(offsets, offset)
		startOffset += uint32(bl.end)
	}
	return offsets
}

func (tb *tableBuilder) writeBlockOffset(bl *block, startOffset uint32) *pb.BlockOffset {
	offset := &pb.BlockOffset{}
	offset.Key = bl.baseKey
	offset.Len = uint32(bl.end)
	offset.Offset = startOffset
	return offset
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
		//todo make a Memory allocator to improve here
		tmp := make([]byte, sz)
		copy(tmp, bb.data)
		bb.data = tmp
	}
	bb.end += need
	return bb.data[bb.end-need : bb.end]
}

// keyDiff Prefix matching
func (tb *tableBuilder) keyDiff(newKey []byte) []byte {
	var i int
	for i = 0; i < len(newKey) && i < len(tb.curBlock.baseKey); i++ {
		if newKey[i] != tb.curBlock.baseKey[i] {
			break
		}
	}
	return newKey[i:]
}

func (tb *tableBuilder) calculateChecksum(data []byte) []byte {
	checkSum := utils.CalculateChecksum(data)
	return utils.U64ToBytes(checkSum)
}

// Copy copy data to the specified byte array
func (bd *buildData) Copy(dst []byte) int {
	var written int
	for _, bl := range bd.blockList {
		written += copy(dst[written:], bl.data[:bl.end])
	}
	written += copy(dst[written:], bd.index)
	written += copy(dst[written:], utils.U32ToBytes(uint32(len(bd.index))))

	written += copy(dst[written:], bd.checksum)
	written += copy(dst[written:], utils.U32ToBytes(uint32(len(bd.checksum))))
	return written
}

func (b block) verifyCheckSum() error {
	return utils.VerifyChecksum(b.data, b.checksum)
}

type blockIterator struct {
	data         []byte
	idx          int
	err          error
	baseKey      []byte
	key          []byte
	val          []byte
	entryOffsets []uint32
	block        *block

	tableID uint64
	blockID int

	prevOverlap uint16

	it utils.Item
}

func (itr *blockIterator) setBlock(b *block) {
	panic("todo")
}

func (itr *blockIterator) Next() {
	//TODO implement me
	panic("implement me")
}

func (itr *blockIterator) Valid() bool {
	//TODO implement me
	panic("implement me")
}

func (itr *blockIterator) Rewind() {
	//TODO implement me
	panic("implement me")
}

func (itr *blockIterator) Item() utils.Item {
	//TODO implement me
	panic("implement me")
}

func (itr *blockIterator) Close() error {
	//TODO implement me
	panic("implement me")
}

func (itr *blockIterator) Seek(key []byte) {
	//TODO implement me
	panic("implement me")
}

func (itr *blockIterator) Error() error {
	return itr.err
}

// seekToFirst brings us to the first element.
func (itr *blockIterator) seekToFirst() {
	itr.setIdx(0)
}

func (itr *blockIterator) seekToLast() {
	itr.setIdx(len(itr.entryOffsets) - 1)
}

func (itr *blockIterator) setIdx(i int) {
	panic("todo")
}
