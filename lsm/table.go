package lsm

import (
	"encoding/binary"
	"fmt"
	"github.com/Kirov7/FayKV/cache"
	"github.com/Kirov7/FayKV/inmemory"
	"github.com/Kirov7/FayKV/pb"
	"github.com/Kirov7/FayKV/persistent"
	"github.com/Kirov7/FayKV/utils"
	"github.com/pkg/errors"
	"io"
	"math"
	"os"
	"sort"
	"sync/atomic"
)

// An SSTable object that contains handles in memory
type table struct {
	sst *persistent.SSTable
	lm  *levelManager
	fid uint64
	ref int32 // For file garbage collection. Atomic.
}

func openTable(lm *levelManager, tableName string, builder *tableBuilder) *table {
	sstSize := int(lm.opt.SSTableMaxSize)
	// if builder not nil ,we need to flush skiplist to sstable
	if builder != nil {
		sstSize = builder.done().size
	}
	var (
		t   *table
		err error
	)
	fid := utils.FID(tableName)
	if builder != nil {
		if t, err = builder.flush(lm, tableName); err != nil {
			return nil
		}
	} else {
		t = &table{lm: lm, fid: fid}
		t.sst = persistent.OpenSSTable(&persistent.Options{
			FileName: tableName,
			Dir:      lm.opt.WorkDir,
			Flag:     os.O_CREATE | os.O_RDWR,
			MaxSize:  sstSize,
		})
	}
	t.IncrRef()
	// init sstable file load index
	if err := t.sst.Init(); err != nil {
		return nil
	}
	// get the max key of the sst from iterator
	itr := t.NewIterator(&utils.Options{})
	defer itr.Close()
	// locate the max key
	itr.Rewind()
	utils.CondPanic(!itr.Valid(), errors.Errorf("failed to read index, form maxKey"))
	maxKey := itr.Item().Entry().Key
	t.sst.SetMaxKey(maxKey)
	return t
}

func (t *table) Search(key []byte, maxVs *uint64) (entry *utils.Entry, err error) {
	t.IncrRef()
	defer t.DecrRef()
	idx := t.sst.Indexs()
	bloomFilter := cache.Filter(idx.BloomFilter)
	if t.sst.HasBloomFilter() && !bloomFilter.BlContains(key) {
		return nil, utils.ErrKeyNotFound
	}
	iter := t.NewIterator(&utils.Options{})
	defer iter.Close()

	iter.Seek(key)
	if !iter.Valid() {
		return nil, utils.ErrKeyNotFound
	}

	if inmemory.SameKey(key, iter.Item().Entry().Key) {
		if version := inmemory.ParseTs(iter.Item().Entry().Key); *maxVs < version {
			*maxVs = version
			return iter.Item().Entry(), nil
		}
	}
	return nil, utils.ErrKeyNotFound
}

// blockCacheKey is used to store blocks in the block TableCache.
func (t *table) blockCacheKey(idx int) []byte {
	utils.CondPanic(t.fid >= math.MaxUint32, fmt.Errorf("t.fid >= math.MaxUint32"))
	utils.CondPanic(uint32(idx) >= math.MaxUint32, fmt.Errorf("uint32(idx) >=  math.MaxUint32"))

	buf := make([]byte, 8)
	// Assume t.ID does not overflow uint32.
	binary.BigEndian.PutUint32(buf[:4], uint32(t.fid))
	binary.BigEndian.PutUint32(buf[4:], uint32(idx))
	return buf
}

func (t *table) Delete() error {
	return t.sst.Detele()
}

// DecrRef decrements the refcount and possibly deletes the table
func (t *table) DecrRef() error {
	newRef := atomic.AddInt32(&t.ref, -1)
	if newRef == 0 {
		// TODO remove from cache
		for i := 0; i < len(t.sst.Indexs().GetOffsets()); i++ {
			t.lm.cache.blocks.Del(t.blockCacheKey(i))
		}
		if err := t.Delete(); err != nil {
			return err
		}
	}
	return nil
}

func (t *table) IncrRef() {
	atomic.AddInt32(&t.ref, 1)
}

func decrRefs(tables []*table) error {
	for _, table := range tables {
		if err := table.DecrRef(); err != nil {
			return err
		}
	}
	return nil
}

func (t *table) read(off, sz int) ([]byte, error) {
	return t.sst.Bytes(off, sz)
}

// Load the block object corresponding to the sst
func (t *table) block(idx int) (*block, error) {
	utils.CondPanic(idx < 0, fmt.Errorf("idx=%d", idx))
	if idx >= len(t.sst.Indexs().Offsets) {
		return nil, errors.New("block out of index")
	}
	var b *block
	key := t.blockCacheKey(idx)
	blk, ok := t.lm.cache.blocks.Get(key)
	if ok && blk != nil {
		b, _ = blk.(*block)
		return b, nil
	}

	var bo pb.BlockOffset
	utils.CondPanic(!t.offsets(&bo, idx), fmt.Errorf("block t.offset id=%d", idx))
	b = &block{
		offset: int(bo.GetOffset()),
	}

	var err error
	if b.data, err = t.read(b.offset, int(bo.GetLen())); err != nil {
		return nil, errors.Wrapf(err,
			"failed to read from sstable: %d at offset: %d, len: %d",
			t.sst.FID(), b.offset, bo.GetLen())
	}

	readPos := len(b.data) - 4 // First read checksum length.
	b.chkLen = int(utils.BytesToU32(b.data[readPos : readPos+4]))

	if b.chkLen > len(b.data) {
		return nil, errors.New("invalid checksum length. Either the data is " +
			"corrupted or the table options are incorrectly set")
	}

	readPos -= b.chkLen
	b.checksum = b.data[readPos : readPos+b.chkLen]

	b.data = b.data[:readPos]

	if err = b.verifyCheckSum(); err != nil {
		return nil, err
	}

	readPos -= 4
	numEntries := int(utils.BytesToU32(b.data[readPos : readPos+4]))
	entriesIndexStart := readPos - (numEntries * 4)
	entriesIndexEnd := entriesIndexStart + numEntries*4

	b.entryOffsets = utils.BytesToU32Slice(b.data[entriesIndexStart:entriesIndexEnd])

	b.entriesIndexStart = entriesIndexStart

	t.lm.cache.blocks.Set(key, b)

	return b, nil
}

func (t *table) offsets(bo *pb.BlockOffset, i int) bool {
	index := t.sst.Indexs()
	if i < 0 || i > len(index.GetOffsets()) {
		return false
	}
	if i == len(index.GetOffsets()) {
		return true
	}
	*bo = *index.GetOffsets()[i]
	return true
}

// Size is its file size in bytes
func (t *table) Size() int64 { return int64(t.sst.Size()) }

// StaleDataSize is the amount of stale data (that can be dropped by a compaction )in this SST.
func (t *table) StaleDataSize() uint32 { return t.sst.Indexs().StaleDataSize }

type tableIterator struct {
	it       utils.Item
	opt      *utils.Options
	t        *table
	blockPos int
	bi       *blockIterator
	err      error
}

func (t *table) NewIterator(options *utils.Options) utils.Iterator {
	t.IncrRef()
	return &tableIterator{
		opt: options,
		t:   t,
		bi:  &blockIterator{},
	}
}

func (itr *tableIterator) Next() {
	itr.err = nil
	if itr.blockPos >= len(itr.t.sst.Indexs().GetOffsets()) {
		itr.err = io.EOF
		return
	}
	if len(itr.bi.data) == 0 {
		block, err := itr.t.block(itr.blockPos)
		if err != nil {
			itr.err = err
			return
		}
		itr.bi.tableID = itr.t.fid
		itr.bi.blockID = itr.blockPos
		itr.bi.setBlock(block)
		itr.bi.seekToFirst()
		itr.err = itr.bi.Error()
		return
	}
	itr.bi.Next()
	if !itr.bi.Valid() {
		itr.blockPos++
		itr.bi.data = nil
		itr.Next()
		return
	}
	itr.it = itr.bi.it
}

func (itr *tableIterator) Valid() bool {
	return itr.err != io.EOF
}

func (itr *tableIterator) Rewind() {
	if itr.opt.IsAsc {
		itr.seekToFirst()
	} else {
		itr.seekToLast()
	}
}

func (itr *tableIterator) Item() utils.Item {
	return itr.it
}

func (itr *tableIterator) Close() error {
	itr.bi.Close()
	return itr.t.DecrRef()
}

func (itr *tableIterator) Seek(key []byte) {
	var bo pb.BlockOffset
	idx := sort.Search(len(itr.t.sst.Indexs().GetOffsets()), func(idx int) bool {
		utils.CondPanic(!itr.t.offsets(&bo, idx), fmt.Errorf("tableutils.Seek idx < 0 || idx > len(index.GetOffsets()"))
		if idx == len(itr.t.sst.Indexs().GetOffsets()) {
			return true
		}
		return inmemory.CompareKeys(bo.GetKey(), key) > 0
	})
	if idx == 0 {
		itr.seekHelper(0, key)
		return
	}
	itr.seekHelper(idx-1, key)
}

func (itr *tableIterator) seekHelper(blockIdx int, key []byte) {
	itr.blockPos = blockIdx
	block, err := itr.t.block(blockIdx)
	if err != nil {
		itr.err = err
		return
	}
	itr.bi.tableID = itr.t.fid
	itr.bi.blockID = itr.blockPos
	itr.bi.setBlock(block)
	itr.bi.Seek(key)
	itr.err = itr.bi.Error()
	itr.it = itr.bi.Item()
}

func (itr *tableIterator) seekToFirst() {
	numBlocks := len(itr.t.sst.Indexs().Offsets)
	if numBlocks == 0 {
		itr.err = io.EOF
		return
	}
	itr.blockPos = 0
	block, err := itr.t.block(itr.blockPos)
	if err != nil {
		itr.err = err
		return
	}
	itr.bi.tableID = itr.t.fid
	itr.bi.blockID = itr.blockPos
	itr.bi.setBlock(block)
	itr.bi.seekToFirst()
	itr.it = itr.bi.Item()
	itr.err = itr.bi.Error()
}

func (itr *tableIterator) seekToLast() {
	numBlocks := len(itr.t.sst.Indexs().Offsets)
	if numBlocks == 0 {
		itr.err = io.EOF
		return
	}
	itr.blockPos = numBlocks - 1
	block, err := itr.t.block(itr.blockPos)
	if err != nil {
		itr.err = err
		return
	}
	itr.bi.tableID = itr.t.fid
	itr.bi.blockID = itr.blockPos
	itr.bi.setBlock(block)
	itr.bi.seekToLast()
	itr.it = itr.bi.Item()
	itr.err = itr.bi.Error()
}
