package lsm

import (
	"encoding/binary"
	"fmt"
	"github.com/Kirov7/FayKV/cache"
	"github.com/Kirov7/FayKV/inmemory"
	"github.com/Kirov7/FayKV/persistent"
	"github.com/Kirov7/FayKV/utils"
	"github.com/pkg/errors"
	"math"
	"os"
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

func (t *tableIterator) Next() {
	//TODO implement me
	panic("implement me")
}

func (t *tableIterator) Valid() bool {
	//TODO implement me
	panic("implement me")
}

func (t *tableIterator) Rewind() {
	//TODO implement me
	panic("implement me")
}

func (t *tableIterator) Item() utils.Item {
	//TODO implement me
	panic("implement me")
}

func (t *tableIterator) Close() error {
	//TODO implement me
	panic("implement me")
}

func (t *tableIterator) Seek(key []byte) {
	//TODO implement me
	panic("implement me")
}
