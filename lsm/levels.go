package lsm

import (
	"bytes"
	"github.com/Kirov7/FayKV/persistent"
	"github.com/Kirov7/FayKV/utils"
	"sync"
)

type levelManager struct {
	maxFID       uint64
	opt          *Options
	cache        *TableCache
	manifestFile *persistent.ManifestFile
	levels       []*levelHandler // Each layer has a handler
	lsm          *LSM
	compactState *compactStatus
}

func (lsm *LSM) initLevelManager(opt *Options) *levelManager {
	lm := &levelManager{lsm: lsm}
	lm.compactState = lsm.newCompactStatus()
	lm.opt = opt
	// Read the index information of the manifest file
	utils.Panic(lm.loadManifest())
	lm.build()
	return lm
}

func (lm *levelManager) loadManifest() error {
	panic("todo")
}

func (lm *levelManager) build() error {
	panic("todo")
}

// flush flush memtable to sstable ondisk
func (lm *levelManager) flush(immutable *memTable) error {
	// Assign a fid
	fid := immutable.wal.Fid()
	sstName := persistent.FileNameSSTable(lm.opt.WorkDir, fid)
	// Create a builder by ranging the immutable
	builder := newTableBuilder(lm.opt)
	iter := immutable.sl.NewSkipListIterator()
	for iter.Rewind(); iter.Valid(); iter.Next() {
		entry := iter.Item().Entry()
		builder.add(entry, false)
	}
	// Create a table instance
	table := openTable(lm, sstName, builder)
	err := lm.manifestFile.AddTableMeta(0, &persistent.TableMeta{
		ID:       fid,
		Checksum: []byte{'f', 'a', 'y', 'd', 'b'},
	})
	utils.Panic(err)
	// The metadata must be updated after the data has been successfully written to the file
	lm.levels[0].add(table)
	return nil
}

func (lm *levelManager) Get(key []byte) (*utils.Entry, error) {
	// query in l0
	if entry, err := lm.levels[0].Get(key); entry != nil {
		return entry, err
	}
	// query in l1-7
	for level := 1; level < lm.opt.MaxLevelNum; level++ {
		ld := lm.levels[level]
		if entry, err := ld.Get(key); entry != nil {
			return entry, err
		}
	}
	return nil, utils.ErrKeyNotFound
}

type levelHandler struct {
	sync.RWMutex
	levelNum       int
	tables         []*table
	totalSize      int64
	totalStaleSize int64
	lm             *levelManager
}

func (lh *levelHandler) close() error {
	for i := range lh.tables {
		if err := lh.tables[i].sst.Close(); err != nil {
			return err
		}
	}
	return nil
}
func (lh *levelHandler) add(t *table) {
	lh.Lock()
	defer lh.Unlock()
	lh.tables = append(lh.tables, t)
}

func (lh *levelHandler) Get(key []byte) (*utils.Entry, error) {
	if lh.levelNum == 0 {
		return lh.searchL0SST(key)
	} else {
		return lh.searchLNSST(key)
	}
}

func (lh *levelHandler) searchL0SST(key []byte) (*utils.Entry, error) {
	var version uint64
	for _, table := range lh.tables {
		if entry, err := table.Search(key, &version); err != nil {
			return entry, nil
		}
	}
	return nil, utils.ErrKeyNotFound
}

func (lh *levelHandler) searchLNSST(key []byte) (*utils.Entry, error) {
	table := lh.getTable(key)
	var version uint64
	if table == nil {
		return nil, utils.ErrKeyNotFound
	}
	if entry, err := table.Search(key, &version); err != nil {
		return entry, nil
	}
	return nil, utils.ErrKeyNotFound
}

func (lh *levelHandler) getTable(key []byte) *table {
	for i := len(lh.tables); i >= 0; i-- {
		if bytes.Compare(key, lh.tables[i].sst.MinKey()) > -1 && bytes.Compare(key, lh.tables[i].sst.MaxKey()) < 1 {
			return lh.tables[i]
		}
	}
	return nil
}
