package lsm

import (
	"bytes"
	"github.com/Kirov7/FayKV/inmemory"
	"github.com/Kirov7/FayKV/persistent"
	"github.com/Kirov7/FayKV/utils"
	"sort"
	"sync"
	"sync/atomic"
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

func (lm *levelManager) loadManifest() (err error) {
	lm.manifestFile, err = persistent.OpenManifestFile(&persistent.Options{Dir: lm.opt.WorkDir})
	return err
}

func (lm *levelManager) build() error {
	lm.levels = make([]*levelHandler, 0, lm.opt.MaxLevelNum)
	for i := 0; i < lm.opt.MaxLevelNum; i++ {
		lm.levels = append(lm.levels, &levelHandler{
			levelNum: i,
			tables:   make([]*table, 0),
			lm:       lm,
		})
	}

	manifest := lm.manifestFile.GetManifest()
	// Compare the correctness of the manifest file
	if err := lm.manifestFile.RevertToManifest(utils.LoadIDMap(lm.opt.WorkDir)); err != nil {
		return err
	}
	// Load the index blocks of the sstable one by one to build the cache
	lm.cache = newCache(lm.opt)
	// TODO During initialization, the index structure is placed in the table, which means that all the data is loaded into the memory.
	// This reduces one disk read but increases the memory consumption
	var maxFID uint64
	for fID, tableInfo := range manifest.Tables {
		fileName := utils.FileNameSSTable(lm.opt.WorkDir, fID)
		if fID > maxFID {
			maxFID = fID
		}
		t := openTable(lm, fileName, nil)
		lm.levels[tableInfo.Level].add(t)
		lm.levels[tableInfo.Level].addSize(t) // Records the total file size of a level
	}
	// Sort each layer
	for i := 0; i < lm.opt.MaxLevelNum; i++ {
		lm.levels[i].Sort()
	}
	// Get the maximum fid value
	atomic.AddUint64(&lm.maxFID, maxFID)
	return nil
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
		Checksum: []byte{'m', 'o', 'c', 'k'},
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

func (lh *levelHandler) Sort() {
	lh.Lock()
	defer lh.Unlock()
	if lh.levelNum == 0 {
		// Key range will overlap. Just sort by fileID in ascending order
		// because newer tables are at the end of level 0.
		sort.Slice(lh.tables, func(i, j int) bool {
			return lh.tables[i].fid < lh.tables[j].fid
		})
	} else {
		// Sort tables by keys.
		sort.Slice(lh.tables, func(i, j int) bool {
			return inmemory.CompareKeys(lh.tables[i].sst.MinKey(), lh.tables[j].sst.MinKey()) < 0
		})
	}
}

func (lh *levelHandler) addSize(t *table) {
	lh.totalSize += t.Size()
	lh.totalStaleSize += int64(t.StaleDataSize())
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
