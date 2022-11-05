package lsm

import (
	"github.com/Kirov7/FayKV/persistent"
	"github.com/Kirov7/FayKV/utils"
	"sync"
)

type levelManager struct {
	maxFID       uint64
	opt          *Options
	cache        *cache
	manifestFile *persistent.ManifestFile
	levels       []*levelHandler
	lsm          *LSM
	compactState *compactStatus
}

func (lsm *LSM) initLevelManager(opt *Options) *levelManager {
	lm := &levelManager{lsm: lsm}
	lm.compactState = lsm.newCompactStatus()
	lm.opt = opt
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

func (lm *levelManager) flush(immutable *memTable) error {
	// Assign a fid
	fid := immutable.wal.Fid()
	sstName := persistent.FileNameSSTable(lm.opt.WorkDir, fid)
	builder := newTableBuilder(lm.opt)
	iter := immutable.sl.NewSkipListIterator()
	for iter.Rewind(); iter.Valid(); iter.Next() {
		entry := iter.Item().Entry()
		builder.add(entry, false)
	}

	table := openTable(lm, sstName, builder)
	err := lm.manifestFile.AddTableMeta(0, &persistent.TableMeta{
		ID:       fid,
		Checksum: []byte{'f', 'a', 'y', 'd', 'b'},
	})
	utils.Panic(err)
	lm.levels[0].add(table)
	return nil
}

type levelHandler struct {
	sync.RWMutex
	tables []*table
	//todo
}

func (lh *levelHandler) add(t *table) {
	lh.Lock()
	defer lh.Unlock()
	lh.tables = append(lh.tables, t)
}
