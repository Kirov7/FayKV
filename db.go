package FayKV

import (
	"github.com/Kirov7/FayKV/inmemory"
	"github.com/Kirov7/FayKV/lsm"
	"github.com/Kirov7/FayKV/utils"
	"sync"
)

type KvAPI interface {
	Set(data *inmemory.Element) error
	Get(key []byte) (*utils.Entry, error)
	Del(key []byte) error
	Info() *Stats
	NewIterator(opt *utils.Options) utils.Iterator
	Close() error
}

type DB struct {
	sync.RWMutex
	opt   *Options
	stats *Stats
	lsm   *lsm.LSM
}

func Open(opt *Options) *DB {
	c := utils.NewCloser()
	db := &DB{opt: opt}
	// init LSM structure
	db.lsm = lsm.NewLSM(&lsm.Options{
		WorkDir:             opt.WorkDir,
		MemTableSize:        opt.MemTableSize,
		SSTableMaxSize:      opt.SSTableMaxSz,
		BlockSize:           8 * 1024,
		BloomFalsePositive:  0, //0.01,
		BaseLevelSize:       10 << 20,
		LevelSizeMultiplier: 10,
		BaseTableSize:       5 << 20,
		TableSizeMultiplier: 2,
		NumLevelZeroTables:  15,
		MaxLevelNum:         7,
		NumCompactors:       1,
	})
	// Example Initialize statistics
	db.stats = newStats(opt)
	// Start the merge compression process for the sstable
	go db.lsm.StartCompacter()
	c.Add(1)
	// todo init worker channel

	return db
}
func (db *DB) Set(data *inmemory.Element) error {
	// todo implement there
	panic("todo")
}

func (db *DB) Get(key []byte) (*utils.Entry, error) {
	// todo implement there
	panic("todo")
}

func (db *DB) Del(key []byte) error {
	// todo implement there
	panic("todo")
}

func (db *DB) Info() *Stats {
	return db.stats
}

func (db *DB) NewIterator(opt *utils.Options) utils.Iterator {
	// todo implement there
	panic("todo")
}

func (db *DB) Close() error {
	// todo implement there
	panic("todo")
}
