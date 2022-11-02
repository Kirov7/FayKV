package lsm

import "github.com/Kirov7/FayKV/utils"

type LSM struct {
	memTable   *memTable
	immutables []*memTable
	levels     *levelManager
	option     *Options
	closer     *utils.Closer
	maxMemFID  uint32
}

type Options struct {
	WorkDir             string
	SSTableMaxSize      int64
	MemTableSize        int64
	BlockSize           int
	BloomFalsePositive  float64
	NumCompactors       int
	BaseLevelSize       int64
	LevelSizeMultiplier int
	TableSizeMultiplier int
	BaseTableSize       int64
	NumLevelZeroTables  int
	MaxLevelNum         int
	DiscardStatsCh      *chan map[uint32]int64
}

func NewLSM(opt *Options) *LSM {
	lsm := &LSM{option: opt}
	lsm.levels = lsm.initLevelManager(opt)
	lsm.memTable, lsm.immutables = lsm.recovery()
	lsm.closer = utils.NewCloser()
	return lsm
}

func (lsm *LSM) recovery() (*memTable, []*memTable) {
	panic("todo")
}

func (lsm *LSM) Close() error {
	panic("todo")
}
