package lsm

import (
	"github.com/Kirov7/FayKV/persistent"
	"github.com/Kirov7/FayKV/utils"
)

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

func (lsm LSM) Set(entry *utils.Entry) (err error) {
	if entry == nil || len(entry.Key) == 0 {
		return utils.ErrEmptyKey
	}
	lsm.closer.Add(1)
	defer lsm.closer.Done()

	if int64(lsm.memTable.wal.Size())+int64(persistent.EstimateWalCodecSize(entry)) > lsm.option.MemTableSize {
		lsm.Seal()
	}
	if err = lsm.memTable.set(entry); err != nil {
		return err
	}
	for _, immutable := range lsm.immutables {
		if err = lsm.levels.flush(immutable); err != nil {
			return err
		}
		err = immutable.close()
		utils.Panic(err)
	}
	if len(lsm.immutables) != 0 {
		lsm.immutables = make([]*memTable, 0)
	}
	return err
}

func (lsm *LSM) recovery() (*memTable, []*memTable) {
	panic("todo")
}

func (lsm *LSM) Close() error {
	panic("todo")
}

// Seal seal the full memTable
func (lsm *LSM) Seal() {
	lsm.immutables = append(lsm.immutables, lsm.memTable)
	lsm.memTable = lsm.NewMemtable()
}
