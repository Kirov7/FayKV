package lsm

import (
	"github.com/Kirov7/FayKV/cache"
	"github.com/Kirov7/FayKV/persistent"
	"github.com/Kirov7/FayKV/utils"
)

type levelManager struct {
	maxFID       uint64
	opt          *Options
	cache        *cache.Cache
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
	panic("todo")
}

type levelHandler struct {
	//todo
}
