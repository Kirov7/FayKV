package lsm

import "github.com/Kirov7/FayKV/cache"

type levelManager struct {
	maxFID uint64
	opt    *Options
	cache  *cache.Cache
	//manifestFile
	levels       []*levelHandler
	lsm          *LSM
	compactState *compactState
}

type levelHandler struct {
	//todo
}
