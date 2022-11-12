package lsm

import (
	fayCache "github.com/Kirov7/FayKV/cache"
)

type TableCache struct {
	indexs *fayCache.Cache // key fid， value table
	blocks *fayCache.Cache // key fid_blockOffset  value block []byte
}

type blockBuffer struct {
	b []byte
}

const defaultCacheSize = 1024

// close
func (c *TableCache) close() error {
	return nil
}

// newCache
func newCache(opt *Options) *TableCache {
	return &TableCache{indexs: fayCache.NewCache(defaultCacheSize), blocks: fayCache.NewCache(defaultCacheSize)}
}

func (c *TableCache) addIndex(fid uint64, t *table) {
	c.indexs.Set(fid, t)
}
