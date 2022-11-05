package lsm

import (
	fayCache "github.com/Kirov7/FayKV/cache"
)

type cache struct {
	indexs *fayCache.Cache // key fid， value table
	blocks *fayCache.Cache // key fid_blockOffset  value block []byte
}

type blockBuffer struct {
	b []byte
}

const defaultCacheSize = 1024

// close
func (c *cache) close() error {
	return nil
}

// newCache
func newCache(opt *Options) *cache {
	return &cache{indexs: fayCache.NewCache(defaultCacheSize), blocks: fayCache.NewCache(defaultCacheSize)}
}

func (c *cache) addIndex(fid uint64, t *table) {
	c.indexs.Set(fid, t)
}
