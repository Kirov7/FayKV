package cache

import (
	"container/list"

	xxhash "github.com/cespare/xxhash/v2"
	"sync"
	"unsafe"
)

type Cache struct {
	m         sync.RWMutex
	lru       *windowLRU
	slru      *segmentedLRU
	bf        *BloomFilter
	c         *cmSketch
	t         int32
	threshold int32
	data      map[uint64]*list.Element
}

type Options struct {
	lruPct uint8
}

// NewCache size: 要缓存的数据数量
func NewCache(size int) *Cache {
	// 定义window部分缓存所占百分比,这里定义为1%
	const lruPct = 1
	// 计算window部分的容量
	lruSize := (lruPct * size) / 100
	if lruSize < 1 {
		lruSize = 1
	}
	// 计算LFU部分的缓存容量
	slruSize := int(float64(size) * ((100 - lruPct) / 100.0))
	if slruSize < 1 {
		slruSize = 1
	}
	// LFU 分为两部分, stageOne部分占比20%
	slru1 := int(0.2 * float64(slruSize))
	if slru1 < 1 {
		slru1 = 1
	}
	data := make(map[uint64]*list.Element, size)
	return &Cache{
		lru:  newWindowLRU(lruSize, data),
		slru: newS2LRU(data, slru1, slruSize-slru1),
		bf:   NewBloomFilter(size, 0.01),
		c:    newCmSketch(int64(size)),
		data: data,
	}
}

// Set
// todo Optimize this method by using generics
func (c *Cache) Set(key, value interface{}) bool {
	c.m.Lock()
	defer c.m.Lock()
	return c.set(key, value)
}

func (c *Cache) set(key, value interface{}) bool {
	// keyHash 用来快速定位, conflictHash 用来判断冲突
	keyHash, conflictHash := c.key2Hash(key)
	// 刚放进去的缓存都先放到window lru 中, 所以stage = 0
	i := storeItem{
		stage:    0,
		key:      keyHash,
		conflict: conflictHash,
		value:    value,
	}
	// 如果window 已满, 返回被淘汰的数据
	eitem, evicted := c.lru.add(i)

	if !evicted {
		return true
	}

	// 如果window中有被淘汰的数据,会走到这里
	// 需要从LFU的stageOne 部分找到一个淘汰者
	// 二者进行再次比较
	victim := c.slru.victim()
	// 如果LFU未满,那么window lru的淘汰数据,可以进入stageOne
	if victim == nil {
		c.slru.add(eitem)
		return true
	}
	// 先在bloomfilter中查找
	// 如果存在,说明访问频率 >= 2
	if !c.bf.Allow(uint32(eitem.key)) {
		return true
	}
	// 估算windowlru和LFU中淘汰数据, 历史访问频次
	// 访问频率高的,更有资格留下
	vcount := c.c.Estimate(victim.key)
	ocount := c.c.Estimate(eitem.key)
	if ocount < vcount {
		return true
	}
	// 留下来的进入 stageOne
	c.slru.add(eitem)
	return true
}

func (c *Cache) Get(key interface{}) (interface{}, bool) {
	c.m.RLock()
	defer c.m.RUnlock()
	return c.get(key)
}

func (c *Cache) get(key interface{}) (interface{}, bool) {
	c.t++
	if c.t == c.threshold {
		c.c.Reset()
		c.bf.Reset()
		c.t = 0
	}
	keyHash, confilctHash := c.key2Hash(key)
	val, ok := c.data[keyHash]
	if !ok {
		c.bf.Allow(uint32(keyHash))
		c.c.Increment(keyHash)
		return nil, false
	}
	item := val.Value.(*storeItem)
	if item.conflict != confilctHash {
		c.bf.Allow(uint32(keyHash))
		c.c.Increment(keyHash)
		return nil, false
	}
	c.bf.Allow(uint32(keyHash))
	c.c.Increment(item.key)

	v := item.value
	if item.stage == 0 {
		c.lru.get(val)
	} else {
		c.slru.get(val)
	}
	return v, true
}

func (c *Cache) Del(key interface{}) (interface{}, bool) {
	c.m.Lock()
	defer c.m.Unlock()
	return c.del(key)
}

func (c *Cache) del(key interface{}) (interface{}, bool) {
	keyHash, conflictHash := c.key2Hash(key)
	val, ok := c.data[keyHash]
	if !ok {
		return 0, false
	}
	item := val.Value.(*storeItem)

	if conflictHash != 0 && (conflictHash != item.conflict) {
		return 0, false
	}
	delete(c.data, keyHash)
	return item.conflict, true
}

func (c *Cache) key2Hash(key interface{}) (uint64, uint64) {
	if key == nil {
		return 0, 0
	}
	switch k := key.(type) {
	case uint64:
		return k, 0
	case string:
		return MemHashString(k), xxhash.Sum64String(k)
	case []byte:
		return MemHash(k), xxhash.Sum64(k)
	case byte:
		return uint64(k), 0
	case int:
		return uint64(k), 0
	case int32:
		return uint64(k), 0
	case uint32:
		return uint64(k), 0
	case int64:
		return uint64(k), 0
	default:
		panic("Key type not supported")
	}
}

type stringStruct struct {
	str unsafe.Pointer
	len int
}

//go:noescape
//go:linkname memhash runtime.memhash
func memhash(p unsafe.Pointer, h, s uintptr) uintptr

func MemHashString(str string) uint64 {
	ss := (*stringStruct)(unsafe.Pointer(&str))
	return uint64(memhash(ss.str, 0, uintptr(ss.len)))
}

func MemHash(data []byte) uint64 {
	ss := (*stringStruct)(unsafe.Pointer(&data))
	return uint64(memhash(ss.str, 0, uintptr(ss.len)))
}

func (c *Cache) String() string {
	var s string
	s += c.lru.String() + " | " + c.slru.String()
	return s
}
