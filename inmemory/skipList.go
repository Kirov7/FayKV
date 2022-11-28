package inmemory

import (
	"github.com/Kirov7/FayKV/utils"
	"log"
	"math"
	"sync/atomic"
	_ "unsafe"
)

const (
	maxHeight      = 20
	heightIncrease = math.MaxUint32 / 3
)

type node struct {
	// Multiple parts of the value are encoded as a single uint64 so that it
	// can be atomically loaded and stored:
	//   value offset: uint32 (bits 0-31)
	//   value size  : uint16 (bits 32-63)
	value uint64

	// A byte slice is 24 bytes. We are trying to save space here.
	keyOffset uint32 // Immutable. No need to lock to access key.
	keySize   uint16 // Immutable. No need to lock to access key.

	// Height of the tower.
	height uint16

	// Most nodes do not need to use the full height of the tower, since the
	// probability of each successive level decreases exponentially. Because
	// these elements are never accessed, they do not need to be allocated.
	// Therefore, when a node is allocated in the memPool, its memory footprint
	// is deliberately truncated to not include unneeded tower elements.
	//
	// All accesses to elements should use CAS operations, with no need to lock.
	tower [maxHeight]uint32
}

type SkipList struct {
	height     int32 // Current height. 1 <= height <= kMaxHeight. CAS.
	headOffset uint32
	ref        int32
	memPool    *MemPool
	OnClose    func()
}

func newNode(memPool *MemPool, key []byte, v utils.ValueStruct, height int) *node {
	nodeOffset := memPool.putNode(height)
	keyOffset := memPool.putKey(key)
	val := encodeValue(memPool.putVal(v), v.EncodedSize())

	node := memPool.getNode(nodeOffset)
	node.keyOffset = keyOffset
	node.keySize = uint16(height)
	node.value = val
	return node
}

func encodeValue(valOffset uint32, valSize uint32) uint64 {
	return uint64(valSize)<<32 | uint64(valOffset)
}

func decodeValue(value uint64) (valOffset, valSize uint32) {
	valOffset = uint32(value)
	valSize = uint32(value >> 32)
	return
}

func NewSkipList(memPoolSize int64) *SkipList {
	memPool := NewMemPool(memPoolSize)
	head := newNode(memPool, nil, utils.ValueStruct{}, maxHeight)
	headOff := memPool.getNodeOffset(head)
	return &SkipList{
		height:     1,
		headOffset: headOff,
		ref:        1,
		memPool:    memPool,
	}
}

func (s *SkipList) IncrRef() {
	atomic.AddInt32(&s.ref, 1)
}

func (n *node) getValueOffset() (uint32, uint32) {
	value := atomic.LoadUint64(&n.value)
	return decodeValue(value)
}

func (n *node) key(memPool *MemPool) []byte {
	return memPool.getKey(n.keyOffset, n.keySize)
}

func (n *node) setValue(memPool *MemPool, vo uint64) {
	atomic.StoreUint64(&n.value, vo)
}

func (n *node) getNextOffset(h int) uint32 {
	return atomic.LoadUint32(&n.tower[h])
}

func (n *node) casNextOffset(h int, old, val uint32) bool {
	return atomic.CompareAndSwapUint32(&n.tower[h], old, val)
}

// getVs return ValueStruct stored in node
func (n *node) getVs(memPool *MemPool) utils.ValueStruct {
	valOffset, valSize := n.getValueOffset()
	return memPool.getVal(valOffset, valSize)
}

func (s *SkipList) getHeight() int32 {
	return atomic.LoadInt32(&s.height)
}

func (s *SkipList) randomHeight() int {
	h := 1
	for h < maxHeight && FastRand() <= heightIncrease {
		h++
	}
	return h
}

func (s *SkipList) getNext(nd *node, height int) *node {
	return s.memPool.getNode(nd.getNextOffset(height))
}

func (s *SkipList) getHead() *node {
	return s.memPool.getNode(s.headOffset)
}

func (s *SkipList) Set(e *utils.Entry) {
	// Since we allow overwrite, we may not need to create a new node. We might not even need to
	// increase the height. Let's defer these actions.
	key, v := e.Key, utils.ValueStruct{
		Meta:      e.Meta,
		Value:     e.Value,
		ExpiresAt: e.ExpiresAt,
		Version:   e.Version,
	}

	listHeight := s.getHeight()
	var prev [maxHeight + 1]uint32
	var next [maxHeight + 1]uint32
	// 最高层的指针
	prev[listHeight] = s.headOffset
	for i := int(listHeight) - 1; i >= 0; i-- {
		// Use higher level to speed up for current level.
		prev[i], next[i] = s.findSpliceForLevel(key, prev[i+1], i)
		// 发现已有相同的key
		if prev[i] == next[i] {
			vo := s.memPool.putVal(v)
			encValue := encodeValue(vo, v.EncodedSize())
			prevNode := s.memPool.getNode(prev[i])
			prevNode.setValue(s.memPool, encValue)
			return
		}
	}

	// We do need to create a new node.
	newHeight := s.randomHeight()
	x := newNode(s.memPool, key, v, newHeight)

	// Try to increase s.height via CAS.
	listHeight = s.getHeight()
	for newHeight > int(listHeight) {
		if atomic.CompareAndSwapInt32(&s.height, listHeight, int32(newHeight)) {
			// Successfully increased skiplist.height.
			break
		}
		listHeight = s.getHeight()
	}

	// We always insert from the base level and up. After you add a node in base level, we cannot
	// create a node in the level above because it would have discovered the node in the base level.
	for i := 0; i < newHeight; i++ {
		for {
			if s.memPool.getNode(prev[i]) == nil {
				AssertTrue(i > 1)
				// This cannot happen in base level.
				// We haven't computed prev, next for this level because height exceeds old listHeight.
				// For these levels, we expect the lists to be sparse, so we can just search from head.
				prev[i], next[i] = s.findSpliceForLevel(key, s.headOffset, i)
				// Someone adds the exact same key before we are able to do so. This can only happen on
				// the base level. But we know we are not on the base level.
				AssertTrue(prev[i] != next[i])
			}
			x.tower[i] = next[i]
			pnode := s.memPool.getNode(prev[i])
			if pnode.casNextOffset(i, next[i], s.memPool.getNodeOffset(x)) {
				// Managed to insert x between prev[i] and next[i]. Go to the next level.
				break
			}
			// CAS failed. We need to recompute prev and next.
			// It is unlikely to be helpful to try to use a different level as we redo the search,
			// because it is unlikely that lots of nodes are inserted between prev[i] and next[i].
			prev[i], next[i] = s.findSpliceForLevel(key, prev[i], i)
			// 如果被占用则直接覆盖重置
			if prev[i] == next[i] {
				if i == 0 {
					log.Fatal("Equality can happen only on base level: %d", i)
				}
				vo := s.memPool.putVal(v)
				encValue := encodeValue(vo, v.EncodedSize())
				prevNode := s.memPool.getNode(prev[i])
				prevNode.setValue(s.memPool, encValue)
				return
			}
		}
	}
}

// findSpliceForLevel returns (outBefore, outAfter) with outBefore.key <= key <= outAfter.key.
// The input "before" tells us where to start looking.
// If we found a node with the same key, then we return outBefore = outAfter.
// Otherwise, outBefore.key < key < outAfter.key.
// 找出key所在的前后节点
func (s *SkipList) findSpliceForLevel(key []byte, before uint32, level int) (uint32, uint32) {
	for {
		// Assume before.key < key.
		// 获取该层的上一个节点
		beforeNode := s.memPool.getNode(before)
		next := beforeNode.getNextOffset(level)
		// 获取该层的下一个节点
		nextNode := s.memPool.getNode(next)
		// 如果下一个节点为空(该节点为该层的末尾节点), 正常返回
		if nextNode == nil {
			return before, next
		}
		// 获取下一个节点的key,与当前key进行比较
		nextKey := nextNode.key(s.memPool)
		cmp := CompareKeys(key, nextKey)
		if cmp == 0 {
			// Equality case.
			// 如果相等, 返回相等的两个节点, 待外层特殊处理
			return next, next
		}
		if cmp < 0 {
			// before.key < key < next.key. We are done for this level.
			// key 小于下一个节点
			return before, next
		}
		// 向右迭代节点
		before = next // Keep moving right on this level.
	}
}

func (s *SkipList) Search(key []byte) utils.ValueStruct {
	n, _ := s.findNear(key, false, true) // findGreaterOrEqual.
	if n == nil {
		return utils.ValueStruct{}
	}

	nextKey := s.memPool.getKey(n.keyOffset, n.keySize)
	if !SameKey(key, nextKey) {
		return utils.ValueStruct{}
	}

	valOffset, valSize := n.getValueOffset()
	vs := s.memPool.getVal(valOffset, valSize)
	vs.ExpiresAt = ParseTs(nextKey)
	return vs
}

func (s *SkipList) findNear(key []byte, left bool, allowEqual bool) (*node, bool) {
	// find the header
	curNode := s.getHead()
	// find current height of the node
	level := int(s.getHeight() - 1)
	for {
		// get the next node in the specified level
		next := s.getNext(curNode, level)
		// at the last node in current level
		if next == nil {
			// if not at the first level enter the lower level
			if level > 0 {
				level--
				continue
			}
			// the last node of the inmemory don`t have other bigger node
			if !left {
				return nil, false
			}
			// the first node of the inmemory don`t have other smaller node
			if curNode == s.getHead() {
				return nil, false
			}
			return curNode, false
		}
		nextKey := next.key(s.memPool)
		cmp := CompareKeys(key, nextKey)
		if cmp > 0 {
			curNode = next
			continue
		}
		if cmp == 0 {
			if allowEqual {
				return next, true
			}
			if !left {
				return s.getNext(next, 0), false
			}
			if level > 0 {
				level--
				continue
			}
			if curNode == s.getHead() {
				return nil, false
			}
			return curNode, false
		}
		if level > 0 {
			level--
			continue
		}
		if !left {
			return next, false
		}
		if curNode == s.getHead() {
			return nil, false
		}
		return curNode, false
	}
}

// MemSize returns the size of the Skiplist in terms of how much memory is used within its internal
// arena.
func (s *SkipList) MemSize() int64 { return s.memPool.size() }

// DecrRef decrements the refcount, deallocating the Skiplist when done using it
func (s *SkipList) DecrRef() {
	newRef := atomic.AddInt32(&s.ref, -1)
	if newRef > 0 {
		return
	}
	if s.OnClose != nil {
		s.OnClose()
	}

	s.memPool = nil
}

//go:linkname FastRand runtime.fastrand
func FastRand() uint32

type SkipListIterator struct {
	list *SkipList
	n    *node
}

func (s *SkipList) NewSkipListIterator() utils.Iterator {
	s.IncrRef()
	return &SkipListIterator{list: s}
}

func (s *SkipListIterator) Next() {
	AssertTrue(s.Valid())
	s.n = s.list.getNext(s.n, 0)
}

func (s *SkipListIterator) Valid() bool {
	return s.n != nil
}

func (s *SkipListIterator) Rewind() {
	s.SeekToFirst()
}

func (s *SkipListIterator) Item() utils.Item {
	return &utils.Entry{
		Key:       s.Key(),
		Value:     s.Value().Value,
		ExpiresAt: s.Value().ExpiresAt,
		Meta:      s.Value().Meta,
		Version:   s.Value().Version,
	}
}

func (s *SkipListIterator) Close() error {
	s.list.DecrRef()
	return nil
}

func (s *SkipListIterator) Seek(target []byte) {
	s.n, _ = s.list.findNear(target, false, true) // find >=.
}

// Key returns the key at the current position.
func (s *SkipListIterator) Key() []byte {
	return s.list.memPool.getKey(s.n.keyOffset, s.n.keySize)
}

// Value returns value.
func (s *SkipListIterator) Value() utils.ValueStruct {
	valOffset, valSize := s.n.getValueOffset()
	return s.list.memPool.getVal(valOffset, valSize)
}

// SeekToFirst seeks position at the first entry in list.
// Final state of iterator is Valid() iff list is not empty.
func (s *SkipListIterator) SeekToFirst() {
	s.n = s.list.getNext(s.list.getHead(), 0)
}
