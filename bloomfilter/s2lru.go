package bloomfilter

import (
	"container/list"
	"fmt"
)

type segmentedLRU struct {
	data                     map[uint64]*list.Element
	stageOneCap, stageTwoCap int
	stageOne, stageTwo       *list.List
}

const (
	STAGE_ONE = iota + 1
	STAGE_TWO
)

func newS2LRU(data map[uint64]*list.Element, stageOneCap, stageTwoCap int) *segmentedLRU {
	return &segmentedLRU{
		data:        data,
		stageOneCap: stageOneCap,
		stageTwoCap: stageTwoCap,
		stageOne:    list.New(),
		stageTwo:    list.New(),
	}
}

func (s2lru *segmentedLRU) add(newItem storeItem) {
	// 先金泰的都放 stageOne
	newItem.stage = STAGE_ONE
	// 如果 stageOne 没满, 整个LFU区域也没满
	if s2lru.stageOne.Len() < s2lru.stageOneCap || s2lru.Len() < s2lru.stageOneCap+s2lru.stageTwoCap {
		s2lru.data[newItem.key] = s2lru.stageOne.PushFront(&newItem)
		return
	}
	// 走到这里说明 stageOne 满了,或者整个LFU都满了
	// 则需要载 stageOne 淘汰数据
	e := s2lru.stageOne.Back()
	item := e.Value.(*storeItem)
	// 淘汰数据
	delete(s2lru.data, item.key)
	*item = newItem
	s2lru.data[item.key] = e
	s2lru.stageOne.MoveToFront(e)
}

func (s2lru *segmentedLRU) get(v *list.Element) {
	item := v.Value.(*storeItem)

	// 若访问的缓存数据已经载StageTwo,只需要按照LRU规则提前即可
	if item.stage == STAGE_TWO {
		s2lru.stageTwo.MoveToFront(v)
		return
	}
	// 若访问的数据还在StageOne,那么两次被访问倒,就需要提升到StageTwo阶段了
	if s2lru.stageTwo.Len() < s2lru.stageTwoCap {
		s2lru.stageOne.Remove(v)
		item.stage = STAGE_TWO
		s2lru.data[item.key] = s2lru.stageTwo.PushFront(item)
		return
	}
	// 新数据加入StageTwo, 需要淘汰旧数据
	// StageTwo 中淘汰的数据不会丢失,会进入StageOne
	// StageOne 中,访问频率低的数据,可能会被淘汰
	back := s2lru.stageTwo.Back()
	bItem := back.Value.(*storeItem)
	*bItem, *item = *item, *bItem
	bItem.stage = STAGE_TWO
	item.stage = STAGE_ONE

	s2lru.data[item.key] = v
	s2lru.data[bItem.key] = back
	s2lru.stageOne.MoveToFront(v)
	s2lru.stageOne.MoveToFront(back)
}
func (s2lru *segmentedLRU) victim() *storeItem {
	// 如果s2lru的容量未满,不需要淘汰
	if s2lru.Len() < s2lru.stageOneCap+s2lru.stageTwoCap {
		return nil
	}
	// 如果已经满了, 则需要从20%的区域淘汰数据,直接从末尾部拿最后一个数据即可
	v := s2lru.stageOne.Back()
	return v.Value.(*storeItem)
}

func (s2lru *segmentedLRU) String() (s string) {
	for e := s2lru.stageTwo.Front(); e != nil; e = e.Next() {
		s += fmt.Sprintf("%v,", e.Value.(*storeItem).value)
	}
	s += fmt.Sprintf(" | ")
	for e := s2lru.stageOne.Front(); e != nil; e = e.Next() {
		s += fmt.Sprintf("%v,", e.Value.(*storeItem).value)
	}
	return s
}

func (s2lru *segmentedLRU) Len() int {
	return s2lru.stageOne.Len() + s2lru.stageTwo.Len()
}
