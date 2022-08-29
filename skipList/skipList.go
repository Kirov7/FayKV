package skipList

import (
	"FayKV/utils"
	"math/rand"
	"sync"
)

const (
	defaultMaxHeight = 64
)

type SkipList struct {
	header *Element

	rand *rand.Rand

	maxLevel int
	length   int
	lock     sync.RWMutex
	size     int64
}

func NewSkipList() *SkipList {
	header := &Element{
		levels: make([]*Element, defaultMaxHeight),
	}

	return &SkipList{
		header:   header,
		maxLevel: defaultMaxHeight - 1,
		rand:     utils.GetRand(),
	}
}

func (list *SkipList) Add(data *Entry) error {
	//todo implement there
	panic("Need to be implemented")
}

func (list *SkipList) Search(key []byte) (e *Entry) {
	//todo implement there
	panic("Need to be implemented")
}

func (list *SkipList) Close() error {
	return nil
}

func (list *SkipList) calcScore(key []byte) (score float64) {
	//todo implement there
	panic("Need to be implemented")
}

func (list *SkipList) compare(score float64, key []byte, next *Element) int {
	//todo implement there
	panic("Need to be implemented")
}

func (list *SkipList) randLevel() int {
	//todo implement there
	panic("Need to be implemented")
}

func (list *SkipList) Size() int64 {
	return list.size
}
