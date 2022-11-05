package inmemory

import "github.com/Kirov7/FayKV/utils"

type Element struct {
	levels []*Element   // levels[i] 第 i 层所指向的下一个节点
	entry  *utils.Entry // 存储的键值对
	score  float64      // 通过计算得出的分数,用于进行快速比较
}

func newElement(score float64, entry *utils.Entry, level int) *Element {
	return &Element{
		levels: make([]*Element, level+1),
		entry:  entry,
		score:  score,
	}
}

func (elem *Element) Entry() *utils.Entry {
	return elem.entry
}
