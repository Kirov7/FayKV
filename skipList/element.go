package skipList

type Element struct {
	// levels[i] 第 i 层所指向的下一个节点
	levels []*Element
	entry  *Entry
	score  float64
}

func newElement(score float64, entry *Entry, level int) *Element {
	return &Element{
		levels: make([]*Element, level+1),
		entry:  entry,
		score:  score,
	}
}

func (elem *Element) Entry() *Entry {
	return elem.entry
}
