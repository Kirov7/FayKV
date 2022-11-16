package lsm

import "github.com/Kirov7/FayKV/utils"

type Iterator struct {
	it    Item
	iters []utils.Iterator
}
type Item struct {
	e *utils.Entry
}

func (it *Item) Entry() *utils.Entry {
	return it.e
}

func (lsm *LSM) NewIterators(opt *utils.Options) []utils.Iterator {
	panic("todo")
}
func (iter *Iterator) Next() {
	iter.iters[0].Next()
}
func (iter *Iterator) Valid() bool {
	return iter.iters[0].Valid()
}
func (iter *Iterator) Rewind() {
	iter.iters[0].Rewind()
}
func (iter *Iterator) Item() utils.Item {
	return iter.iters[0].Item()
}
func (iter *Iterator) Close() error {
	return nil
}

func (iter *Iterator) Seek(key []byte) {
}
