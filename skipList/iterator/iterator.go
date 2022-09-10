package iterator

import "github.com/Kirov7/FayKV/skipList"

type Iterator interface {
	Next()
	Valid() bool
	Rewind()
	Item() Item
	Close() error
}

type Item interface {
	Entry() *skipList.Element
}
type Options struct {
	Prefix []byte
	IsAsc  bool
}
