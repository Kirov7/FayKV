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
	Prefix []byte // 前缀
	IsAsc  bool   // 是否升序
}
