package FayKV

import "github.com/Kirov7/FayKV/skipList"

type KvAPI interface {
	Set(data *skipList.Element) error
	Get(key []byte) (*skipList.Entry, error)
	Del(key []byte) error
	Info() *Stats
	Close() error
}
