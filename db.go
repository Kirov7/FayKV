package FayKV

import (
	"github.com/Kirov7/FayKV/skipList"
	"github.com/Kirov7/FayKV/skipList/iterator"
)

type KvAPI interface {
	Set(data *skipList.Element) error
	Get(key []byte) (*skipList.Entry, error)
	Del(key []byte) error
	Info() *Stats
	NewIterator(opt *iterator.Options) iterator.Iterator
	Close() error
}

type DB struct {
	stats *Stats
}

func (db *DB) Set(data *skipList.Element) error {
	// todo implement there
	panic("todo")
}

func (db *DB) Get(key []byte) (*skipList.Entry, error) {
	// todo implement there
	panic("todo")
}

func (db *DB) Del(key []byte) error {
	// todo implement there
	panic("todo")
}

func (db *DB) Info() *Stats {
	return db.stats
}

func (db *DB) NewIterator(opt *iterator.Options) iterator.Iterator {
	// todo implement there
	panic("todo")
}

func (db *DB) Close() error {
	// todo implement there
	panic("todo")
}
