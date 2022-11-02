package FayKV

import (
	"github.com/Kirov7/FayKV/inmemory"
)

type KvAPI interface {
	Set(data *inmemory.Element) error
	Get(key []byte) (*inmemory.Entry, error)
	Del(key []byte) error
	Info() *Stats
	NewIterator(opt *inmemory.Options) inmemory.Iterator
	Close() error
}

type DB struct {
	stats *Stats
}

func (db *DB) Set(data *inmemory.Element) error {
	// todo implement there
	panic("todo")
}

func (db *DB) Get(key []byte) (*inmemory.Entry, error) {
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

func (db *DB) NewIterator(opt *inmemory.Options) inmemory.Iterator {
	// todo implement there
	panic("todo")
}

func (db *DB) Close() error {
	// todo implement there
	panic("todo")
}
