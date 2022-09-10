package FayKV

import "github.com/Kirov7/FayKV/skipList"

type KvAPI interface {
	Set(data *skipList.Element) error
	Get(key []byte) (*skipList.Entry, error)
	Del(key []byte) error
	Info() *Stats
	Close() error
}

type DB struct {
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
	// todo implement there
	panic("todo")
}

func (db *DB) Close() error {
	// todo implement there
	panic("todo")
}
