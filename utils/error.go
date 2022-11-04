package utils

import "github.com/pkg/errors"

var (
	ErrChecksumMismatch = errors.New("checksum mismatch")
	ErrEmptyKey         = errors.New("Key cannot be empty")
)

// Panic 如果err 不为nil 则panicc
func Panic(err error) {
	if err != nil {
		panic(err)
	}
}

func Panic2(_ interface{}, err error) {
	Panic(err)
}

func CondPanic(condition bool, err error) {
	if condition {
		Panic(err)
	}
}
