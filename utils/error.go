package utils

import "github.com/pkg/errors"

var (
	ErrChecksumMismatch = errors.New("checksum mismatch")
	ErrEmptyKey         = errors.New("Key cannot be empty")
	ErrKeyNotFound      = errors.New("Key Not Found")
	ErrReWriteFailure   = errors.New("reWrite failure")
	ErrBadMagic         = errors.New("bad magic")
	ErrBadChecksum      = errors.New("bad check sum")
)

// Panic if err != nil then panic
func Panic(err error) {
	if err != nil {
		panic(err)
	}
}

func PanicTwoParams(_ interface{}, err error) {
	Panic(err)
}

func CondPanic(condition bool, err error) {
	if condition {
		Panic(err)
	}
}
