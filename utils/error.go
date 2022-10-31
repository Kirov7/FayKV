package utils

import "github.com/pkg/errors"

var (
	ErrChecksumMismatch = errors.New("checksum mismatch")
)

// Panic 如果err 不为nil 则panicc
func Panic(err error) {
	if err != nil {
		panic(err)
	}
}
