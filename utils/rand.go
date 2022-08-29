package utils

import (
	"math/rand"
	"time"
)

var (
	r = rand.New(rand.NewSource(time.Now().UnixNano()))
)

func GetRand() *rand.Rand {
	return r
}
