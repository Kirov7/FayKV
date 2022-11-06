package utils

import (
	"path"
	"strconv"
	"strings"
)

// FID Get its fid from the file name
func FID(name string) uint64 {
	name = path.Base(name)
	if !strings.HasSuffix(name, ".sst") {
		return 0
	}
	//	suffix := name[len(fileSuffix):]
	name = strings.TrimSuffix(name, ".sst")
	id, err := strconv.Atoi(name)
	if err != nil {
		Panic(err)
		return 0
	}
	return uint64(id)
}
