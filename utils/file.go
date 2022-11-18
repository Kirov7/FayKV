package utils

import (
	"fmt"
	"io/ioutil"
	"path"
	"path/filepath"
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

func LoadIDMap(dir string) map[uint64]struct{} {
	fileInfos, _ := ioutil.ReadDir(dir)
	idMap := make(map[uint64]struct{})
	for _, info := range fileInfos {
		if info.IsDir() {
			continue
		}
		fileID := FID(info.Name())
		if fileID != 0 {
			idMap[fileID] = struct{}{}
		}
	}
	return idMap
}

// FileNameSSTable  sst file name
func FileNameSSTable(dir string, id uint64) string {
	return filepath.Join(dir, fmt.Sprintf("%05d.sst", id))
}
