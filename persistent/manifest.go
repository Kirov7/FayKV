package persistent

import (
	"os"
	"sync"
)

type ManifestFile struct {
	opt                       *Options
	f                         *os.File
	lock                      sync.Mutex
	deletionsRewriteThreshold int
	manifest                  *Manifest
}

type Manifest struct {
	Levels    []levelManifest
	Tables    map[uint64]TableManifest
	Creations int
	Deletions int
}

type TableManifest struct {
	Level    uint8
	Checksum []byte
}

type levelManifest struct {
	Tables map[uint64]struct{}
}

func (mf *ManifestFile) AddTableMeta(levelNum int, t *TableMeta) (err error) {
	panic("todo")
}

type TableMeta struct {
	ID       uint64
	Checksum []byte
}
