package persistent

import (
	"bytes"
	"os"
	"sync"
)

type WalFile struct {
	lock    *sync.RWMutex
	f       *MmapFile
	opts    *Options
	buf     *bytes.Buffer
	size    uint32
	writeAt uint32
}

func (wf *WalFile) Fid() uint64 {
	return wf.opts.FID
}

func (wf *WalFile) Close() error {
	fileName := wf.f.Fd.Name()
	if err := wf.f.Close(); err != nil {
		return err
	}
	return os.Remove(fileName)
}

func (wf *WalFile) Name() string {
	return wf.f.Fd.Name()
}

func (wf *WalFile) Size() uint32 {
	return wf.writeAt
}
