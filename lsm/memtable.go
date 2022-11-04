package lsm

import (
	"bytes"
	"fmt"
	"github.com/Kirov7/FayKV/inmemory"
	"github.com/Kirov7/FayKV/persistent"
	"os"
	"path/filepath"
	"sync/atomic"
)

type memTable struct {
	lsm        *LSM
	wal        *persistent.WalFile
	sl         *inmemory.SkipList
	buf        *bytes.Buffer
	maxVersion uint64
}

func (lsm *LSM) NewMemtable() *memTable {
	newFid := atomic.AddUint64(&(lsm.levels.maxFID), 1)
	fileOpt := &persistent.Options{
		FID:      newFid,
		FileName: mtFilePath(lsm.option.WorkDir, newFid),
		Dir:      lsm.option.WorkDir,
		Flag:     os.O_CREATE | os.O_RDWR,
		MaxSize:  int(lsm.option.MemTableSize),
	}
	return &memTable{wal: persistent.OpenWalFile(fileOpt), sl: inmemory.NewSkipList(int64(1 << 20)), lsm: lsm}
}

func (m *memTable) set(entry *inmemory.Entry) error {
	// 写到wal 日志中，防止崩溃
	if err := m.wal.Write(entry); err != nil {
		return err
	}
	// 写到memtable中
	m.sl.Put(entry)
	return nil
}

func (m *memTable) close() error {
	if err := m.wal.Close(); err != nil {
		return err
	}

	return nil
}

func mtFilePath(dir string, fid uint64) string {
	return filepath.Join(dir, fmt.Sprintf("%05d%s", fid, persistent.WalFileExt))
}
