package lsm

import (
	"bytes"
	"fmt"
	"github.com/Kirov7/FayKV/inmemory"
	"github.com/Kirov7/FayKV/persistent"
	"github.com/Kirov7/FayKV/utils"
	"github.com/pkg/errors"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
)

type memTable struct {
	lsm        *LSM
	wal        *persistent.WalFile
	sl         *inmemory.SkipList
	buf        *bytes.Buffer
	maxVersion uint64
}

func (lsm *LSM) NewMemTable() *memTable {
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

func (lsm *LSM) openMemTable(fid uint64) (*memTable, error) {
	fileOpt := &persistent.Options{
		Dir:      lsm.option.WorkDir,
		Flag:     os.O_CREATE | os.O_RDWR,
		MaxSize:  int(lsm.option.MemTableSize),
		FID:      fid,
		FileName: mtFilePath(lsm.option.WorkDir, fid),
	}
	s := inmemory.NewSkipList(int64(1 << 20))
	mt := &memTable{
		sl:  s,
		buf: &bytes.Buffer{},
		lsm: lsm,
	}
	mt.wal = persistent.OpenWalFile(fileOpt)
	err := mt.UpdateSkipList()
	utils.CondPanic(err != nil, errors.WithMessage(err, "while updating skiplist"))
	return mt, nil
}

func (m *memTable) set(entry *utils.Entry) error {
	// Write wal logs to prevent crashes
	if err := m.wal.Write(entry); err != nil {
		return err
	}
	// Write to memtable
	m.sl.Set(entry)
	return nil
}

func (m *memTable) Get(key []byte) (*utils.Entry, error) {
	vs := m.sl.Search(key)
	e := &utils.Entry{
		Key:       key,
		Value:     vs.Value,
		ExpiresAt: vs.ExpiresAt,
		Meta:      vs.Meta,
		Version:   vs.Version,
	}
	return e, nil
}

func (m *memTable) close() error {
	if err := m.wal.Close(); err != nil {
		return err
	}

	return nil
}

func (lsm *LSM) recovery() (*memTable, []*memTable) {
	// Get all files from the working directory
	files, err := ioutil.ReadDir(lsm.option.WorkDir)
	if err != nil {
		utils.Panic(err)
		return nil, nil
	}
	var fids []uint64
	maxFid := lsm.levels.maxFID
	// Identify files with the suffix .wal
	for _, file := range files {
		if !strings.HasSuffix(file.Name(), persistent.WalFileExt) {
			continue
		}
		fsz := len(file.Name())
		fid, err := strconv.ParseUint(file.Name()[:fsz-len(persistent.WalFileExt)], 10, 64)
		// Update maxFid considering the presence of .wal files
		if maxFid < fid {
			maxFid = fid
		}
		if err != nil {
			utils.Panic(err)
			return nil, nil
		}
		fids = append(fids, fid)
	}
	// Sort the fid
	sort.Slice(fids, func(i, j int) bool {
		return fids[i] < fids[j]
	})
	imms := []*memTable{}
	// Iterate over fid and decode into memTable
	for _, fid := range fids {
		mt, err := lsm.openMemTable(fid)
		utils.CondPanic(err != nil, err)
		if mt.sl.MemSize() == 0 {
			// mt.DecrRef()
			continue
		}
		imms = append(imms, mt)
	}
	// 更新最终的maxfid，初始化一定是串行执行的，因此不需要原子操作
	lsm.levels.maxFID = maxFid
	return lsm.NewMemTable(), imms
}

func (m *memTable) UpdateSkipList() error {
	if m.wal == nil || m.sl == nil {
		return nil
	}
	endOff, err := m.wal.Iterate(true, 0, m.replayFunction(m.lsm.option))
	if err != nil {
		return errors.WithMessage(err, fmt.Sprintf("while iterating wal: %s", m.wal.Name()))
	}
	// To prevent waste of space, wal is truncated
	return m.wal.Truncate(int64(endOff))
}

func (m *memTable) replayFunction(opt *Options) func(*utils.Entry, *utils.ValuePtr) error {
	return func(e *utils.Entry, _ *utils.ValuePtr) error { // Function for replaying.
		if ts := inmemory.ParseTs(e.Key); ts > m.maxVersion {
			m.maxVersion = ts
		}
		m.sl.Set(e)
		return nil
	}
}

func mtFilePath(dir string, fid uint64) string {
	return filepath.Join(dir, fmt.Sprintf("%05d%s", fid, persistent.WalFileExt))
}
