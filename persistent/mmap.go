package persistent

import (
	"fmt"
	mmap "github.com/Kirov7/FayKV/persistent/syscall"
	"github.com/pkg/errors"
	"os"
	"path/filepath"
)

// MmapFile mmap file contains buffer and the file descriptor
type MmapFile struct {
	Data []byte
	Fd   *os.File
}

func OpenMmapFile(filename string, flag int, maxSize int) (*MmapFile, error) {
	fd, err := os.OpenFile(filename, flag, 0666)
	if err != nil {
		return nil, errors.Wrapf(err, "open file error: %s", filename)
	}
	writable := true
	if flag == os.O_RDONLY {
		writable = false
	}
	return OpenMmapFileSys(fd, maxSize, writable)
}

func OpenMmapFileSys(fd *os.File, size int, writable bool) (*MmapFile, error) {
	filename := fd.Name()
	fi, err := fd.Stat()
	if err != nil {
		return nil, errors.Wrapf(err, "stat file error: %s", filename)
	}

	fileSize := fi.Size()
	if size > 0 && fileSize == 0 {
		err = fd.Truncate(int64(size))
		if err != nil {
			return nil, errors.Wrapf(err, "turncate error: %s", filename)
		}
		fileSize = int64(size)
	}
	buf, err := mmap.Mmap(fd, writable, fileSize)
	if err != nil {
		return nil, errors.Wrapf(err, "mmap mapping %s with size %d error", fd.Name(), fileSize)
	}

	if fileSize == 0 {
		dir, _ := filepath.Split(filename)
		go SyncDir(dir)
	}
	return &MmapFile{
		Data: buf,
		Fd:   fd,
	}, err
}

func (m *MmapFile) Delete() error {
	if m.Fd == nil {
		return nil
	}

	if err := mmap.Munmap(m.Data); err != nil {
		return fmt.Errorf("while munmap file: %s, error: %v\n", m.Fd.Name(), err)
	}
	m.Data = nil
	if err := m.Fd.Truncate(0); err != nil {
		return fmt.Errorf("while truncate file: %s, error: %v\n", m.Fd.Name(), err)
	}
	if err := m.Fd.Close(); err != nil {
		return fmt.Errorf("while close file: %s, error: %v\n", m.Fd.Name(), err)
	}
	return os.Remove(m.Fd.Name())
}

func (m *MmapFile) Close() error {
	if m.Fd == nil {
		return nil
	}
	if err := m.Sync(); err != nil {
		return errors.Wrapf(err, "while sync file: %s", m.Fd.Name())
	}
	if err := mmap.Munmap(m.Data); err != nil {
		return errors.Wrapf(err, "while munmap file: %s", m.Fd.Name())
	}
	return m.Fd.Close()
}

func (m *MmapFile) Sync() error {
	if m == nil {
		return nil
	}
	return mmap.Msync(m.Data)
}

func SyncDir(dir string) error {
	df, err := os.Open(dir)
	if err != nil {
		return errors.Wrapf(err, "opening error: %s", dir)
	}
	if err := df.Sync(); err != nil {
		return errors.Wrapf(err, "syncing error: %s", dir)
	}
	if err := df.Close(); err != nil {
		return errors.Wrapf(err, "closing error: %s", dir)
	}
	return nil
}
