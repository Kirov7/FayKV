package persistent

import (
	"fmt"
	mmap "github.com/Kirov7/FayKV/persistent/syscall"
	"github.com/pkg/errors"
	"io"
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
		// if file is empty then truncate it
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

// AppendBuffer Append a buffer to the memory and remap it to expand it if space runs out
func (m *MmapFile) AppendBuffer(offset uint32, buf []byte) error {
	const oneGB = 1 << 30
	size := len(m.Data)
	needSize := len(buf)
	end := int(offset) + needSize
	if end > size {
		growBy := size
		if growBy > oneGB {
			growBy = oneGB
		}
		if growBy < needSize {
			growBy = needSize
		}
		if err := m.Truncature(int64(end)); err != nil {
			return err
		}
	}
	dLen := copy(m.Data[offset:end], buf)
	if dLen != needSize {
		return errors.Errorf("dLen != needSize AppendBuffer failed")
	}
	return nil
}

// Bytes returns data starting from offset off of size sz. If there's not enough data, it would
// return nil slice and io.EOF.
func (m *MmapFile) Bytes(off, sz int) ([]byte, error) {
	if len(m.Data[off:]) < sz {
		return nil, io.EOF
	}
	return m.Data[off : off+sz], nil
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

// Truncature _
func (m *MmapFile) Truncature(maxSz int64) error {
	if err := m.Sync(); err != nil {
		return fmt.Errorf("while sync file: %s, error: %v\n", m.Fd.Name(), err)
	}
	if err := m.Fd.Truncate(maxSz); err != nil {
		return fmt.Errorf("while truncate file: %s, error: %v\n", m.Fd.Name(), err)
	}

	var err error
	m.Data, err = mmap.Mremap(m.Data, int(maxSz)) // Mmap up to max size.
	return err
}

type mmapReader struct {
	Data   []byte
	offset int
}

func (mr *mmapReader) Read(buf []byte) (int, error) {
	if mr.offset > len(mr.Data) {
		return 0, io.EOF
	}
	n := copy(buf, mr.Data[mr.offset:])
	mr.offset += n
	if n < len(buf) {
		return n, io.EOF
	}
	return n, nil
}

func (m *MmapFile) NewReader(offset int) io.Reader {
	return &mmapReader{
		Data:   m.Data,
		offset: offset,
	}
}
