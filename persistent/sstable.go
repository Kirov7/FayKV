package persistent

import (
	"fmt"
	"github.com/Kirov7/FayKV/pb"
	"github.com/Kirov7/FayKV/utils"
	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"
	"io"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"time"
)

type SSTable struct {
	m              *sync.RWMutex
	f              *MmapFile
	maxKey         []byte
	minKey         []byte
	idxTables      *pb.TableIndex
	hasBloomFilter bool
	idxLen         int
	idxStart       int
	fid            uint64
	createdAt      time.Time
}

func OpenSSTable(opt *Options) *SSTable {
	f, err := OpenMmapFile(opt.FileName, os.O_CREATE|os.O_RDWR, opt.MaxSize)
	utils.Panic(err)

	return &SSTable{
		f:   f,
		fid: opt.FID,
		m:   &sync.RWMutex{},
	}
}

func (ss *SSTable) Init() error {
	blockOffset, err := ss.initTable()
	if err != nil {
		return err
	}
	stat, _ := ss.f.Fd.Stat()
	statType := stat.Sys().(*syscall.Stat_t)
	ss.createdAt = time.Unix(statType.Ctim.Sec, statType.Ctim.Nsec)
	keyBytes := blockOffset.GetKey()
	// init min key
	minKey := make([]byte, len(keyBytes))
	copy(minKey, keyBytes)
	ss.minKey = minKey
	// todo init max key
	ss.maxKey = minKey
	return nil
}

func (ss *SSTable) initTable() (blockOffset *pb.BlockOffset, err error) {
	readPos := len(ss.f.Data)

	// read checksum len at the last 4 bytes
	readPos -= 4
	buf := ss.readCheckError(readPos, 4)
	checksumLen := int(utils.BytesToU32(buf))
	if checksumLen < 0 {
		return nil, errors.New("checksum length less than zero. Data corrupted")
	}

	// read checksum value
	readPos -= checksumLen
	expectedCks := ss.readCheckError(readPos, checksumLen)

	// read idx len at last
	readPos -= 4
	buf = ss.readCheckError(readPos, 4)
	ss.idxLen = int(utils.BytesToU32(buf))

	// read index
	readPos -= ss.idxLen
	ss.idxStart = readPos
	data := ss.readCheckError(readPos, ss.idxLen)
	if err := utils.VerifyChecksum(data, expectedCks); err != nil {
		return nil, errors.Wrapf(err, "failed to verify checksum for table: %s", ss.f.Fd.Name())
	}
	indexTable := &pb.TableIndex{}
	if err := proto.Unmarshal(data, indexTable); err != nil {
		return nil, err
	}
	ss.idxTables = indexTable

	ss.hasBloomFilter = len(indexTable.BloomFilter) > 0
	if len(indexTable.GetOffsets()) > 0 {
		return indexTable.GetOffsets()[0], nil
	}
	return nil, errors.New("read index fail, offset is nil")
}

// SetMaxKey max 需要使用table的迭代器，来获取最后一个block的最后一个key
func (ss *SSTable) SetMaxKey(maxKey []byte) {
	ss.maxKey = maxKey
}

func (ss *SSTable) read(off, sz int) ([]byte, error) {
	if len(ss.f.Data) > 0 {
		if len(ss.f.Data[off:]) < sz {
			return nil, io.EOF
		}
		return ss.f.Data[off : off+sz], nil
	}
	// make sure you can read the data, read data directly from the file
	res := make([]byte, sz)
	_, err := ss.f.Fd.ReadAt(res, int64(off))
	return res, err
}

// Detele _
func (ss *SSTable) Detele() error {
	return ss.f.Delete()
}

func (ss *SSTable) readCheckError(off, sz int) []byte {
	buf, err := ss.read(off, sz)
	utils.Panic(err)
	return buf
}

func (ss *SSTable) Indexs() *pb.TableIndex {
	return ss.idxTables
}

// FID get fid
func (ss *SSTable) FID() uint64 {
	return ss.fid
}

// Bytes returns data starting from offset off of size sz. If there's not enough data, it would
// return nil slice and io.EOF.
func (ss *SSTable) Bytes(off, sz int) ([]byte, error) {
	return ss.f.Bytes(off, sz)
}

func FileNameSSTable(dir string, id uint64) string {
	return filepath.Join(dir, fmt.Sprintf("%05d.sst", id))
}
