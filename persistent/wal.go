package persistent

import (
	"bytes"
	"encoding/binary"
	"github.com/Kirov7/FayKV/inmemory"
	"github.com/Kirov7/FayKV/utils"
	"hash/crc32"
	"io"
	"log"
	"os"
	"sync"
)

const WalFileExt string = ".wal"

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

func OpenWalFile(opt *Options) *WalFile {
	fd, err := OpenMmapFile(opt.FileName, os.O_CREATE|os.O_RDWR, opt.MaxSize)
	if err != nil {
		log.Println(err)
	}
	wf := &WalFile{
		lock: &sync.RWMutex{},
		f:    fd,
		opts: opt,
		buf:  &bytes.Buffer{},
		size: uint32(len(fd.Data)),
	}
	return wf
}

func (wf WalFile) Write(entry *inmemory.Entry) error {
	panic("todo")
}

const maxHeaderSize int = 21

type WalHeader struct {
	KeyLen    uint32
	ValueLen  uint32
	Meta      byte
	ExpiresAt uint64
}

func (h *WalHeader) Encode(out []byte) int {
	index := 0
	index = binary.PutUvarint(out[index:], uint64(h.KeyLen))
	index += binary.PutUvarint(out[index:], uint64(h.ValueLen))
	index += binary.PutUvarint(out[index:], uint64(h.Meta))
	index += binary.PutUvarint(out[index:], h.ExpiresAt)
	return index
}

func WalCodec(buf *bytes.Buffer, e *inmemory.Entry) int {
	buf.Reset()
	h := WalHeader{
		KeyLen:    uint32(len(e.Key)),
		ValueLen:  uint32(len(e.Value)),
		ExpiresAt: e.ExpiresAt,
	}

	hash := crc32.New(utils.CastagnoliCrcTable)
	writer := io.MultiWriter(buf, hash)

	// encode header.
	var headerEnc [maxHeaderSize]byte
	sz := h.Encode(headerEnc[:])
	utils.PanicTwoParams(writer.Write(headerEnc[:sz]))
	utils.PanicTwoParams(writer.Write(e.Key))
	utils.PanicTwoParams(writer.Write(e.Value))
	// write crc32 hash.
	var crcBuf [crc32.Size]byte
	binary.BigEndian.PutUint32(crcBuf[:], hash.Sum32())
	utils.PanicTwoParams(buf.Write(crcBuf[:]))
	// return encoded length.
	return len(headerEnc[:sz]) + len(e.Key) + len(e.Value) + len(crcBuf)
}

func EstimateWalCodecSize(e *inmemory.Entry) int {
	return len(e.Key) + len(e.Value) + 8 /* ExpiresAt uint64 */ +
		crc32.Size + maxHeaderSize
}
