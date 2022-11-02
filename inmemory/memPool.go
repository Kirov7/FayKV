package inmemory

import (
	"log"
	"sync/atomic"
	"unsafe"
)

const (
	offsetSize = int(unsafe.Sizeof(uint32(0)))

	// Always align nodes on 64-bit boundaries, even on 32-bit architectures,
	// so that the node.value field is 64-bit aligned. This is necessary because
	// node.getValueOffset uses atomic.LoadUint64, which expects its input
	// pointer to be 64-bit aligned.
	nodeAlign = int(unsafe.Sizeof(uint64(0))) - 1

	MaxNodeSize = int(unsafe.Sizeof(node{}))
)

type MemPool struct {
	n   uint32
	buf []byte
}

func NewMemPool(sz int64) *MemPool {
	return &MemPool{
		n:   1,
		buf: make([]byte, sz),
	}
}

func (a *MemPool) allocate(sz uint32) uint32 {
	offset := atomic.AddUint32(&a.n, sz)
	if int(offset) > len(a.buf)-MaxNodeSize {
		growBy := uint32(len(a.buf))
		if growBy > 1<<30 {
			growBy = 1 << 30
		}
		if growBy < sz {
			growBy = sz
		}
		newBuf := make([]byte, len(a.buf)+int(growBy))
		if len(a.buf) != copy(newBuf, a.buf) {
			log.Fatal("Error while copying")
		}
		a.buf = newBuf
	}
	return offset - sz
}

func (a *MemPool) size() int64 {
	return int64(atomic.LoadUint32(&a.n))
}

func (a *MemPool) putNode(height int) uint32 {
	// Compute the amount of the tower that will never be used, since the height
	// is less than maxHeight.
	unusedSize := (maxHeight - height) * offsetSize

	// Pad the allocation with enough bytes to ensure pointer alignment.
	l := uint32(MaxNodeSize - unusedSize + nodeAlign)
	n := a.allocate(l)

	// Return the aligned offset.
	m := (n + uint32(nodeAlign)) & ^uint32(nodeAlign)
	return m
}

func (a *MemPool) putVal(v ValueStruct) uint32 {
	l := v.EncodedSize()
	offset := a.allocate(l)
	v.EncodeValue(a.buf[offset:])
	return offset
}

func (a *MemPool) putKey(key []byte) uint32 {
	keySz := uint32(len(key))
	offset := a.allocate(keySz)
	buf := a.buf[offset : offset+keySz]
	if len(key) != copy(buf, key) {
		log.Fatal("Error while copying")
	}
	return offset
}

func (a *MemPool) getNode(offset uint32) *node {
	if offset == 0 {
		return nil
	}
	return (*node)(unsafe.Pointer(&a.buf[offset]))
}

func (a *MemPool) getKey(offset uint32, size uint16) []byte {
	return a.buf[offset : offset+uint32(size)]
}

func (a *MemPool) getVal(offset uint32, size uint32) (ret ValueStruct) {
	ret.DecodeValue(a.buf[offset : offset+size])
	return
}

// getNodeOffset returns the offset of node in the memPool. If the node pointer is
// nil, then the zero offset is returned.
func (a *MemPool) getNodeOffset(nd *node) uint32 {
	if nd == nil {
		return 0 //返回空指针
	}
	//implement me here！！！
	//获取某个节点,在 memPool 当中的偏移量
	//unsafe.Pointer等价于void*,uintptr可以专门把void*的对于地址转化为数值型变量
	return uint32(uintptr(unsafe.Pointer(nd)) - uintptr(unsafe.Pointer(&a.buf[0])))
}
