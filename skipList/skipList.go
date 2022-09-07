package skipList

import (
	"bytes"
	"github.com/Kirov7/FayKV/utils"
	"math/rand"
	"sync"
)

const (
	defaultMaxHeight = 64
)

type SkipList struct {
	header *Element

	rand *rand.Rand

	maxLevel int
	length   int
	lock     sync.RWMutex
	size     int64
}

func NewSkipList() *SkipList {
	header := &Element{
		levels: make([]*Element, defaultMaxHeight),
	}

	return &SkipList{
		header:   header,
		maxLevel: defaultMaxHeight - 1,
		rand:     utils.GetRand(),
	}
}

func (list *SkipList) Add(data *Entry) error {
	// 需要加锁
	list.lock.Lock()
	defer list.lock.Unlock()

	prevs := make([]*Element, list.maxLevel+1)

	key := data.Key
	keyScore := list.calcScore(key)
	header, maxLevel := list.header, list.maxLevel
	prev := header
	// 从最高层开始比较
	for i := maxLevel; i >= 0; i-- {
		for ne := prev.levels[i]; ne != nil; ne = prev.levels[i] {
			if comp := list.compare(keyScore, key, ne); comp <= 0 {
				if comp == 0 {
					// 如果kv对已存在,则直接更新数据
					ne.entry = data
					return nil
				} else {
					prev = ne
				}
			} else {
				// 如果同层下一个元素大于当前key的话则向下一层
				break
			}
		}
		prevs[i] = prev
	}
	// 找到插入的位置后,计算层数和key前8字节的摘要
	randLevel, keyScore := list.randLevel(), list.calcScore(key)
	e := newElement(keyScore, data, randLevel)

	for i := randLevel; i >= 0; i-- {
		ne := prevs[i].levels[i]
		prevs[i].levels[i] = e
		e.levels[i] = ne
	}
	return nil
}

func (list *SkipList) Search(key []byte) (e *Entry) {
	// 加读锁
	list.lock.RLock()
	defer list.lock.RUnlock()
	keyScore := list.calcScore(key)
	header, maxLevel := list.header, list.maxLevel
	prev := header
	for i := maxLevel; i >= 0; i-- {
		for ne := prev.levels[i]; ne != nil; ne = prev.levels[i] {
			if comp := list.compare(keyScore, key, ne); comp <= 0 {
				if comp == 0 {
					return ne.entry
				} else {
					prev = ne
				}
			} else {
				break
			}
		}
	}
	return nil
}

// Close 关闭skipList资源
func (list *SkipList) Close() error {
	return nil
}

// calcScore 计算 key 的摘要值
// 1byte 为 8bit, 一个uint64可以存储64bit,即可以存储8byte
// 将key的前8个字符存在uint64里面,就可以将逐位比较改为无符号整数比较,以优化比较速度
func (list *SkipList) calcScore(key []byte) (score float64) {
	var hash uint64
	l := len(key)

	if l > 8 {
		l = 8
	}

	// 将第i个byte存在到uint64中的搞 8 * i 位
	for i := 0; i < l; i++ {
		shift := uint(64 - 8 - i*8)
		hash |= uint64(key[i]) << shift
	}

	score = float64(hash)
	return
}

// compare 比较器函数
func (list *SkipList) compare(score float64, key []byte, next *Element) int {
	if score == next.score {
		return bytes.Compare(key, next.entry.Key)
	}

	if score < next.score {
		return -1
	} else {
		return 1
	}
}

// randLevel 添加新节点的时候,随机产生层数
func (list *SkipList) randLevel() int {
	// 2^(-i) 的概率返回 i
	for i := 0; i < list.maxLevel; i++ {
		if list.rand.Intn(2) == 0 {
			return i
		}
	}

	return list.maxLevel
}

func (list *SkipList) Size() int64 {
	return list.size
}
