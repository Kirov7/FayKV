package skipList

type Entry struct {
	Key   []byte
	Value []byte
}

func NewEntry(key, value []byte) *Entry {
	return &Entry{
		Key:   key,
		Value: value,
	}
}

func (e *Entry) Size() int64 {
	return int64(len(e.Key) + len(e.Value))
}
