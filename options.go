package FayKV

import "github.com/Kirov7/FayKV/utils"

type Options struct {
	ValueThreshold      int64
	WorkDir             string
	MemTableSize        int64
	SSTableMaxSz        int64
	MaxBatchCount       int64
	MaxBatchSize        int64 // max batch size in bytes
	ValueLogFileSize    int
	VerifyValueChecksum bool
	ValueLogMaxEntries  uint32
	LogRotatesToFlush   int32
	MaxTableSize        int64
}

type Stats struct {
	closer   *utils.Closer
	EntryNum int64 // Number of stored entries
}

// NewStats
func newStats(opt *Options) *Stats {
	s := &Stats{}
	s.closer = utils.NewCloser()
	s.EntryNum = 1
	return s
}

// Close
func (s *Stats) close() error {
	return nil
}
