package utils

import (
	"hash/crc32"
	"os"
)

// file
const (
	ManifestFilename                  = "MANIFEST"
	ManifestRewriteFilename           = "REWRITEMANIFEST"
	ManifestDeletionsRewriteThreshold = 10000
	ManifestDeletionsRatio            = 10
	DefaultFileFlag                   = os.O_RDWR | os.O_CREATE | os.O_APPEND
	DefaultFileMode                   = 0666
)

// codec
var (
	MagicText    = [4]byte{'F', 'A', 'Y', 'A'}
	MagicVersion = uint32(1)
	// CastagnoliCrcTable is a CRC32 polynomial table (You can think of it as a salt)
	CastagnoliCrcTable = crc32.MakeTable(crc32.Castagnoli)
)
