package snapshot

import (
	"errors"
	"strings"
)

var ErrParserNotImplemented = errors.New("snapshot parser not implemented")

type ScanStrategy int

const (
	ScanStrategyCursor ScanStrategy = iota
	ScanStrategyStartKey
)

const (
	TypeStrings uint32 = 1 << 0
	TypeLists   uint32 = 1 << 1
	TypeHashes  uint32 = 1 << 2
	TypeSets    uint32 = 1 << 3
	TypeZSets   uint32 = 1 << 4
	TypeStreams uint32 = 1 << 5
	TypeAll     uint32 = 0xFFFFFFFF
)

type Record struct {
	DataType string
	Key      []byte
	RawRESP  []byte
}

type Iterator interface {
	Next() (Record, error)
	NextBatch(max int) ([]Record, error)
	Close() error
}

type Parser interface {
	Open(dumpPath, dbName string, opts ParserOptions) (Iterator, error)
}

type ParserOptions struct {
	BatchNum     int
	CgoBatch     int
	ScanBatch    int64
	ScanPattern  string
	ScanStrategy ScanStrategy
	TypeMask     uint32
	ListTailN    int64
}

type RDBParser struct{}

func NewRDBParser() *RDBParser {
	return &RDBParser{}
}

func normalizeOptions(opts ParserOptions) ParserOptions {
	if opts.BatchNum <= 0 {
		opts.BatchNum = 512
	}
	if opts.CgoBatch <= 0 {
		opts.CgoBatch = 1
	}
	if opts.ListTailN < 0 {
		opts.ListTailN = 0
	}
	if opts.TypeMask == 0 {
		opts.TypeMask = TypeAll
	}
	if opts.ScanStrategy != ScanStrategyStartKey {
		opts.ScanStrategy = ScanStrategyCursor
	}
	return opts
}

func ParseScanStrategy(value string) ScanStrategy {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "scanx", "startkey", "start_key":
		return ScanStrategyStartKey
	default:
		return ScanStrategyCursor
	}
}
