package wal

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
)

const (
	Magic              = 0x4c415750 // "PWAL" little-endian
	HeaderLen          = 56
	Version            = 1
	RecordTypeBinlog   = 1
	RecordTypeSnapshot = 2
)

type Config struct {
	Dir          string
	SegmentBytes int64
	SyncEvery    int
}

type State struct {
	Segment uint32
	Offset  uint64
}

type Writer struct {
	mu      sync.Mutex
	cfg     Config
	file    *os.File
	segment uint32
	offset  uint64
	synced  int
	crc     *crc32.Table
}

func OpenWriter(cfg Config) (*Writer, error) {
	if cfg.Dir == "" {
		cfg.Dir = "./wal/"
	}
	if cfg.SegmentBytes <= 0 {
		cfg.SegmentBytes = 256 * 1024 * 1024
	}
	if cfg.SyncEvery <= 0 {
		cfg.SyncEvery = 1
	}
	if err := os.MkdirAll(cfg.Dir, 0o755); err != nil {
		return nil, err
	}

	segment, offset, f, err := openLatestSegment(cfg.Dir, cfg.SegmentBytes)
	if err != nil {
		return nil, err
	}
	return &Writer{
		cfg:     cfg,
		file:    f,
		segment: segment,
		offset:  offset,
		crc:     crc32.MakeTable(crc32.Castagnoli),
	}, nil
}

func (w *Writer) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.file == nil {
		return nil
	}
	err := w.file.Sync()
	_ = w.file.Close()
	w.file = nil
	return err
}

func (w *Writer) AppendBinlog(epoch uint32, seq uint64, filenum uint32, offset uint64, payload []byte) (State, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.file == nil {
		return State{}, fmt.Errorf("wal not open")
	}
	recordLen := uint64(HeaderLen) + uint64(len(payload))
	if w.cfg.SegmentBytes > 0 && w.offset+recordLen > uint64(w.cfg.SegmentBytes) {
		if err := w.rotateLocked(); err != nil {
			return State{}, err
		}
	}

	header := w.buildHeader(epoch, seq, filenum, offset, payload)
	if _, err := w.file.Write(header); err != nil {
		return State{}, err
	}
	if _, err := w.file.Write(payload); err != nil {
		return State{}, err
	}
	w.offset += recordLen
	w.synced++
	if w.synced >= w.cfg.SyncEvery {
		if err := w.file.Sync(); err != nil {
			return State{}, err
		}
		w.synced = 0
	}
	return State{Segment: w.segment, Offset: w.offset}, nil
}

func (w *Writer) buildHeader(epoch uint32, seq uint64, filenum uint32, offset uint64, payload []byte) []byte {
	buf := make([]byte, HeaderLen)
	binary.LittleEndian.PutUint32(buf[0:4], Magic)
	binary.LittleEndian.PutUint16(buf[4:6], HeaderLen)
	buf[6] = Version
	buf[7] = RecordTypeBinlog
	binary.LittleEndian.PutUint16(buf[8:10], 0)
	binary.LittleEndian.PutUint16(buf[10:12], 0)
	binary.LittleEndian.PutUint32(buf[12:16], epoch)
	binary.LittleEndian.PutUint64(buf[16:24], seq)
	binary.LittleEndian.PutUint32(buf[24:28], filenum)
	binary.LittleEndian.PutUint64(buf[28:36], offset)
	binary.LittleEndian.PutUint32(buf[36:40], uint32(len(payload)))
	payloadCRC := crc32.Checksum(payload, w.crc)
	binary.LittleEndian.PutUint32(buf[40:44], payloadCRC)
	// header_crc32c at [44:48], fill after compute.
	binary.LittleEndian.PutUint32(buf[44:48], 0)
	binary.LittleEndian.PutUint32(buf[48:52], 0)
	binary.LittleEndian.PutUint32(buf[52:56], 0)
	headerCRC := crc32.Checksum(buf, w.crc)
	binary.LittleEndian.PutUint32(buf[44:48], headerCRC)
	return buf
}

func (w *Writer) rotateLocked() error {
	if w.file != nil {
		_ = w.file.Sync()
		_ = w.file.Close()
	}
	w.segment++
	path := filepath.Join(w.cfg.Dir, segmentName(w.segment))
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	w.file = f
	w.offset = 0
	w.synced = 0
	return nil
}

func openLatestSegment(dir string, segmentBytes int64) (uint32, uint64, *os.File, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return 0, 0, nil, err
	}
	var segments []uint32
	for _, ent := range entries {
		if ent.IsDir() {
			continue
		}
		name := ent.Name()
		if !strings.HasPrefix(name, "wal-") || !strings.HasSuffix(name, ".log") {
			continue
		}
		var seg uint32
		_, err := fmt.Sscanf(name, "wal-%08d.log", &seg)
		if err != nil {
			continue
		}
		segments = append(segments, seg)
	}
	if len(segments) == 0 {
		path := filepath.Join(dir, segmentName(1))
		f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
		if err != nil {
			return 0, 0, nil, err
		}
		return 1, 0, f, nil
	}
	sort.Slice(segments, func(i, j int) bool { return segments[i] < segments[j] })
	seg := segments[len(segments)-1]
	path := filepath.Join(dir, segmentName(seg))
	info, err := os.Stat(path)
	if err != nil {
		return 0, 0, nil, err
	}
	size := info.Size()
	if segmentBytes > 0 && size >= segmentBytes {
		path = filepath.Join(dir, segmentName(seg+1))
		f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
		if err != nil {
			return 0, 0, nil, err
		}
		return seg + 1, 0, f, nil
	}
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return 0, 0, nil, err
	}
	return seg, uint64(size), f, nil
}

func segmentName(segment uint32) string {
	return fmt.Sprintf("wal-%08d.log", segment)
}
