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
	Dir            string
	SegmentBytes   int64
	SyncEvery      int
	RetainSegments int
	AckPosProvider func() (uint32, uint64)
}

type State struct {
	Segment uint32
	Offset  uint64
}

type Writer struct {
	mu                  sync.Mutex
	cfg                 Config
	file                *os.File
	path                string
	segment             uint32
	offset              uint64
	segmentStartFilenum uint32
	segmentStartOffset  uint64
	synced              int
	crc                 *crc32.Table

	gcMu      sync.Mutex
	gcRunning bool
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
	if cfg.RetainSegments <= 0 {
		cfg.RetainSegments = 2
	}
	if err := os.MkdirAll(cfg.Dir, 0o755); err != nil {
		return nil, err
	}

	segment, offset, startFilenum, startOffset, path, f, err := openLatestSegment(cfg.Dir, cfg.SegmentBytes)
	if err != nil {
		return nil, err
	}
	return &Writer{
		cfg:                 cfg,
		file:                f,
		path:                path,
		segment:             segment,
		offset:              offset,
		segmentStartFilenum: startFilenum,
		segmentStartOffset:  startOffset,
		crc:                 crc32.MakeTable(crc32.Castagnoli),
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
	if w.offset == 0 && w.segmentStartFilenum == 0 && w.segmentStartOffset == 0 {
		newPath := filepath.Join(w.cfg.Dir, segmentName(w.segment, filenum, offset))
		if w.path != "" && w.path != newPath {
			if err := os.Rename(w.path, newPath); err != nil {
				return State{}, err
			}
			w.path = newPath
		}
		w.segmentStartFilenum = filenum
		w.segmentStartOffset = offset
	}
	recordLen := uint64(HeaderLen) + uint64(len(payload))
	if w.cfg.SegmentBytes > 0 && w.offset+recordLen > uint64(w.cfg.SegmentBytes) {
		if err := w.rotateLocked(filenum, offset); err != nil {
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

func (w *Writer) rotateLocked(startFilenum uint32, startOffset uint64) error {
	if w.file != nil {
		_ = w.file.Sync()
		_ = w.file.Close()
	}
	w.segment++
	path := filepath.Join(w.cfg.Dir, segmentName(w.segment, startFilenum, startOffset))
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	w.file = f
	w.path = path
	w.offset = 0
	w.synced = 0
	w.segmentStartFilenum = startFilenum
	w.segmentStartOffset = startOffset
	w.triggerGC()
	return nil
}

type walFile struct {
	segment      uint32
	startFilenum uint32
	startOffset  uint64
	path         string
}

func openLatestSegment(dir string, segmentBytes int64) (uint32, uint64, uint32, uint64, string, *os.File, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return 0, 0, 0, 0, "", nil, err
	}
	var files []walFile
	for _, ent := range entries {
		if ent.IsDir() {
			continue
		}
		name := ent.Name()
		seg, startFilenum, startOffset, ok := parseSegmentName(name)
		if !ok {
			continue
		}
		files = append(files, walFile{
			segment:      seg,
			startFilenum: startFilenum,
			startOffset:  startOffset,
			path:         filepath.Join(dir, name),
		})
	}
	if len(files) == 0 {
		path := filepath.Join(dir, segmentName(1, 0, 0))
		f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
		if err != nil {
			return 0, 0, 0, 0, "", nil, err
		}
		return 1, 0, 0, 0, path, f, nil
	}
	sort.Slice(files, func(i, j int) bool { return files[i].segment < files[j].segment })
	last := files[len(files)-1]
	path := last.path
	info, err := os.Stat(path)
	if err != nil {
		return 0, 0, 0, 0, "", nil, err
	}
	size := info.Size()
	if segmentBytes > 0 && size >= segmentBytes {
		path = filepath.Join(dir, segmentName(last.segment+1, 0, 0))
		f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
		if err != nil {
			return 0, 0, 0, 0, "", nil, err
		}
		return last.segment + 1, 0, 0, 0, path, f, nil
	}
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return 0, 0, 0, 0, "", nil, err
	}
	return last.segment, uint64(size), last.startFilenum, last.startOffset, path, f, nil
}

func segmentName(segment uint32, startFilenum uint32, startOffset uint64) string {
	return fmt.Sprintf("wal-%08d-%020d-%08d.log", startFilenum, startOffset, segment)
}

func parseSegmentName(name string) (uint32, uint32, uint64, bool) {
	if !strings.HasPrefix(name, "wal-") || !strings.HasSuffix(name, ".log") {
		return 0, 0, 0, false
	}
	var seg uint32
	var startFilenum uint32
	var startOffset uint64
	if _, err := fmt.Sscanf(name, "wal-%08d-%020d-%08d.log", &startFilenum, &startOffset, &seg); err == nil {
		return seg, startFilenum, startOffset, true
	}
	return 0, 0, 0, false
}

func (w *Writer) triggerGC() {
	if w.cfg.AckPosProvider == nil {
		return
	}
	w.gcMu.Lock()
	if w.gcRunning {
		w.gcMu.Unlock()
		return
	}
	w.gcRunning = true
	w.gcMu.Unlock()

	go func() {
		defer func() {
			w.gcMu.Lock()
			w.gcRunning = false
			w.gcMu.Unlock()
		}()
		w.cleanupSegments()
	}()
}

func (w *Writer) cleanupSegments() {
	ackFilenum := uint32(0)
	ackOffset := uint64(0)
	if w.cfg.AckPosProvider != nil {
		ackFilenum, ackOffset = w.cfg.AckPosProvider()
	}
	if ackFilenum == 0 && ackOffset == 0 {
		return
	}
	retain := w.cfg.RetainSegments
	if retain <= 0 {
		retain = 2
	}

	entries, err := os.ReadDir(w.cfg.Dir)
	if err != nil {
		return
	}
	files := make([]walFile, 0, len(entries))
	for _, ent := range entries {
		if ent.IsDir() {
			continue
		}
		seg, startFilenum, startOffset, ok := parseSegmentName(ent.Name())
		if !ok {
			continue
		}
		files = append(files, walFile{
			segment:      seg,
			startFilenum: startFilenum,
			startOffset:  startOffset,
			path:         filepath.Join(w.cfg.Dir, ent.Name()),
		})
	}
	if len(files) <= retain {
		return
	}
	sort.Slice(files, func(i, j int) bool {
		if files[i].startFilenum != files[j].startFilenum {
			return files[i].startFilenum < files[j].startFilenum
		}
		if files[i].startOffset != files[j].startOffset {
			return files[i].startOffset < files[j].startOffset
		}
		return files[i].segment < files[j].segment
	})
	limit := len(files) - retain
	for i := 0; i < limit; i++ {
		cur := files[i]
		next := files[i+1]
		if cur.startFilenum == 0 && cur.startOffset == 0 {
			continue
		}
		if next.startFilenum == 0 && next.startOffset == 0 {
			continue
		}
		if newerOrEqualPos(ackFilenum, ackOffset, next.startFilenum, next.startOffset) {
			_ = os.Remove(cur.path)
		}
	}
}

func newerOrEqualPos(aFile uint32, aOff uint64, bFile uint32, bOff uint64) bool {
	if aFile > bFile {
		return true
	}
	if aFile < bFile {
		return false
	}
	return aOff >= bOff
}
