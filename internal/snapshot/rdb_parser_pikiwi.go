package snapshot

/*
#cgo CXXFLAGS: -std=c++17 -O2 -DNDEBUG -I/tmp/pikiwidb/src -I/tmp/pikiwidb/src/storage -I/tmp/pikiwidb/src/storage/include -I/tmp/pikiwidb/src/pstd/include -I/tmp/pikiwidb/deps/include
#cgo LDFLAGS: -L/tmp/pikiwidb/deps/lib -lrocksdb -lz -lzstd -lsnappy -lpthread -ldl -lglog -lgflags -lfmt -lprotobuf -lprotobuf-lite -lstdc++
#include "pikiwi_snapshot.h"
#include <stdlib.h>
*/
import "C"

import (
	"fmt"
	"io"
	"unsafe"
)

type cgoIterator struct {
	ptr      *C.SnapshotIter
	cgoBatch int
}

func (p *RDBParser) Open(dumpPath, dbName string, opts ParserOptions) (Iterator, error) {
	if p == nil {
		return nil, fmt.Errorf("snapshot parser is nil")
	}
	cDump := C.CString(dumpPath)
	cDB := C.CString(dbName)
	defer C.free(unsafe.Pointer(cDump))
	defer C.free(unsafe.Pointer(cDB))

	opts = normalizeOptions(opts)
	var cPattern *C.char
	if opts.ScanPattern != "" {
		cPattern = C.CString(opts.ScanPattern)
		defer C.free(unsafe.Pointer(cPattern))
	}

	cOpts := C.SnapshotOptions{
		batch_num:     C.int(opts.BatchNum),
		scan_batch:    C.int64_t(opts.ScanBatch),
		scan_pattern:  cPattern,
		type_mask:     C.uint(opts.TypeMask),
		scan_strategy: C.int(opts.ScanStrategy),
		list_tail_n:   C.int64_t(opts.ListTailN),
	}

	var errMsg *C.char
	it := C.snapshot_open(cDump, cDB, cOpts, &errMsg)
	if it == nil {
		return nil, fmt.Errorf("snapshot_open failed: %s", cGoString(errMsg))
	}
	return &cgoIterator{ptr: it, cgoBatch: opts.CgoBatch}, nil
}

func (it *cgoIterator) Next() (Record, error) {
	records, err := it.NextBatch(1)
	if err != nil {
		return Record{}, err
	}
	if len(records) == 0 {
		return Record{}, io.EOF
	}
	return records[0], nil
}

func (it *cgoIterator) NextBatch(max int) ([]Record, error) {
	if it == nil || it.ptr == nil {
		return nil, io.EOF
	}
	batch := it.cgoBatch
	if max > 0 {
		batch = max
	}
	if batch <= 0 {
		batch = 1
	}
	var recs *C.SnapshotRecord
	var count C.size_t
	var errMsg *C.char
	ret := C.snapshot_next_batch(it.ptr, &recs, &count, C.int(batch), &errMsg)
	if ret == 0 {
		return nil, io.EOF
	}
	if ret < 0 {
		return nil, fmt.Errorf("snapshot_next_batch failed: %s", cGoString(errMsg))
	}
	defer C.snapshot_free_batch(recs, count)

	cSlice := unsafe.Slice(recs, int(count))
	out := make([]Record, 0, len(cSlice))
	for _, rec := range cSlice {
		dataType := cGoBytes(rec.data_type, rec.data_type_len)
		key := cGoBytes(rec.key, rec.key_len)
		raw := cGoBytes(rec.raw_resp, rec.raw_resp_len)
		out = append(out, Record{DataType: string(dataType), Key: key, RawRESP: raw})
	}
	return out, nil
}

func (it *cgoIterator) Close() error {
	if it == nil || it.ptr == nil {
		return nil
	}
	C.snapshot_close(it.ptr)
	it.ptr = nil
	return nil
}

func cGoBytes(ptr *C.char, n C.size_t) []byte {
	if ptr == nil || n == 0 {
		return nil
	}
	return C.GoBytes(unsafe.Pointer(ptr), C.int(n))
}

func cGoString(ptr *C.char) string {
	if ptr == nil {
		return ""
	}
	defer C.free(unsafe.Pointer(ptr))
	return C.GoString(ptr)
}
