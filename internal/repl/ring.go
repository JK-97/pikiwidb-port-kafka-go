package repl

import "github.com/OpenAtomFoundation/pikiwidb-port-kafka-go/internal/kafka"

type ringEntry struct {
	seq       uint64
	ready     bool
	hasRecord bool
	record    kafka.Record
}

type ringWindow struct {
	slots []ringEntry
	head  int
	tail  int
	count int
	index map[uint64]int
}

func newRingWindow(size int) *ringWindow {
	if size <= 0 {
		size = 1
	}
	return &ringWindow{
		slots: make([]ringEntry, size),
		index: make(map[uint64]int, size),
	}
}

func (r *ringWindow) IsFull() bool {
	return r.count == len(r.slots)
}

func (r *ringWindow) Add(seq uint64) bool {
	if r.count == len(r.slots) {
		return false
	}
	r.slots[r.tail] = ringEntry{seq: seq}
	r.index[seq] = r.tail
	r.tail = (r.tail + 1) % len(r.slots)
	r.count++
	return true
}

func (r *ringWindow) MarkReady(seq uint64, record kafka.Record, hasRecord bool) bool {
	idx, ok := r.index[seq]
	if !ok {
		return false
	}
	entry := &r.slots[idx]
	entry.ready = true
	entry.hasRecord = hasRecord
	if hasRecord {
		entry.record = record
	} else {
		entry.record = kafka.Record{}
	}
	return true
}

func (r *ringWindow) Drain(maxRecords int) ([]kafka.Record, bool) {
	if r.count == 0 {
		return nil, false
	}
	if maxRecords <= 0 {
		maxRecords = 1
	}
	out := make([]kafka.Record, 0, maxRecords)
	advanced := false
	for r.count > 0 {
		entry := &r.slots[r.head]
		if !entry.ready {
			break
		}
		if entry.hasRecord {
			if len(out) >= maxRecords {
				break
			}
			out = append(out, entry.record)
		}
		delete(r.index, entry.seq)
		*entry = ringEntry{}
		r.head = (r.head + 1) % len(r.slots)
		r.count--
		advanced = true
	}
	return out, advanced
}

func (r *ringWindow) Count() int {
	return r.count
}

func (r *ringWindow) Size() int {
	return len(r.slots)
}
