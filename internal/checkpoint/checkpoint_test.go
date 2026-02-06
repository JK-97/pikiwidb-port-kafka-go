package checkpoint

import (
	"os"
	"path/filepath"
	"testing"
)

func TestJSONRoundtrip(t *testing.T) {
	cp := Checkpoint{
		Version:  1,
		SourceID: "10.0.0.1:9221",
		Epoch:    7,
		AckCP: Position{
			Seq:      12,
			Filenum:  3,
			Offset:   456,
			LogicID:  8899,
			ServerID: 0,
			TermID:   5,
			TsMS:     1700000000000,
		},
		DurableCP: Position{
			Seq:        13,
			Filenum:    3,
			Offset:     789,
			WalSegment: 2,
			WalOffset:  4096,
		},
		Wal: WalState{Segment: 2, Offset: 4096},
		Snapshot: SnapshotState{
			State:           "done",
			BgsaveEndFile:   10,
			BgsaveEndOffset: 999,
			ProgressFile:    "dump-000010.rdb",
			ProgressOffset:  1024,
			SnapshotSeq:     9000,
		},
	}
	data, err := Encode(cp)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	parsed, err := Decode(data)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if parsed.SourceID != cp.SourceID || parsed.Epoch != cp.Epoch {
		t.Fatalf("parsed mismatch: %+v vs %+v", parsed, cp)
	}
	if parsed.AckCP != cp.AckCP {
		t.Fatalf("ack_cp mismatch: %+v vs %+v", parsed.AckCP, cp.AckCP)
	}
	if parsed.DurableCP != cp.DurableCP {
		t.Fatalf("durable_cp mismatch: %+v vs %+v", parsed.DurableCP, cp.DurableCP)
	}
}

func TestDecode_RequiresSourceIDAndVersion(t *testing.T) {
	_, err := Decode([]byte(`{"version":1}`))
	if err == nil {
		t.Fatalf("expected error")
	}
	_, err = Decode([]byte(`{"version":2,"source_id":"s"}`))
	if err == nil {
		t.Fatalf("expected error")
	}
}

func TestManager_OnDone_Ordered(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "checkpoint.json")
	m := NewManager(path, "s")
	if err := m.BeginEpoch(1); err != nil {
		t.Fatalf("BeginEpoch: %v", err)
	}

	p1 := Position{Seq: 1, Filenum: 1, Offset: 10}
	p2 := Position{Seq: 2, Filenum: 1, Offset: 20}
	p3 := Position{Seq: 3, Filenum: 1, Offset: 30}

	if err := m.OnDone(1, p1); err != nil {
		t.Fatalf("OnDone p1: %v", err)
	}
	if err := m.OnDone(1, p3); err != nil {
		t.Fatalf("OnDone p3: %v", err)
	}
	if err := m.OnDone(1, p2); err != nil {
		t.Fatalf("OnDone p2: %v", err)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	cp, err := Decode(data)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if cp.AckCP.Seq != 3 {
		t.Fatalf("ack seq=%d", cp.AckCP.Seq)
	}
	if cp.AckCP.Offset != 30 {
		t.Fatalf("ack offset=%d", cp.AckCP.Offset)
	}
}

func TestManager_OnDoneDeferred_Flush(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "checkpoint.json")
	m := NewManager(path, "s")
	if err := m.BeginEpoch(1); err != nil {
		t.Fatalf("BeginEpoch: %v", err)
	}

	p1 := Position{Seq: 1, Filenum: 1, Offset: 10}
	if err := m.OnDoneDeferred(1, p1); err != nil {
		t.Fatalf("OnDoneDeferred: %v", err)
	}
	if ack, ok := m.Ack(); !ok || ack.Seq != 1 {
		t.Fatalf("ack in memory=%+v ok=%v", ack, ok)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	cp, err := Decode(data)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if cp.AckCP.Seq != 0 {
		t.Fatalf("ack seq should remain 0 before flush, got %d", cp.AckCP.Seq)
	}

	if err := m.FlushAck(); err != nil {
		t.Fatalf("FlushAck: %v", err)
	}
	data, err = os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile after flush: %v", err)
	}
	cp, err = Decode(data)
	if err != nil {
		t.Fatalf("Decode after flush: %v", err)
	}
	if cp.AckCP.Seq != 1 {
		t.Fatalf("ack seq after flush=%d", cp.AckCP.Seq)
	}
}
