package checkpoint

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const FormatVersion = 1

var (
	ErrInvalidCheckpoint = errors.New("invalid checkpoint")
	ErrEpochMismatch     = errors.New("epoch mismatch")
)

type Position struct {
	Seq        uint64 `json:"seq"`
	Filenum    uint32 `json:"filenum"`
	Offset     uint64 `json:"offset"`
	LogicID    uint64 `json:"logic_id"`
	ServerID   uint32 `json:"server_id"`
	TermID     uint32 `json:"term_id"`
	TsMS       uint64 `json:"ts_ms"`
	WalSegment uint32 `json:"wal_segment,omitempty"`
	WalOffset  uint64 `json:"wal_offset,omitempty"`
}

type WalState struct {
	Segment uint32 `json:"segment"`
	Offset  uint64 `json:"offset"`
}

type SnapshotState struct {
	State           string `json:"state"`
	BgsaveEndFile   uint32 `json:"bgsave_end_file_num"`
	BgsaveEndOffset uint64 `json:"bgsave_end_offset"`
	ProgressFile    string `json:"progress_file"`
	ProgressOffset  uint64 `json:"progress_offset"`
	SnapshotSeq     uint64 `json:"snapshot_seq"`
}

type Checkpoint struct {
	Version   uint32        `json:"version"`
	SourceID  string        `json:"source_id"`
	Epoch     uint32        `json:"epoch"`
	AckCP     Position      `json:"ack_cp"`
	DurableCP Position      `json:"durable_cp"`
	Wal       WalState      `json:"wal"`
	Snapshot  SnapshotState `json:"snapshot"`
	Checksum  string        `json:"checksum,omitempty"`
}

type Manager struct {
	path     string
	sourceID string

	mu       sync.Mutex
	state    Checkpoint
	hasState bool

	nextAckSeq uint64
	pending    map[uint64]Position

	nextSnapshotSeq uint64
	snapshotPending map[uint64]struct{}

	ackDirty bool

	now func() time.Time
}

func NewManager(path, sourceID string) *Manager {
	m := &Manager{
		path:     path,
		sourceID: sourceID,
		now:      time.Now,
	}
	m.state.Version = FormatVersion
	m.state.SourceID = sourceID
	return m
}

func (m *Manager) SourceID() string { return m.sourceID }

func (m *Manager) BeginEpoch(epoch uint32) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.state.Epoch = epoch
	m.state.AckCP.Seq = 0
	m.state.DurableCP.Seq = 0
	m.nextAckSeq = 1
	m.pending = make(map[uint64]Position)
	m.ackDirty = false
	m.hasState = true
	return m.persistLocked()
}

func (m *Manager) BeginSnapshot(state SnapshotState) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	state.SnapshotSeq = 0
	m.state.Snapshot = state
	m.nextSnapshotSeq = 1
	m.snapshotPending = make(map[uint64]struct{})
	m.hasState = true
	return m.persistLocked()
}

func (m *Manager) SetSnapshotState(state SnapshotState) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.state.Snapshot = state
	if m.nextSnapshotSeq == 0 {
		if state.SnapshotSeq > 0 {
			m.nextSnapshotSeq = state.SnapshotSeq + 1
		} else {
			m.nextSnapshotSeq = 1
		}
	}
	m.hasState = true
	return m.persistLocked()
}

func (m *Manager) Load() (Checkpoint, bool, error) {
	if cp, ok, err := m.loadFromFile(); err != nil {
		return Checkpoint{}, false, err
	} else if ok {
		m.setState(cp)
		return cp, true, nil
	}
	return Checkpoint{}, false, nil
}

func (m *Manager) setState(cp Checkpoint) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.state = cp
	m.hasState = true
	m.ackDirty = false
	if m.nextAckSeq == 0 {
		if cp.AckCP.Seq > 0 {
			m.nextAckSeq = cp.AckCP.Seq + 1
		} else {
			m.nextAckSeq = 1
		}
	}
	if m.pending == nil {
		m.pending = make(map[uint64]Position)
	}
	if m.nextSnapshotSeq == 0 {
		if cp.Snapshot.SnapshotSeq > 0 {
			m.nextSnapshotSeq = cp.Snapshot.SnapshotSeq + 1
		} else {
			m.nextSnapshotSeq = 1
		}
	}
	if m.snapshotPending == nil {
		m.snapshotPending = make(map[uint64]struct{})
	}
}

func (m *Manager) State() (Checkpoint, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.hasState {
		return Checkpoint{}, false
	}
	return m.state, true
}

func (m *Manager) Ack() (Position, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.hasState {
		return Position{}, false
	}
	return m.state.AckCP, true
}

func (m *Manager) SetAckPosition(pos Position) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.state.AckCP = pos
	if pos.Seq > 0 {
		m.nextAckSeq = pos.Seq + 1
	} else if m.nextAckSeq == 0 {
		m.nextAckSeq = 1
	}
	m.ackDirty = false
	m.hasState = true
	return m.persistLocked()
}

func (m *Manager) OnSnapshotDone(seq uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if seq == 0 {
		return fmt.Errorf("%w: snapshot seq required", ErrInvalidCheckpoint)
	}
	if m.snapshotPending == nil {
		m.snapshotPending = make(map[uint64]struct{})
	}
	if m.nextSnapshotSeq == 0 {
		m.nextSnapshotSeq = 1
	}
	if seq < m.nextSnapshotSeq {
		return nil
	}
	m.snapshotPending[seq] = struct{}{}

	advanced := false
	for {
		if _, ok := m.snapshotPending[m.nextSnapshotSeq]; !ok {
			break
		}
		delete(m.snapshotPending, m.nextSnapshotSeq)
		m.state.Snapshot.SnapshotSeq = m.nextSnapshotSeq
		m.nextSnapshotSeq++
		advanced = true
	}
	m.hasState = true
	if !advanced {
		return nil
	}
	return m.persistLocked()
}

func (m *Manager) OnDone(epoch uint32, pos Position) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	advanced, err := m.onDoneLocked(epoch, pos)
	if err != nil {
		return err
	}
	m.hasState = true
	if !advanced {
		return nil
	}
	return m.persistLocked()
}

func (m *Manager) OnDoneDeferred(epoch uint32, pos Position) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	advanced, err := m.onDoneLocked(epoch, pos)
	if err != nil {
		return err
	}
	m.hasState = true
	if !advanced {
		return nil
	}
	m.ackDirty = true
	return nil
}

func (m *Manager) FlushAck() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if !m.ackDirty {
		return nil
	}
	return m.persistLocked()
}

func (m *Manager) onDoneLocked(epoch uint32, pos Position) (bool, error) {
	if m.state.Epoch != 0 && epoch != m.state.Epoch {
		return false, ErrEpochMismatch
	}
	if pos.Seq == 0 {
		return false, fmt.Errorf("%w: seq required", ErrInvalidCheckpoint)
	}
	if m.pending == nil {
		m.pending = make(map[uint64]Position)
	}
	if pos.Seq < m.nextAckSeq {
		return false, nil
	}
	m.pending[pos.Seq] = pos

	advanced := false
	for {
		p, ok := m.pending[m.nextAckSeq]
		if !ok {
			break
		}
		delete(m.pending, m.nextAckSeq)
		m.state.AckCP = p
		m.nextAckSeq++
		advanced = true
	}
	return advanced, nil
}

func (m *Manager) OnDurable(epoch uint32, pos Position, wal WalState) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.state.Epoch != 0 && epoch != m.state.Epoch {
		return ErrEpochMismatch
	}
	if pos.Seq == 0 {
		return fmt.Errorf("%w: seq required", ErrInvalidCheckpoint)
	}
	if pos.Seq <= m.state.DurableCP.Seq {
		return nil
	}
	pos.WalSegment = wal.Segment
	pos.WalOffset = wal.Offset
	m.state.DurableCP = pos
	m.state.Wal = wal
	m.hasState = true
	return m.persistLocked()
}

func (m *Manager) loadFromFile() (Checkpoint, bool, error) {
	if m.path == "" {
		return Checkpoint{}, false, nil
	}
	data, err := os.ReadFile(m.path)
	if err != nil {
		if os.IsNotExist(err) {
			return Checkpoint{}, false, nil
		}
		return Checkpoint{}, false, err
	}
	cp, err := Decode(data)
	if err != nil {
		return Checkpoint{}, false, err
	}
	if cp.SourceID != m.sourceID {
		return Checkpoint{}, false, fmt.Errorf("%w: source_id mismatch", ErrInvalidCheckpoint)
	}
	return cp, true, nil
}

func (m *Manager) persistLocked() error {
	if m.path == "" {
		return nil
	}
	m.state.Version = FormatVersion
	m.state.SourceID = m.sourceID

	data, err := Encode(m.state)
	if err != nil {
		return err
	}
	dir := filepath.Dir(m.path)
	if dir != "" && dir != "." {
		_ = os.MkdirAll(dir, 0o755)
	}
	tmp := m.path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	if err := os.Rename(tmp, m.path); err != nil {
		return err
	}
	m.ackDirty = false
	return nil
}

func Encode(cp Checkpoint) ([]byte, error) {
	if cp.Version == 0 {
		cp.Version = FormatVersion
	}
	return json.Marshal(cp)
}

func Decode(data []byte) (Checkpoint, error) {
	var cp Checkpoint
	if err := json.Unmarshal(data, &cp); err != nil {
		return Checkpoint{}, fmt.Errorf("%w: unmarshal: %v", ErrInvalidCheckpoint, err)
	}
	if cp.Version != FormatVersion {
		return Checkpoint{}, fmt.Errorf("%w: version=%d", ErrInvalidCheckpoint, cp.Version)
	}
	if cp.SourceID == "" {
		return Checkpoint{}, fmt.Errorf("%w: source_id missing", ErrInvalidCheckpoint)
	}
	return cp, nil
}
