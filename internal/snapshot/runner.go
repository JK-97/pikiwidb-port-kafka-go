package snapshot

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/OpenAtomFoundation/pikiwidb-port-kafka-go/internal/event"
	"github.com/OpenAtomFoundation/pikiwidb-port-kafka-go/internal/filter"
	"github.com/OpenAtomFoundation/pikiwidb-port-kafka-go/internal/kafka"
	"github.com/OpenAtomFoundation/pikiwidb-port-kafka-go/internal/resp"
)

type Runner struct {
	DBName   string
	SourceID string

	StreamMode    string
	TopicSnapshot string
	TopicSingle   string

	Filter          *filter.Filter
	Producer        *kafka.Producer
	Parser          Parser
	Builder         event.Builder
	Workers         int
	BatchNum        int
	CgoBatch        int
	ScanBatch       int64
	ScanStrategy    ScanStrategy
	ReaderMode      string
	ReaderTypes     []string
	ReaderPrefixes  []string
	WindowSlots     int
	SenderBatchSize int
	ListTailN       int64
	Ckpt            interface{ OnSnapshotDone(seq uint64) error }

	Log *log.Logger
}

func (r *Runner) Run(ctx context.Context, dumpPath string) error {
	if r == nil {
		return fmt.Errorf("snapshot runner is nil")
	}
	if dumpPath == "" {
		return fmt.Errorf("dump_path required")
	}
	if r.DBName == "" {
		return fmt.Errorf("db_name required")
	}
	if r.Producer == nil {
		return fmt.Errorf("kafka producer required")
	}
	if r.Parser == nil {
		return fmt.Errorf("snapshot parser required")
	}
	if r.Log == nil {
		r.Log = log.New(os.Stderr, "", log.LstdFlags)
	}
	if r.StreamMode == "" {
		r.StreamMode = "dual"
	}
	if r.TopicSnapshot == "" {
		r.TopicSnapshot = "pika.snapshot"
	}
	if r.TopicSingle == "" {
		r.TopicSingle = "pika.stream"
	}
	specs, warnings := buildReaderSpecs(r.ReaderMode, r.ReaderTypes, r.ReaderPrefixes, r.Filter)
	for _, warning := range warnings {
		r.Log.Printf("snapshot reader warning: %s", warning)
	}
	if len(specs) == 0 {
		return fmt.Errorf("snapshot reader spec empty")
	}
	for i, spec := range specs {
		r.Log.Printf("snapshot reader[%d] type_mask=%d pattern=%s scan_batch=%d strategy=%d",
			i, spec.typeMask, spec.pattern, r.ScanBatch, r.ScanStrategy)
	}

	workers := r.Workers
	if workers <= 0 {
		workers = 4
	}
	windowSlots := r.WindowSlots
	if windowSlots <= 0 {
		windowSlots = 64
	}

	type workItem struct {
		rec Record
		seq uint64
	}
	type resultItem struct {
		record    kafka.Record
		seq       uint64
		hasRecord bool
	}

	workCh := make(chan workItem, workers*2)
	resultCh := make(chan resultItem, workers*2)
	errCh := make(chan error, 1)

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var (
		readSeq    atomic.Uint64
		enqueued   atomic.Uint64
		parsed     atomic.Uint64
		filtered   atomic.Uint64
		produced   atomic.Uint64
		lastReadAt atomic.Int64
		lastWorkAt atomic.Int64
		lastSendAt atomic.Int64
	)

	windowTokens := make(chan struct{}, windowSlots)
	for i := 0; i < windowSlots; i++ {
		windowTokens <- struct{}{}
	}

	topic := selectTopic(r.StreamMode, r.TopicSnapshot, r.TopicSingle)
	var seqCounter atomic.Uint64

	// Readers: I/O only (parallel).
	var readerWG sync.WaitGroup
	readerWG.Add(len(specs))
	for _, spec := range specs {
		spec := spec
		go func() {
			defer readerWG.Done()
			it, err := r.Parser.Open(dumpPath, r.DBName, ParserOptions{
				BatchNum:     r.BatchNum,
				CgoBatch:     r.CgoBatch,
				ScanBatch:    r.ScanBatch,
				ScanPattern:  spec.pattern,
				ScanStrategy: r.ScanStrategy,
				TypeMask:     spec.typeMask,
				ListTailN:    r.ListTailN,
			})
			if err != nil {
				select {
				case errCh <- err:
				default:
				}
				cancel()
				return
			}
			defer it.Close()
			for {
				records, err := it.NextBatch(r.CgoBatch)
				if err != nil {
					if err == io.EOF {
						return
					}
					select {
					case errCh <- err:
					default:
					}
					cancel()
					return
				}
				if len(records) == 0 {
					continue
				}
				for _, rec := range records {
					seq := seqCounter.Add(1)
					readSeq.Store(seq)
					lastReadAt.Store(time.Now().UnixNano())
					select {
					case <-windowTokens:
					case <-ctx.Done():
						return
					}
					select {
					case workCh <- workItem{rec: rec, seq: seq}:
						enqueued.Add(1)
					case <-ctx.Done():
						return
					}
				}
			}
		}()
	}

	go func() {
		readerWG.Wait()
		close(workCh)
	}()

	var wg sync.WaitGroup
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func() {
			defer wg.Done()
			for item := range workCh {
				rec := item.rec
				parsed.Add(1)
				lastWorkAt.Store(time.Now().UnixNano())
				argv, err := resp.ParseArray(rec.RawRESP)
				if err != nil || len(argv) == 0 {
					r.Log.Printf("snapshot parse resp failed: %v", err)
					filtered.Add(1)
					select {
					case resultCh <- resultItem{seq: item.seq, hasRecord: false}:
					case <-ctx.Done():
						return
					}
					continue
				}
				op := strings.ToLower(string(argv[0]))
				shouldSend := true
				if r.Filter != nil {
					shouldSend = r.Filter.ShouldSend(string(rec.Key), rec.DataType, op)
				}
				if !shouldSend {
					filtered.Add(1)
					select {
					case resultCh <- resultItem{seq: item.seq, hasRecord: false}:
					case <-ctx.Done():
						return
					}
					continue
				}
				payload := r.Builder.BuildSnapshotEventJSON(argv, r.DBName, rec.DataType, r.SourceID, rec.RawRESP, rec.Key)
				partitionKey := event.BuildPartitionKey(r.DBName, rec.DataType, rec.Key)
				select {
				case resultCh <- resultItem{record: kafka.Record{
					Topic:          topic,
					Key:            partitionKey,
					Value:          payload,
					HasAck:         false,
					HasSnapshotAck: true,
					SnapshotSeq:    item.seq,
				}, seq: item.seq, hasRecord: true}:
					lastWorkAt.Store(time.Now().UnixNano())
				case <-ctx.Done():
					return
				}
			}
		}()
	}

	go func() {
		wg.Wait()
		close(resultCh)
	}()

	progressTicker := time.NewTicker(5 * time.Second)
	defer progressTicker.Stop()

	pending := make(map[uint64]resultItem)
	nextSeq := uint64(1)

	logProgress := func(tag string) {
		now := time.Now()
		lastRead := time.Duration(0)
		if ts := lastReadAt.Load(); ts > 0 {
			lastRead = now.Sub(time.Unix(0, ts))
		}
		lastWork := time.Duration(0)
		if ts := lastWorkAt.Load(); ts > 0 {
			lastWork = now.Sub(time.Unix(0, ts))
		}
		lastSend := time.Duration(0)
		if ts := lastSendAt.Load(); ts > 0 {
			lastSend = now.Sub(time.Unix(0, ts))
		}
		r.Log.Printf("snapshot progress tag=%s read_seq=%d enqueued=%d parsed=%d produced=%d filtered=%d work_q=%d result_q=%d pending=%d last_read=%s last_work=%s last_send=%s",
			tag,
			readSeq.Load(),
			enqueued.Load(),
			parsed.Load(),
			produced.Load(),
			filtered.Load(),
			len(workCh),
			len(resultCh),
			len(pending),
			lastRead,
			lastWork,
			lastSend,
		)
	}

	drainPending := func() {
		for {
			res, ok := pending[nextSeq]
			if !ok {
				return
			}
			delete(pending, nextSeq)
			if res.hasRecord {
				if err := r.Producer.Produce(res.record); err != nil {
					r.Log.Printf("snapshot kafka produce failed: %v", err)
				} else {
					produced.Add(1)
					lastSendAt.Store(time.Now().UnixNano())
				}
			} else if r.Ckpt != nil {
				_ = r.Ckpt.OnSnapshotDone(nextSeq)
			}
			select {
			case windowTokens <- struct{}{}:
			default:
			}
			nextSeq++
		}
	}

	for {
		select {
		case err := <-errCh:
			if err != nil {
				return err
			}
		case res, ok := <-resultCh:
			if !ok {
				goto done
			}
			pending[res.seq] = res
			drainPending()
		case <-progressTicker.C:
			logProgress("running")
		case <-ctx.Done():
			return ctx.Err()
		}
	}

done:
	drainPending()
	if len(pending) > 0 {
		r.Log.Printf("snapshot pending not empty after drain: %d", len(pending))
	}

	remaining := r.Producer.Flush(10 * time.Second)
	if remaining > 0 {
		r.Log.Printf("snapshot flush timeout remaining=%d", remaining)
	}

	logProgress("done")
	return nil
}

func selectTopic(streamMode, snapshotTopic, singleTopic string) string {
	if streamMode == "single" {
		return singleTopic
	}
	return snapshotTopic
}
