package repl

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/OpenAtomFoundation/pikiwidb-port-kafka-go/internal/binlog"
	ckpt "github.com/OpenAtomFoundation/pikiwidb-port-kafka-go/internal/checkpoint"
	"github.com/OpenAtomFoundation/pikiwidb-port-kafka-go/internal/event"
	"github.com/OpenAtomFoundation/pikiwidb-port-kafka-go/internal/filter"
	"github.com/OpenAtomFoundation/pikiwidb-port-kafka-go/internal/kafka"
	"github.com/OpenAtomFoundation/pikiwidb-port-kafka-go/internal/pb/innermessage"
	"github.com/OpenAtomFoundation/pikiwidb-port-kafka-go/internal/pbnet"
	"github.com/OpenAtomFoundation/pikiwidb-port-kafka-go/internal/resp"
	"github.com/OpenAtomFoundation/pikiwidb-port-kafka-go/internal/rsync2"
	"github.com/OpenAtomFoundation/pikiwidb-port-kafka-go/internal/wal"
)

const (
	portShiftReplServer = 2000

	replConnectTimeout = 1500 * time.Millisecond
	replRecvTimeout    = 1000 * time.Millisecond
	replSendTimeout    = 30 * time.Second

	ackInterval      = time.Second
	ackFlushInterval = 500 * time.Millisecond
)

type Offset struct {
	Filenum uint32
	Offset  uint64
}

func (o Offset) IsZero() bool { return o.Filenum == 0 && o.Offset == 0 }

func newerOffset(a, b Offset) bool {
	if a.Filenum > b.Filenum {
		return true
	}
	if a.Filenum == b.Filenum && a.Offset > b.Offset {
		return true
	}
	return false
}

type SnapshotRunner interface {
	Run(ctx context.Context, dumpPath string) error
}

type Client struct {
	MasterHost string
	MasterPort int
	Password   string

	LocalIP   string
	LocalPort int

	DBName   string
	DumpPath string

	StreamMode    string
	TopicSnapshot string
	TopicBinlog   string
	TopicSingle   string

	BinlogWorkers          int
	BinlogQueueSize        int
	WindowSlots            int
	SenderBatchSize        int
	BinlogProgressInterval time.Duration

	AckDelayWarn time.Duration
	IdleTimeout  time.Duration

	Filter   *filter.Filter
	Builder  event.Builder
	Producer *kafka.Producer
	Ckpt     *ckpt.Manager
	Wal      *wal.Writer

	Rsync2Timeout         time.Duration
	WaitBgsaveTimeout     time.Duration
	ForceFullSync         bool
	SyncPointPurgedAction string
	FetchMasterOffset     func(ctx context.Context) (Offset, error)

	Snapshot SnapshotRunner

	Log *log.Logger
}

func (c *Client) Run(ctx context.Context) error {
	if c.Log == nil {
		c.Log = log.New(os.Stderr, "", log.LstdFlags)
	}
	if c.MasterHost == "" || c.MasterPort == 0 {
		return fmt.Errorf("master_host/master_port required")
	}
	if c.DBName == "" {
		return fmt.Errorf("db_name required")
	}
	if c.Producer == nil {
		return fmt.Errorf("kafka producer required")
	}
	if c.Ckpt == nil {
		return fmt.Errorf("checkpoint manager required")
	}
	if c.Wal == nil {
		return fmt.Errorf("wal writer required")
	}

	if c.BinlogWorkers <= 0 {
		c.BinlogWorkers = 1
	}
	if c.BinlogQueueSize <= 0 {
		c.BinlogQueueSize = 1024
	}
	if c.WindowSlots <= 0 {
		c.WindowSlots = 64
	}
	if c.SenderBatchSize <= 0 {
		c.SenderBatchSize = 256
	}
	if c.AckDelayWarn > 0 && c.AckDelayWarn < time.Second {
		c.AckDelayWarn = time.Second
	}
	if c.IdleTimeout > 0 && c.IdleTimeout < time.Second {
		c.IdleTimeout = time.Second
	}

	localIP := c.LocalIP
	if ip, err := resolveLocalIP(c.MasterHost, c.MasterPort, replConnectTimeout); err == nil && ip != "" {
		localIP = ip
	}
	if localIP == "" {
		localIP = "127.0.0.1"
	}
	localPort := c.LocalPort
	if localPort == 0 {
		localPort = 21333
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		conn, err := c.connectRepl()
		if err != nil {
			c.Log.Printf("pb repl: connect failed: %v", err)
			sleepContext(ctx, time.Second)
			continue
		}

		if err := c.sendMetaSync(conn, localIP, localPort); err != nil {
			c.Log.Printf("pb repl: MetaSync failed: %v", err)
			_ = conn.Close()
			sleepContext(ctx, time.Second)
			continue
		}

		start := c.getStartOffset()
		var sessionID int32
		var replyCode innermessage.InnerResponse_TrySync_ReplyCode

		if c.ForceFullSync {
			c.Log.Printf("pb repl: full sync forced on startup")
			start = Offset{Filenum: 0, Offset: 0}
			if _, err := c.sendDBSync(conn, localIP, localPort, start); err != nil {
				c.Log.Printf("pb repl: DBSync failed: %v", err)
				_ = conn.Close()
				sleepContext(ctx, time.Second)
				continue
			}
			newOffset, err := c.performFullSync(ctx)
			if err != nil {
				c.Log.Printf("pb repl: full sync failed: %v", err)
				_ = conn.Close()
				sleepContext(ctx, time.Second)
				continue
			}
			_ = conn.Close()
			c.ForceFullSync = false

			conn, err = c.connectRepl()
			if err != nil {
				sleepContext(ctx, time.Second)
				continue
			}
			if err := c.sendMetaSync(conn, localIP, localPort); err != nil {
				_ = conn.Close()
				sleepContext(ctx, time.Second)
				continue
			}
			sessionID, replyCode, err = c.sendTrySync(conn, localIP, localPort, newOffset)
			if err != nil || replyCode != innermessage.InnerResponse_TrySync_kOk {
				c.Log.Printf("pb repl: TrySync after full sync failed: err=%v reply=%v", err, replyCode)
				_ = conn.Close()
				sleepContext(ctx, time.Second)
				continue
			}
			start = newOffset
		} else {
			sessionID, replyCode, err = c.sendTrySync(conn, localIP, localPort, start)
			if err != nil {
				c.Log.Printf("pb repl: TrySync failed: %v", err)
				_ = conn.Close()
				sleepContext(ctx, time.Second)
				continue
			}

			if replyCode == innermessage.InnerResponse_TrySync_kSyncPointBePurged {
				action := strings.TrimSpace(strings.ToLower(c.SyncPointPurgedAction))
				if action == "" {
					action = "full_sync"
				}
				switch action {
				case "full_sync":
					if _, err := c.sendDBSync(conn, localIP, localPort, start); err != nil {
						c.Log.Printf("pb repl: DBSync failed: %v", err)
						_ = conn.Close()
						sleepContext(ctx, time.Second)
						continue
					}
					newOffset, err := c.performFullSync(ctx)
					if err != nil {
						c.Log.Printf("pb repl: full sync failed: %v", err)
						_ = conn.Close()
						sleepContext(ctx, time.Second)
						continue
					}
					_ = conn.Close()

					conn, err = c.connectRepl()
					if err != nil {
						sleepContext(ctx, time.Second)
						continue
					}
					if err := c.sendMetaSync(conn, localIP, localPort); err != nil {
						_ = conn.Close()
						sleepContext(ctx, time.Second)
						continue
					}
					sessionID, replyCode, err = c.sendTrySync(conn, localIP, localPort, newOffset)
					if err != nil || replyCode != innermessage.InnerResponse_TrySync_kOk {
						c.Log.Printf("pb repl: TrySync after full sync failed: err=%v reply=%v", err, replyCode)
						_ = conn.Close()
						sleepContext(ctx, time.Second)
						continue
					}
					start = newOffset
				case "start_from_master":
					if c.FetchMasterOffset == nil {
						c.Log.Printf("pb repl: sync point purged but FetchMasterOffset not configured")
						_ = conn.Close()
						sleepContext(ctx, time.Second)
						continue
					}
					masterOffset, err := c.FetchMasterOffset(ctx)
					if err != nil {
						c.Log.Printf("pb repl: fetch master binlog_offset failed: %v", err)
						_ = conn.Close()
						sleepContext(ctx, time.Second)
						continue
					}
					c.Log.Printf("pb repl: sync point purged, start from master binlog_offset=%d:%d", masterOffset.Filenum, masterOffset.Offset)
					_ = conn.Close()

					conn, err = c.connectRepl()
					if err != nil {
						sleepContext(ctx, time.Second)
						continue
					}
					if err := c.sendMetaSync(conn, localIP, localPort); err != nil {
						_ = conn.Close()
						sleepContext(ctx, time.Second)
						continue
					}
					sessionID, replyCode, err = c.sendTrySync(conn, localIP, localPort, masterOffset)
					if err != nil || replyCode != innermessage.InnerResponse_TrySync_kOk {
						c.Log.Printf("pb repl: TrySync after start_from_master failed: err=%v reply=%v", err, replyCode)
						_ = conn.Close()
						sleepContext(ctx, time.Second)
						continue
					}
					start = masterOffset
				case "pause":
					c.Log.Printf("pb repl: sync point purged, action=pause; waiting for operator")
					_ = conn.Close()
					for {
						if ctx.Err() != nil {
							return ctx.Err()
						}
						sleepContext(ctx, time.Second)
					}
				default:
					c.Log.Printf("pb repl: unknown sync_point_purged_action=%q, fallback to full_sync", action)
					if _, err := c.sendDBSync(conn, localIP, localPort, start); err != nil {
						c.Log.Printf("pb repl: DBSync failed: %v", err)
						_ = conn.Close()
						sleepContext(ctx, time.Second)
						continue
					}
					newOffset, err := c.performFullSync(ctx)
					if err != nil {
						c.Log.Printf("pb repl: full sync failed: %v", err)
						_ = conn.Close()
						sleepContext(ctx, time.Second)
						continue
					}
					_ = conn.Close()

					conn, err = c.connectRepl()
					if err != nil {
						sleepContext(ctx, time.Second)
						continue
					}
					if err := c.sendMetaSync(conn, localIP, localPort); err != nil {
						_ = conn.Close()
						sleepContext(ctx, time.Second)
						continue
					}
					sessionID, replyCode, err = c.sendTrySync(conn, localIP, localPort, newOffset)
					if err != nil || replyCode != innermessage.InnerResponse_TrySync_kOk {
						c.Log.Printf("pb repl: TrySync after full sync failed: err=%v reply=%v", err, replyCode)
						_ = conn.Close()
						sleepContext(ctx, time.Second)
						continue
					}
					start = newOffset
				}
			} else if replyCode != innermessage.InnerResponse_TrySync_kOk {
				c.Log.Printf("pb repl: TrySync error code %v", replyCode)
				_ = conn.Close()
				sleepContext(ctx, time.Second)
				continue
			}
		}

		if err := c.binlogLoop(ctx, conn, localIP, localPort, start, sessionID); err != nil {
			c.Log.Printf("pb repl: binlog loop exited: %v", err)
			_ = conn.Close()
			sleepContext(ctx, time.Second)
			continue
		}
		_ = conn.Close()
	}
}

func (c *Client) connectRepl() (*pbnet.Conn, error) {
	addr := fmt.Sprintf("%s:%d", c.MasterHost, c.MasterPort+portShiftReplServer)
	conn, err := pbnet.Dial(addr, replConnectTimeout)
	if err != nil {
		return nil, err
	}
	_ = conn.SetReadDeadline(time.Now().Add(replRecvTimeout))
	_ = conn.SetWriteDeadline(time.Now().Add(replSendTimeout))
	return conn, nil
}

func (c *Client) sendMetaSync(conn *pbnet.Conn, localIP string, localPort int) error {
	req := &innermessage.InnerRequest{
		Type: innermessage.Type_kMetaSync.Enum(),
		MetaSync: &innermessage.InnerRequest_MetaSync{
			Node: &innermessage.Node{Ip: protoString(localIP), Port: protoInt32(int32(localPort))},
		},
	}
	if c.Password != "" {
		req.MetaSync.Auth = protoString(c.Password)
	}
	_ = conn.SetWriteDeadline(time.Now().Add(replSendTimeout))
	if err := conn.Send(req); err != nil {
		return err
	}
	_ = conn.SetReadDeadline(time.Now().Add(replRecvTimeout))
	var resp innermessage.InnerResponse
	if err := conn.Recv(&resp); err != nil {
		return err
	}
	if resp.GetCode() != innermessage.StatusCode_kOk {
		return fmt.Errorf("metasync error: %s", resp.GetReply())
	}
	return nil
}

func (c *Client) sendTrySync(conn *pbnet.Conn, localIP string, localPort int, start Offset) (sessionID int32, replyCode innermessage.InnerResponse_TrySync_ReplyCode, err error) {
	req := &innermessage.InnerRequest{
		Type: innermessage.Type_kTrySync.Enum(),
		TrySync: &innermessage.InnerRequest_TrySync{
			Node: &innermessage.Node{Ip: protoString(localIP), Port: protoInt32(int32(localPort))},
			Slot: &innermessage.Slot{DbName: protoString(c.DBName), SlotId: protoUint32(0)},
			BinlogOffset: &innermessage.BinlogOffset{
				Filenum: protoUint32(start.Filenum),
				Offset:  protoUint64(start.Offset),
			},
		},
	}
	_ = conn.SetWriteDeadline(time.Now().Add(replSendTimeout))
	if err := conn.Send(req); err != nil {
		return 0, 0, err
	}

	var resp innermessage.InnerResponse
	unexpected := 0
	for {
		_ = conn.SetReadDeadline(time.Now().Add(replRecvTimeout))
		if err := conn.Recv(&resp); err != nil {
			return 0, 0, err
		}
		if resp.GetType() == innermessage.Type_kTrySync && resp.GetTrySync() != nil {
			break
		}
		unexpected++
		if unexpected >= 10 {
			return 0, 0, fmt.Errorf("unexpected TrySync response type")
		}
	}
	ts := resp.GetTrySync()
	return ts.GetSessionId(), ts.GetReplyCode(), nil
}

func (c *Client) sendDBSync(conn *pbnet.Conn, localIP string, localPort int, start Offset) (sessionID int32, err error) {
	req := &innermessage.InnerRequest{
		Type: innermessage.Type_kDBSync.Enum(),
		DbSync: &innermessage.InnerRequest_DBSync{
			Node: &innermessage.Node{Ip: protoString(localIP), Port: protoInt32(int32(localPort))},
			Slot: &innermessage.Slot{DbName: protoString(c.DBName), SlotId: protoUint32(0)},
			BinlogOffset: &innermessage.BinlogOffset{
				Filenum: protoUint32(start.Filenum),
				Offset:  protoUint64(start.Offset),
			},
		},
	}
	_ = conn.SetWriteDeadline(time.Now().Add(replSendTimeout))
	if err := conn.Send(req); err != nil {
		return 0, err
	}
	_ = conn.SetReadDeadline(time.Now().Add(replRecvTimeout))
	var resp innermessage.InnerResponse
	if err := conn.Recv(&resp); err != nil {
		return 0, err
	}
	if resp.GetType() != innermessage.Type_kDBSync || resp.GetDbSync() == nil {
		return 0, fmt.Errorf("dbsync response missing")
	}
	if resp.GetCode() != innermessage.StatusCode_kOk {
		return 0, fmt.Errorf("dbsync error: %s", resp.GetReply())
	}
	return resp.GetDbSync().GetSessionId(), nil
}

func (c *Client) sendBinlogAck(conn *pbnet.Conn, localIP string, localPort int, start, end Offset, sessionID int32, first bool) error {
	req := &innermessage.InnerRequest{
		Type: innermessage.Type_kBinlogSync.Enum(),
		BinlogSync: &innermessage.InnerRequest_BinlogSync{
			Node:          &innermessage.Node{Ip: protoString(localIP), Port: protoInt32(int32(localPort))},
			DbName:        protoString(c.DBName),
			SlotId:        protoUint32(0),
			AckRangeStart: &innermessage.BinlogOffset{Filenum: protoUint32(start.Filenum), Offset: protoUint64(start.Offset), Term: protoUint32(0), Index: protoUint64(0)},
			AckRangeEnd:   &innermessage.BinlogOffset{Filenum: protoUint32(end.Filenum), Offset: protoUint64(end.Offset), Term: protoUint32(0), Index: protoUint64(0)},
			SessionId:     protoInt32(sessionID),
			FirstSend:     protoBool(first),
		},
	}
	_ = conn.SetWriteDeadline(time.Now().Add(replSendTimeout))
	return conn.Send(req)
}

func (c *Client) getStartOffset() Offset {
	if cp, ok := c.Ckpt.State(); ok {
		return Offset{Filenum: cp.AckCP.Filenum, Offset: cp.AckCP.Offset}
	}
	loaded, ok, err := c.Ckpt.Load()
	if err == nil && ok {
		return Offset{Filenum: loaded.AckCP.Filenum, Offset: loaded.AckCP.Offset}
	}
	return Offset{Filenum: math.MaxUint32, Offset: 0}
}

func (c *Client) performFullSync(ctx context.Context) (Offset, error) {
	dumpPath := buildDumpPath(c.DumpPath, c.DBName)
	rsyncTimeout := c.Rsync2Timeout
	if rsyncTimeout == 0 {
		rsyncTimeout = 30 * time.Second
	}
	rc := &rsync2.Client{
		MasterHost:        c.MasterHost,
		MasterPort:        c.MasterPort,
		DBName:            c.DBName,
		DumpPath:          dumpPath,
		Timeout:           rsyncTimeout,
		WaitBgsaveTimeout: c.WaitBgsaveTimeout,
		Log:               c.Log,
	}
	if err := rc.Fetch(); err != nil {
		return Offset{}, err
	}
	newOffset, err := loadBgsaveInfo(dumpPath, c.DumpPath)
	if err != nil {
		return Offset{}, err
	}
	if c.Snapshot == nil {
		return Offset{}, errors.New("snapshot runner not configured")
	}
	_ = c.Ckpt.BeginSnapshot(ckpt.SnapshotState{
		State:           "running",
		BgsaveEndFile:   newOffset.Filenum,
		BgsaveEndOffset: newOffset.Offset,
	})
	if err := c.Snapshot.Run(ctx, dumpPath); err != nil {
		return Offset{}, err
	}
	var snapshotSeq uint64
	if cp, ok := c.Ckpt.State(); ok {
		snapshotSeq = cp.Snapshot.SnapshotSeq
	}
	_ = c.Ckpt.SetSnapshotState(ckpt.SnapshotState{
		State:           "done",
		BgsaveEndFile:   newOffset.Filenum,
		BgsaveEndOffset: newOffset.Offset,
		SnapshotSeq:     snapshotSeq,
	})
	_ = c.Ckpt.SetAckPosition(ckpt.Position{Seq: 0, Filenum: newOffset.Filenum, Offset: newOffset.Offset})
	return newOffset, nil
}

func (c *Client) binlogLoop(ctx context.Context, conn *pbnet.Conn, localIP string, localPort int, start Offset, sessionID int32) error {
	epoch := uint32(sessionID)
	if err := c.Ckpt.BeginEpoch(epoch); err != nil {
		return err
	}
	c.Log.Printf("pb repl: enter binlog loop session_id=%d start=%d:%d", sessionID, start.Filenum, start.Offset)

	stats := &binlogStats{}

	lastSentAck := start
	var durable Offset
	hasDurable := false

	pendingAckStart := Offset{}
	hasPendingAck := false

	lastAckTime := time.Now()
	lastWarnTime := time.Now()
	lastBinlogTime := time.Now()
	lastNonBinlogLog := lastBinlogTime
	lastSessionMismatchLog := lastBinlogTime
	logThrottle := 5 * time.Second
	progressInterval := c.BinlogProgressInterval
	var progressTicker *time.Ticker
	var progressCh <-chan time.Time
	if progressInterval > 0 {
		progressTicker = time.NewTicker(progressInterval)
		progressCh = progressTicker.C
		defer progressTicker.Stop()
	}

	if err := c.sendBinlogAck(conn, localIP, localPort, start, start, sessionID, true); err != nil {
		return err
	}
	durable = start
	hasDurable = true

	ackCtx, ackCancel := context.WithCancel(ctx)
	defer ackCancel()
	go c.ackKeepaliveLoop(ackCtx, conn, localIP, localPort, sessionID)

	flushCtx, flushCancel := context.WithCancel(ctx)
	defer flushCancel()
	go func() {
		ticker := time.NewTicker(ackFlushInterval)
		defer ticker.Stop()
		for {
			select {
			case <-flushCtx.Done():
				return
			case <-ticker.C:
				if err := c.Ckpt.FlushAck(); err != nil {
					c.Log.Printf("checkpoint flush failed: %v", err)
				}
			}
		}
	}()

	worker := newWorkerPool(c, epoch, stats)
	defer worker.stop()

	seqCounter := uint64(1)
	ring := newRingWindow(c.WindowSlots)

	sendCh := make(chan []kafka.Record, 4)
	senderCtx, senderCancel := context.WithCancel(ctx)
	senderDone := make(chan struct{})
	go func() {
		defer close(senderDone)
		for {
			select {
			case <-senderCtx.Done():
				return
			case batch, ok := <-sendCh:
				if !ok {
					return
				}
				for _, record := range batch {
					if err := c.Producer.Produce(record); err != nil {
						c.Log.Printf("kafka produce failed: %v", err)
					} else if stats != nil {
						stats.producedCount.Add(1)
						stats.lastSendAt.Store(time.Now().UnixNano())
					}
				}
			}
		}
	}()
	defer func() {
		close(sendCh)
		senderCancel()
		<-senderDone
	}()

	drainRing := func() error {
		for {
			batch, advanced := ring.Drain(c.SenderBatchSize)
			if len(batch) > 0 {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case sendCh <- batch:
				}
			}
			if !advanced {
				return nil
			}
		}
	}

	handleResult := func(res binlogResult) error {
		if res.err != nil {
			c.Log.Printf("pb repl: binlog task failed: %v", res.err)
			return res.err
		}
		if !ring.MarkReady(res.seq, res.record, res.hasRecord) {
			return fmt.Errorf("ring missing seq %d", res.seq)
		}
		if !res.hasRecord {
			if err := c.Ckpt.OnDoneDeferred(epoch, res.pos); err != nil {
				return err
			}
		}
		return drainRing()
	}

	drainResults := func(block bool) error {
		if block {
			res, ok := worker.recv(ctx)
			if !ok {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				return fmt.Errorf("worker stopped")
			}
			if err := handleResult(res); err != nil {
				return err
			}
		}
		for {
			res, ok := worker.tryRecv()
			if !ok {
				return nil
			}
			if err := handleResult(res); err != nil {
				return err
			}
		}
	}

	logProgress := func(tag string) {
		if stats == nil {
			return
		}
		now := time.Now()
		lastRecv := time.Duration(0)
		if ts := stats.lastRecvAt.Load(); ts > 0 {
			lastRecv = now.Sub(time.Unix(0, ts))
		}
		lastWork := time.Duration(0)
		if ts := stats.lastWorkAt.Load(); ts > 0 {
			lastWork = now.Sub(time.Unix(0, ts))
		}
		lastSend := time.Duration(0)
		if ts := stats.lastSendAt.Load(); ts > 0 {
			lastSend = now.Sub(time.Unix(0, ts))
		}
		c.Log.Printf("binlog progress tag=%s recv=%d enqueued=%d parsed=%d produced=%d filtered=%d err=%d work_q=%d result_q=%d ring=%d/%d ack=%d:%d durable=%d:%d last_recv=%s last_work=%s last_send=%s",
			tag,
			stats.recvCount.Load(),
			stats.enqCount.Load(),
			stats.parsedCount.Load(),
			stats.producedCount.Load(),
			stats.filteredCount.Load(),
			stats.errCount.Load(),
			len(worker.tasks),
			len(worker.results),
			ring.Count(),
			ring.Size(),
			lastSentAck.Filenum,
			lastSentAck.Offset,
			durable.Filenum,
			durable.Offset,
			lastRecv,
			lastWork,
			lastSend,
		)
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-progressCh:
			logProgress("running")
		default:
		}

		var resp innermessage.InnerResponse
		_ = conn.SetReadDeadline(time.Now().Add(replRecvTimeout))
		err := conn.Recv(&resp)
		now := time.Now()
		if err != nil {
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				// keep alive
			} else {
				return err
			}
		} else {
			if resp.GetType() == innermessage.Type_kBinlogSync {
				matchedSession := false
				loggedMismatch := false
				for _, res := range resp.GetBinlogSync() {
					if res.GetSessionId() != sessionID {
						if !loggedMismatch && now.Sub(lastSessionMismatchLog) >= logThrottle {
							c.Log.Printf("pb repl: binlog session mismatch expected=%d got=%d", sessionID, res.GetSessionId())
							lastSessionMismatchLog = now
							loggedMismatch = true
						}
						continue
					}
					matchedSession = true
					raw := res.GetBinlog()
					if stats != nil {
						stats.recvCount.Add(1)
						stats.lastRecvAt.Store(time.Now().UnixNano())
					}
					if len(raw) < 34 && now.Sub(lastNonBinlogLog) >= logThrottle {
						c.Log.Printf("pb repl: binlog too short len=%d filenum=%d offset=%d",
							len(raw), res.GetBinlogOffset().GetFilenum(), res.GetBinlogOffset().GetOffset())
						lastNonBinlogLog = now
					}
					task := binlogTask{
						seq: seqCounter,
						raw: raw,
					}
					for ring.IsFull() {
						if err := drainResults(true); err != nil {
							return err
						}
					}
					walPos := ckpt.Position{
						Seq:     task.seq,
						Filenum: res.GetBinlogOffset().GetFilenum(),
						Offset:  res.GetBinlogOffset().GetOffset(),
					}
					walState, err := c.Wal.AppendBinlog(epoch, walPos.Seq, walPos.Filenum, walPos.Offset, task.raw)
					if err != nil {
						return err
					}
					if err := c.Ckpt.OnDurable(epoch, walPos, ckpt.WalState{Segment: walState.Segment, Offset: walState.Offset}); err != nil {
						return err
					}
					if !ring.Add(task.seq) {
						return fmt.Errorf("ring add failed")
					}
					durable = Offset{Filenum: walPos.Filenum, Offset: walPos.Offset}
					hasDurable = true
					if !hasPendingAck && newerOffset(durable, lastSentAck) {
						pendingAckStart = durable
						hasPendingAck = true
					}
					seqCounter++
					for !worker.tryPush(task) {
						if err := drainResults(true); err != nil {
							return err
						}
					}
					if stats != nil {
						stats.enqCount.Add(1)
					}
				}
				if matchedSession {
					lastBinlogTime = now
				}
			} else if now.Sub(lastNonBinlogLog) >= logThrottle {
				c.Log.Printf("pb repl: unexpected response type while waiting binlog sync type=%v", resp.GetType())
				lastNonBinlogLog = now
			}
		}

		if err := drainResults(false); err != nil {
			return err
		}

		committed := lastSentAck
		if hasDurable && newerOffset(durable, committed) {
			committed = durable
		}
		msSince := time.Since(lastAckTime)
		if c.AckDelayWarn > 0 && msSince >= c.AckDelayWarn {
			if time.Since(lastWarnTime) >= c.AckDelayWarn {
				c.Log.Printf("pb repl: ack delay %s session_id=%d last_ack=%d:%d durable=%d:%d",
					msSince, sessionID, lastSentAck.Filenum, lastSentAck.Offset, committed.Filenum, committed.Offset)
				lastWarnTime = time.Now()
			}
		}
		if c.IdleTimeout > 0 && time.Since(lastBinlogTime) >= c.IdleTimeout {
			return fmt.Errorf("pb repl: idle %s without binlog response, reconnecting", time.Since(lastBinlogTime))
		}

		sendPing := false
		shouldSendAck := false
		ackStart := Offset{}
		ackEnd := Offset{}
		if hasPendingAck && newerOffset(committed, lastSentAck) {
			ackStart = pendingAckStart
			ackEnd = committed
			shouldSendAck = true
		} else if msSince >= ackInterval {
			sendPing = true
			shouldSendAck = true
		}
		if shouldSendAck {
			if err := c.sendBinlogAck(conn, localIP, localPort, ackStart, ackEnd, sessionID, false); err == nil {
				lastAckTime = time.Now()
				if !sendPing {
					lastSentAck = ackEnd
					hasPendingAck = false
				}
			}
		}
	}
}

func (c *Client) ackKeepaliveLoop(ctx context.Context, conn *pbnet.Conn, localIP string, localPort int, sessionID int32) {
	ticker := time.NewTicker(ackInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			_ = c.sendBinlogAck(conn, localIP, localPort, Offset{}, Offset{}, sessionID, false)
		}
	}
}

type binlogTask struct {
	seq uint64
	raw []byte
}

type binlogResult struct {
	seq       uint64
	pos       ckpt.Position
	hasRecord bool
	record    kafka.Record
	err       error
}

type binlogStats struct {
	recvCount     atomic.Uint64
	enqCount      atomic.Uint64
	parsedCount   atomic.Uint64
	producedCount atomic.Uint64
	filteredCount atomic.Uint64
	errCount      atomic.Uint64
	lastRecvAt    atomic.Int64
	lastWorkAt    atomic.Int64
	lastSendAt    atomic.Int64
}

type workerPool struct {
	c     *Client
	epoch uint32
	stats *binlogStats

	tasks   chan binlogTask
	results chan binlogResult

	stopOnce sync.Once
	stopCh   chan struct{}
}

func newWorkerPool(c *Client, epoch uint32, stats *binlogStats) *workerPool {
	wp := &workerPool{
		c:       c,
		epoch:   epoch,
		stats:   stats,
		tasks:   make(chan binlogTask, c.BinlogQueueSize),
		results: make(chan binlogResult, c.BinlogQueueSize),
		stopCh:  make(chan struct{}),
	}
	for i := 0; i < c.BinlogWorkers; i++ {
		go wp.workerLoop()
	}
	return wp
}

func (w *workerPool) stop() {
	w.stopOnce.Do(func() {
		close(w.stopCh)
	})
}

func (w *workerPool) tryPush(task binlogTask) bool {
	select {
	case <-w.stopCh:
		return false
	case w.tasks <- task:
		return true
	default:
		return false
	}
}

func (w *workerPool) tryRecv() (binlogResult, bool) {
	select {
	case res := <-w.results:
		return res, true
	default:
		return binlogResult{}, false
	}
}

func (w *workerPool) recv(ctx context.Context) (binlogResult, bool) {
	select {
	case res := <-w.results:
		return res, true
	case <-w.stopCh:
		return binlogResult{}, false
	case <-ctx.Done():
		return binlogResult{}, false
	}
}

func (w *workerPool) workerLoop() {
	for {
		select {
		case <-w.stopCh:
			return
		case task := <-w.tasks:
			res := w.handleTask(task)
			select {
			case <-w.stopCh:
				return
			case w.results <- res:
			}
		}
	}
}

func (w *workerPool) handleTask(task binlogTask) binlogResult {
	res := binlogResult{
		seq: task.seq,
	}

	item, err := binlog.DecodeTypeFirst(task.raw)
	if err != nil {
		if w.stats != nil {
			w.stats.errCount.Add(1)
			w.stats.lastWorkAt.Store(time.Now().UnixNano())
		}
		res.err = fmt.Errorf("binlog decode failed seq=%d len=%d: %w", task.seq, len(task.raw), err)
		return res
	}
	argv, err := resp.ParseArray(item.Content)
	if err != nil {
		if w.stats != nil {
			w.stats.errCount.Add(1)
			w.stats.lastWorkAt.Store(time.Now().UnixNano())
		}
		res.err = fmt.Errorf("binlog parse resp failed seq=%d len=%d: %w", task.seq, len(item.Content), err)
		return res
	}
	if len(argv) == 0 {
		if w.stats != nil {
			w.stats.errCount.Add(1)
			w.stats.lastWorkAt.Store(time.Now().UnixNano())
		}
		res.err = fmt.Errorf("binlog parse resp empty seq=%d", task.seq)
		return res
	}
	if w.stats != nil {
		w.stats.parsedCount.Add(1)
		w.stats.lastWorkAt.Store(time.Now().UnixNano())
	}

	var key []byte
	if len(argv) > 1 {
		key = argv[1]
	}
	rawResp := item.Content
	nowSec := uint64(time.Now().Unix())
	rawResp = binlog.MaybeTransformPksetexatRawRESP(argv, nowSec, rawResp)

	cmd := string(argv[0])
	dataType := event.CommandDataType(cmd)
	shouldSend := true
	if w.c.Filter != nil {
		shouldSend = w.c.Filter.ShouldSend(string(key), dataType, cmd)
	}

	pos := ckpt.Position{
		Seq:      task.seq,
		Filenum:  item.Filenum,
		Offset:   item.Offset,
		LogicID:  item.LogicID,
		ServerID: 0,
		TermID:   item.TermID,
		TsMS:     uint64(item.ExecTime) * 1000,
	}
	res.pos = pos

	if !shouldSend {
		if w.stats != nil {
			w.stats.filteredCount.Add(1)
		}
		return res
	}

	payload := w.c.Builder.BuildBinlogEventJSON(argv, event.BinlogItem{
		ExecTime: item.ExecTime,
		TermID:   item.TermID,
		LogicID:  item.LogicID,
		Filenum:  item.Filenum,
		Offset:   item.Offset,
	}, w.c.DBName, dataType, w.c.Ckpt.SourceID(), rawResp, key)

	topic := selectTopic(w.c.StreamMode, w.c.TopicSnapshot, w.c.TopicBinlog, w.c.TopicSingle, "binlog")
	partitionKey := event.BuildPartitionKey(w.c.DBName, dataType, key)
	res.hasRecord = true
	res.record = kafka.Record{
		Topic:  topic,
		Key:    partitionKey,
		Value:  payload,
		HasAck: true,
		Ack:    pos,
		Epoch:  w.epoch,
	}
	return res
}

func selectTopic(streamMode, snapshotTopic, binlogTopic, singleTopic, eventType string) string {
	if streamMode == "single" {
		return singleTopic
	}
	if eventType == "snapshot" {
		return snapshotTopic
	}
	return binlogTopic
}

func buildDumpPath(root, dbName string) string {
	if root == "" {
		return dbName
	}
	if strings.HasSuffix(root, "/") {
		return root + dbName
	}
	return root + "/" + dbName
}

func loadBgsaveInfo(dbRoot, dumpRoot string) (Offset, error) {
	infoPath := filepath.Join(dbRoot, "info")
	if _, err := os.Stat(infoPath); err != nil {
		alt := filepath.Join(dumpRoot, "info")
		if _, err2 := os.Stat(alt); err2 != nil {
			return Offset{}, fmt.Errorf("info file missing after dbsync")
		}
		infoPath = alt
	}
	data, err := os.ReadFile(infoPath)
	if err != nil {
		return Offset{}, err
	}
	lines := strings.Split(string(data), "\n")
	if len(lines) < 5 {
		return Offset{}, fmt.Errorf("info file malformed (need >=5 lines)")
	}
	filenum, err := strconv.ParseUint(strings.TrimSpace(lines[3]), 10, 32)
	if err != nil {
		return Offset{}, fmt.Errorf("parse info filenum: %w", err)
	}
	offset, err := strconv.ParseUint(strings.TrimSpace(lines[4]), 10, 64)
	if err != nil {
		return Offset{}, fmt.Errorf("parse info offset: %w", err)
	}
	_ = os.Remove(infoPath)
	return Offset{Filenum: uint32(filenum), Offset: offset}, nil
}

func resolveLocalIP(masterHost string, masterPort int, timeout time.Duration) (string, error) {
	conn, err := net.DialTimeout("tcp", fmt.Sprintf("%s:%d", masterHost, masterPort), timeout)
	if err != nil {
		return "", err
	}
	defer conn.Close()
	if tcp, ok := conn.LocalAddr().(*net.TCPAddr); ok && tcp.IP != nil {
		return tcp.IP.String(), nil
	}
	return "", fmt.Errorf("unexpected local addr %v", conn.LocalAddr())
}

func sleepContext(ctx context.Context, d time.Duration) {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
	case <-timer.C:
	}
}

func protoString(v string) *string { return &v }
func protoInt32(v int32) *int32    { return &v }
func protoUint32(v uint32) *uint32 { return &v }
func protoUint64(v uint64) *uint64 { return &v }
func protoBool(v bool) *bool       { return &v }
