package rsync2

import (
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/OpenAtomFoundation/pikiwidb-port-kafka-go/internal/pb/rsyncservice"
	"github.com/OpenAtomFoundation/pikiwidb-port-kafka-go/internal/pbnet"
)

const (
	defaultChunkBytes = 4 * 1024 * 1024
	portShiftRsync2   = 10001
)

type Client struct {
	MasterHost string
	MasterPort int

	DBName   string
	DumpPath string

	ChunkBytes        int
	Timeout           time.Duration
	WaitBgsaveTimeout time.Duration

	Log *log.Logger
}

func (c *Client) Fetch() error {
	if c.MasterHost == "" || c.MasterPort == 0 {
		return fmt.Errorf("master host/port required")
	}
	if c.DBName == "" {
		return fmt.Errorf("db_name required")
	}
	if c.DumpPath == "" {
		return fmt.Errorf("dump_path required")
	}
	timeout := c.Timeout
	if timeout == 0 {
		timeout = 30 * time.Second
	}
	chunk := c.ChunkBytes
	if chunk <= 0 {
		chunk = defaultChunkBytes
	}

	if err := os.RemoveAll(c.DumpPath); err != nil {
		return err
	}
	if err := os.MkdirAll(c.DumpPath, 0o755); err != nil {
		return err
	}

	snapshotUUID, files, err := c.fetchMeta(timeout)
	if err != nil {
		return err
	}
	for _, file := range files {
		if err := c.fetchFile(file, snapshotUUID, chunk, timeout); err != nil {
			return err
		}
	}
	return nil
}

func (c *Client) fetchMeta(timeout time.Duration) (snapshotUUID string, files []string, err error) {
	start := time.Now()
	attempt := 0
	metaErrorCount := 0
	wait := c.WaitBgsaveTimeout
	if wait <= 0 {
		wait = 30 * time.Minute
	}
	backoff := time.Second
	for {
		if time.Since(start) >= wait {
			return "", nil, fmt.Errorf("rsync2 meta timeout after %s attempts=%d", time.Since(start), attempt)
		}
		attempt++
		conn, err := pbnet.Dial(fmt.Sprintf("%s:%d", c.MasterHost, c.MasterPort+portShiftRsync2), timeout)
		if err != nil {
			time.Sleep(time.Second)
			continue
		}
		_ = conn.SetWriteDeadline(time.Now().Add(timeout))
		req := &rsyncservice.RsyncRequest{
			Type:        rsyncservice.Type_kRsyncMeta.Enum(),
			ReaderIndex: protoInt32(0),
			DbName:      protoString(c.DBName),
			SlotId:      protoUint32(0),
		}
		if err := conn.Send(req); err != nil {
			_ = conn.Close()
			time.Sleep(time.Second)
			continue
		}

		_ = conn.SetReadDeadline(time.Now().Add(timeout))
		var resp rsyncservice.RsyncResponse
		err = conn.Recv(&resp)
		_ = conn.Close()
		if err != nil {
			time.Sleep(time.Second)
			continue
		}
		if resp.GetCode() != rsyncservice.StatusCode_kOk || resp.GetMetaResp() == nil {
			metaErrorCount++
			if metaErrorCount == 1 || metaErrorCount%5 == 0 {
				if c.Log != nil {
					c.Log.Printf("rsync2 meta not ready, waiting bgsave attempt=%d elapsed=%s next_sleep=%s",
						metaErrorCount, time.Since(start).Truncate(time.Second), backoff)
				}
			}
			time.Sleep(backoff)
			backoff *= 2
			if backoff > 15*time.Second {
				backoff = 15 * time.Second
			}
			continue
		}
		snapshotUUID = resp.GetSnapshotUuid()
		files = append([]string(nil), resp.GetMetaResp().GetFilenames()...)
		return snapshotUUID, files, nil
	}
}

func (c *Client) fetchFile(filename, snapshotUUID string, chunkBytes int, timeout time.Duration) error {
	if filename == "" {
		return errors.New("rsync2: empty filename")
	}
	conn, err := pbnet.Dial(fmt.Sprintf("%s:%d", c.MasterHost, c.MasterPort+portShiftRsync2), timeout)
	if err != nil {
		return err
	}
	defer conn.Close()

	localPath := filepath.Join(c.DumpPath, filename)
	if err := os.MkdirAll(filepath.Dir(localPath), 0o755); err != nil {
		return err
	}
	out, err := os.OpenFile(localPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	defer out.Close()

	var offset uint64
	for {
		req := &rsyncservice.RsyncRequest{
			Type:        rsyncservice.Type_kRsyncFile.Enum(),
			ReaderIndex: protoInt32(0),
			DbName:      protoString(c.DBName),
			SlotId:      protoUint32(0),
			FileReq: &rsyncservice.FileRequest{
				Filename: protoString(filename),
				Offset:   protoUint64(offset),
				Count:    protoUint64(uint64(chunkBytes)),
			},
		}
		_ = conn.SetWriteDeadline(time.Now().Add(timeout))
		if err := conn.Send(req); err != nil {
			return err
		}
		_ = conn.SetReadDeadline(time.Now().Add(timeout))
		var resp rsyncservice.RsyncResponse
		if err := conn.Recv(&resp); err != nil {
			return err
		}
		if resp.GetCode() != rsyncservice.StatusCode_kOk || resp.GetFileResp() == nil {
			return fmt.Errorf("rsync2: file response error")
		}
		if resp.GetSnapshotUuid() != snapshotUUID {
			return fmt.Errorf("rsync2: snapshot uuid changed expected=%s actual=%s", snapshotUUID, resp.GetSnapshotUuid())
		}
		fileResp := resp.GetFileResp()
		if fileResp.GetOffset() != offset {
			// Keep writing at the requested offset (same as C++ seekp).
		}
		if _, err := out.Seek(int64(offset), io.SeekStart); err != nil {
			return err
		}
		if _, err := out.Write(fileResp.GetData()); err != nil {
			return err
		}
		offset += fileResp.GetCount()
		if fileResp.GetEof() != 0 {
			return nil
		}
	}
}

func protoString(v string) *string { return &v }
func protoInt32(v int32) *int32    { return &v }
func protoUint32(v uint32) *uint32 { return &v }
func protoUint64(v uint64) *uint64 { return &v }
