package logging

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type RotatingFile struct {
	mu         sync.Mutex
	dir        string
	prefix     string
	interval   time.Duration
	file       *os.File
	nextRotate time.Time
	now        func() time.Time
}

func NewRotatingFile(dir, prefix string, interval time.Duration) (*RotatingFile, error) {
	if dir == "" {
		return nil, fmt.Errorf("log dir required")
	}
	if prefix == "" {
		prefix = "pika_port_go"
	}
	if interval < 0 {
		interval = 0
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	r := &RotatingFile{
		dir:      dir,
		prefix:   prefix,
		interval: interval,
		now:      time.Now,
	}
	if err := r.rotateLocked(r.now()); err != nil {
		return nil, err
	}
	return r, nil
}

func (r *RotatingFile) Write(p []byte) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.file == nil {
		if err := r.rotateLocked(r.now()); err != nil {
			return 0, err
		}
	}
	if r.interval > 0 {
		now := r.now()
		if !r.nextRotate.IsZero() && now.After(r.nextRotate) {
			if err := r.rotateLocked(now); err != nil {
				return 0, err
			}
		}
	}
	return r.file.Write(p)
}

func (r *RotatingFile) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.file == nil {
		return nil
	}
	err := r.file.Close()
	r.file = nil
	return err
}

func (r *RotatingFile) rotateLocked(now time.Time) error {
	if r.file != nil {
		_ = r.file.Close()
	}
	filename := r.prefix + ".log"
	if r.interval > 0 {
		start := now.Truncate(r.interval)
		r.nextRotate = start.Add(r.interval)
		filename = fmt.Sprintf("%s-%s.log", r.prefix, start.Format("20060102-150405"))
	} else {
		r.nextRotate = time.Time{}
	}
	path := filepath.Join(r.dir, filename)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	r.file = f
	return nil
}

func MultiWriter(writers ...io.Writer) io.Writer {
	return io.MultiWriter(writers...)
}
