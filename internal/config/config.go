package config

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/OpenAtomFoundation/pikiwidb-port-kafka-go/internal/event"
)

type Config struct {
	LocalIP   string
	LocalPort int

	MasterIP   string
	MasterPort int
	Passwd     string

	DBName string

	LogPath        string
	DumpPath       string
	CheckpointPath string

	KafkaBrokers           string
	KafkaClientID          string
	KafkaStreamMode        string
	KafkaTopicSnapshot     string
	KafkaTopicBinlog       string
	KafkaTopicSingle       string
	KafkaMessageMaxBytes   int
	KafkaEnableIdempotence bool

	WaitBgsaveTimeout time.Duration

	BinlogWorkers          int
	BinlogQueueSize        int
	WindowSlots            int
	SenderBatchSize        int
	BinlogProgressInterval time.Duration

	PBAckDelayWarn time.Duration
	PBIdleTimeout  time.Duration

	WalDir            string
	WalSegmentBytes   int64
	WalSyncEvery      int
	WalRetainSegments int

	SnapshotBatchNum       int
	SnapshotWorkers        int
	SnapshotReaderMode     string
	SnapshotReaderTypes    []string
	SnapshotReaderPrefixes []string
	SnapshotCgoBatch       int
	SnapshotScanBatch      int64
	SnapshotScanStrategy   string
	SnapshotListTailN      int64

	ArgsEncoding    event.PayloadEncoding
	RawRespEncoding event.PayloadEncoding
	IncludeRawResp  bool

	StartFromMaster bool

	SourceID string

	StartFilenum *uint32
	StartOffset  *uint64

	FilterGroupSpecs   []string
	FilterExcludeSpecs []string
}

type LoadResult struct {
	FilenumSpecified bool
	OffsetSpecified  bool
	Warnings         []string
}

func Default() Config {
	return Config{
		LocalIP:        "127.0.0.1",
		LocalPort:      0,
		MasterIP:       "127.0.0.1",
		MasterPort:     0,
		DBName:         "db0",
		LogPath:        "./log/",
		DumpPath:       "./rsync_dump/",
		CheckpointPath: "./checkpoint.json",

		KafkaClientID:          "pika-port-kafka",
		KafkaStreamMode:        "dual",
		KafkaTopicSnapshot:     "pika.snapshot",
		KafkaTopicBinlog:       "pika.binlog",
		KafkaTopicSingle:       "pika.stream",
		KafkaMessageMaxBytes:   1000000,
		KafkaEnableIdempotence: true,

		WaitBgsaveTimeout: 1800 * time.Second,

		BinlogWorkers:          4,
		BinlogQueueSize:        4096,
		WindowSlots:            64,
		SenderBatchSize:        256,
		BinlogProgressInterval: 5 * time.Second,

		PBAckDelayWarn: 10 * time.Second,
		PBIdleTimeout:  30 * time.Second,

		WalDir:            "./wal/",
		WalSegmentBytes:   256 * 1024 * 1024,
		WalSyncEvery:      1,
		WalRetainSegments: 2,

		SnapshotBatchNum:     512,
		SnapshotWorkers:      4,
		SnapshotReaderMode:   "type",
		SnapshotCgoBatch:     64,
		SnapshotScanBatch:    0,
		SnapshotScanStrategy: "scan",
		SnapshotListTailN:    0,

		ArgsEncoding:    event.PayloadEncodingBase64,
		RawRespEncoding: event.PayloadEncodingBase64,
		IncludeRawResp:  true,

		StartFromMaster: false,
	}
}

func LoadFile(path string, cfg *Config) (LoadResult, error) {
	var result LoadResult
	if path == "" {
		return result, nil
	}
	f, err := os.Open(path)
	if err != nil {
		result.Warnings = append(result.Warnings, "config file not found: "+path)
		return result, err
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	lineno := 0
	for scanner.Scan() {
		lineno++
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") || strings.HasPrefix(line, ";") {
			continue
		}
		pos := strings.IndexByte(line, '=')
		if pos < 0 {
			result.Warnings = append(result.Warnings, fmt.Sprintf("config parse error line %d: missing '='", lineno))
			continue
		}
		key := strings.TrimSpace(line[:pos])
		value := strings.TrimSpace(line[pos+1:])
		if key == "" {
			result.Warnings = append(result.Warnings, fmt.Sprintf("config parse error line %d: empty key", lineno))
			continue
		}
		if err := applyValue(key, value, cfg, &result); err != nil {
			result.Warnings = append(result.Warnings,
				fmt.Sprintf("config parse error line %d: %s", lineno, key))
		}
	}
	if err := scanner.Err(); err != nil {
		return result, err
	}
	return result, nil
}

func Apply(key, value string, cfg *Config, result *LoadResult) error {
	return applyValue(key, value, cfg, result)
}

func applyValue(key, value string, cfg *Config, result *LoadResult) error {
	lower := strings.ToLower(key)

	switch lower {
	case "filter":
		cfg.FilterGroupSpecs = append(cfg.FilterGroupSpecs, value)
		return nil
	case "exclude", "exclude_keys":
		cfg.FilterExcludeSpecs = append(cfg.FilterExcludeSpecs, value)
		return nil
	case "sync_protocol":
		result.Warnings = append(result.Warnings, "sync_protocol ignored in Go version (PB only)")
		return nil
	}

	var (
		i64 int64
		u64 uint64
		b   bool
		err error
	)
	switch lower {
	case "local_ip":
		cfg.LocalIP = value
	case "local_port":
		i64, err = parseInt64(value)
		if err != nil || i64 < 0 {
			return fmt.Errorf("invalid local_port")
		}
		cfg.LocalPort = int(i64)
	case "master_ip":
		cfg.MasterIP = value
	case "master_port":
		i64, err = parseInt64(value)
		if err != nil || i64 < 0 {
			return fmt.Errorf("invalid master_port")
		}
		cfg.MasterPort = int(i64)
	case "passwd", "password", "master_password":
		cfg.Passwd = value
	case "filenum":
		u64, err = strconv.ParseUint(value, 10, 32)
		if err != nil {
			return fmt.Errorf("invalid filenum")
		}
		v := uint32(u64)
		cfg.StartFilenum = &v
		if result != nil {
			result.FilenumSpecified = true
		}
	case "offset":
		u64, err = strconv.ParseUint(value, 10, 64)
		if err != nil {
			return fmt.Errorf("invalid offset")
		}
		cfg.StartOffset = &u64
		if result != nil {
			result.OffsetSpecified = true
		}
	case "log_path":
		cfg.LogPath = normalizePath(value)
	case "dump_path":
		cfg.DumpPath = normalizePath(value)
	case "checkpoint_path":
		cfg.CheckpointPath = value
	case "kafka_brokers":
		cfg.KafkaBrokers = value
	case "kafka_client_id":
		cfg.KafkaClientID = value
	case "kafka_stream_mode":
		cfg.KafkaStreamMode = value
	case "kafka_topic_snapshot":
		cfg.KafkaTopicSnapshot = value
	case "kafka_topic_binlog":
		cfg.KafkaTopicBinlog = value
	case "kafka_topic_single":
		cfg.KafkaTopicSingle = value
	case "kafka_message_max_bytes":
		i64, err = parseInt64(value)
		if err != nil || i64 <= 0 {
			return fmt.Errorf("invalid kafka_message_max_bytes")
		}
		cfg.KafkaMessageMaxBytes = int(i64)
	case "kafka_enable_idempotence":
		b, err = parseBool(value)
		if err != nil {
			return err
		}
		cfg.KafkaEnableIdempotence = b
	case "binlog_workers":
		i64, err = parseInt64(value)
		if err != nil || i64 <= 0 {
			return fmt.Errorf("invalid binlog_workers")
		}
		cfg.BinlogWorkers = int(i64)
	case "binlog_queue_size":
		i64, err = parseInt64(value)
		if err != nil || i64 <= 0 {
			return fmt.Errorf("invalid binlog_queue_size")
		}
		cfg.BinlogQueueSize = int(i64)
	case "window_slots":
		i64, err = parseInt64(value)
		if err != nil || i64 <= 0 {
			return fmt.Errorf("invalid window_slots")
		}
		cfg.WindowSlots = int(i64)
	case "sender_batch_size":
		i64, err = parseInt64(value)
		if err != nil || i64 <= 0 {
			return fmt.Errorf("invalid sender_batch_size")
		}
		cfg.SenderBatchSize = int(i64)
	case "binlog_progress_interval", "binlog_progress_interval_sec":
		i64, err = parseInt64(value)
		if err != nil {
			return fmt.Errorf("invalid binlog_progress_interval")
		}
		if i64 <= 0 {
			cfg.BinlogProgressInterval = 0
		} else {
			cfg.BinlogProgressInterval = time.Duration(i64) * time.Second
		}
	case "snapshot_batch_num":
		i64, err = parseInt64(value)
		if err != nil || i64 <= 0 {
			return fmt.Errorf("invalid snapshot_batch_num")
		}
		cfg.SnapshotBatchNum = int(i64)
	case "snapshot_workers":
		i64, err = parseInt64(value)
		if err != nil || i64 <= 0 {
			return fmt.Errorf("invalid snapshot_workers")
		}
		cfg.SnapshotWorkers = int(i64)
	case "snapshot_reader_mode":
		cfg.SnapshotReaderMode = value
	case "snapshot_reader_types":
		cfg.SnapshotReaderTypes = splitList(value)
	case "snapshot_reader_prefixes", "snapshot_reader_prefix":
		cfg.SnapshotReaderPrefixes = splitList(value)
	case "snapshot_cgo_batch":
		i64, err = parseInt64(value)
		if err != nil || i64 <= 0 {
			return fmt.Errorf("invalid snapshot_cgo_batch")
		}
		cfg.SnapshotCgoBatch = int(i64)
	case "snapshot_scan_batch":
		i64, err = parseInt64(value)
		if err != nil {
			return fmt.Errorf("invalid snapshot_scan_batch")
		}
		if i64 <= 0 {
			cfg.SnapshotScanBatch = 0
		} else {
			cfg.SnapshotScanBatch = i64
		}
	case "snapshot_scan_strategy":
		cfg.SnapshotScanStrategy = value
	case "snapshot_list_tail_n":
		i64, err = parseInt64(value)
		if err != nil || i64 < 0 {
			return fmt.Errorf("invalid snapshot_list_tail_n")
		}
		cfg.SnapshotListTailN = i64
	case "pb_ack_delay_warn", "pb_ack_delay_warn_sec":
		i64, err = parseInt64(value)
		if err != nil {
			return fmt.Errorf("invalid pb_ack_delay_warn")
		}
		if i64 <= 0 {
			cfg.PBAckDelayWarn = 0
		} else {
			cfg.PBAckDelayWarn = time.Duration(i64) * time.Second
		}
	case "pb_idle_timeout", "pb_idle_timeout_sec":
		i64, err = parseInt64(value)
		if err != nil {
			return fmt.Errorf("invalid pb_idle_timeout")
		}
		if i64 <= 0 {
			cfg.PBIdleTimeout = 0
		} else {
			cfg.PBIdleTimeout = time.Duration(i64) * time.Second
		}
	case "wait_bgsave_timeout":
		i64, err = parseInt64(value)
		if err != nil || i64 < 0 {
			return fmt.Errorf("invalid wait_bgsave_timeout")
		}
		cfg.WaitBgsaveTimeout = time.Duration(i64) * time.Second
	case "wal_dir":
		cfg.WalDir = normalizePath(value)
	case "wal_segment_bytes":
		i64, err = parseInt64(value)
		if err != nil || i64 <= 0 {
			return fmt.Errorf("invalid wal_segment_bytes")
		}
		cfg.WalSegmentBytes = i64
	case "wal_sync_every":
		i64, err = parseInt64(value)
		if err != nil || i64 <= 0 {
			return fmt.Errorf("invalid wal_sync_every")
		}
		cfg.WalSyncEvery = int(i64)
	case "wal_retain_segments":
		i64, err = parseInt64(value)
		if err != nil || i64 <= 0 {
			return fmt.Errorf("invalid wal_retain_segments")
		}
		cfg.WalRetainSegments = int(i64)
	case "args_encoding":
		enc, err := parsePayloadEncoding(value)
		if err != nil {
			return err
		}
		cfg.ArgsEncoding = enc
	case "raw_resp_encoding":
		enc, err := parsePayloadEncoding(value)
		if err != nil {
			return err
		}
		cfg.RawRespEncoding = enc
	case "include_raw_resp":
		b, err = parseBool(value)
		if err != nil {
			return err
		}
		cfg.IncludeRawResp = b
	case "start_from_master":
		b, err = parseBool(value)
		if err != nil {
			return err
		}
		cfg.StartFromMaster = b
	case "source_id":
		cfg.SourceID = value
	case "db_name":
		cfg.DBName = value
	default:
		if result != nil {
			result.Warnings = append(result.Warnings, "Unknown config key: "+key)
		}
	}
	return nil
}

func normalizePath(path string) string {
	if path == "" {
		return path
	}
	if strings.HasSuffix(path, "/") {
		return path
	}
	return path + "/"
}

func parseInt64(value string) (int64, error) {
	v, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return 0, err
	}
	return v, nil
}

func parseBool(value string) (bool, error) {
	lower := strings.ToLower(value)
	switch lower {
	case "1", "true", "yes", "y":
		return true, nil
	case "0", "false", "no", "n":
		return false, nil
	default:
		return false, fmt.Errorf("invalid bool: %s", value)
	}
}

func parsePayloadEncoding(value string) (event.PayloadEncoding, error) {
	lower := strings.ToLower(value)
	switch lower {
	case "base64", "b64":
		return event.PayloadEncodingBase64, nil
	case "none", "raw":
		return event.PayloadEncodingNone, nil
	default:
		return event.PayloadEncodingNone, fmt.Errorf("invalid payload encoding: %s", value)
	}
}

func splitList(value string) []string {
	if value == "" {
		return nil
	}
	parts := strings.Split(value, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		item := strings.TrimSpace(part)
		if item == "" {
			continue
		}
		out = append(out, item)
	}
	return out
}
