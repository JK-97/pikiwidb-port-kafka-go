package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	ckpt "github.com/OpenAtomFoundation/pikiwidb-port-kafka-go/internal/checkpoint"
	"github.com/OpenAtomFoundation/pikiwidb-port-kafka-go/internal/config"
	"github.com/OpenAtomFoundation/pikiwidb-port-kafka-go/internal/event"
	"github.com/OpenAtomFoundation/pikiwidb-port-kafka-go/internal/filter"
	"github.com/OpenAtomFoundation/pikiwidb-port-kafka-go/internal/info"
	"github.com/OpenAtomFoundation/pikiwidb-port-kafka-go/internal/kafka"
	"github.com/OpenAtomFoundation/pikiwidb-port-kafka-go/internal/repl"
	"github.com/OpenAtomFoundation/pikiwidb-port-kafka-go/internal/snapshot"
	"github.com/OpenAtomFoundation/pikiwidb-port-kafka-go/internal/wal"
)

type multiStringFlag []string

func (f *multiStringFlag) String() string { return strings.Join(*f, ",") }
func (f *multiStringFlag) Set(v string) error {
	*f = append(*f, v)
	return nil
}

type multiKVFlag []string

func (f *multiKVFlag) String() string { return strings.Join(*f, ",") }
func (f *multiKVFlag) Set(v string) error {
	*f = append(*f, v)
	return nil
}

func main() {
	var (
		configPath  string
		overrideKVs multiKVFlag
		filterArgs  multiStringFlag
		excludeArgs multiStringFlag
	)

	flag.StringVar(&configPath, "C", "", "config file path")
	flag.StringVar(&configPath, "config", "", "config file path")
	flag.Var(&overrideKVs, "set", "override config key=value (repeatable)")
	flag.Var(&filterArgs, "F", "filter group spec or filter file (repeatable)")
	flag.Var(&excludeArgs, "X", "exclude keys (comma separated, repeatable)")
	flag.Parse()

	logger := log.New(os.Stderr, "", log.LstdFlags)

	cfg := config.Default()
	loadRes, err := config.LoadFile(configPath, &cfg)
	if err != nil && configPath != "" {
		logger.Printf("config load failed path=%s err=%v", configPath, err)
		os.Exit(2)
	}

	for _, kv := range overrideKVs {
		key, val, ok := strings.Cut(kv, "=")
		if !ok {
			loadRes.Warnings = append(loadRes.Warnings, "override skipped (missing '='): "+kv)
			continue
		}
		if err := config.Apply(key, val, &cfg, &loadRes); err != nil {
			loadRes.Warnings = append(loadRes.Warnings, "override error: "+key)
		}
	}

	filterGroupArgs := append([]string(nil), cfg.FilterGroupSpecs...)
	filterGroupArgs = append(filterGroupArgs, filterArgs...)
	filterExcludeArgs := append([]string(nil), cfg.FilterExcludeSpecs...)
	filterExcludeArgs = append(filterExcludeArgs, excludeArgs...)

	filterGroupSpecs, filterWarnings := expandFilterGroupArgs(filterGroupArgs)
	filterObj, buildWarnings := filter.Build(filterGroupSpecs, filterExcludeArgs)
	loadRes.Warnings = append(loadRes.Warnings, filterWarnings...)
	loadRes.Warnings = append(loadRes.Warnings, buildWarnings...)

	for _, w := range loadRes.Warnings {
		logger.Printf("WARN %s", w)
	}

	if cfg.MasterIP == "" || cfg.MasterPort == 0 {
		logger.Printf("master_ip/master_port required")
		os.Exit(2)
	}
	if cfg.KafkaBrokers == "" {
		logger.Printf("kafka_brokers required")
		os.Exit(2)
	}

	if cfg.SourceID == "" {
		cfg.SourceID = cfg.MasterIP + ":" + strconv.Itoa(cfg.MasterPort)
	}

	filenumSpecified := loadRes.FilenumSpecified
	offsetSpecified := loadRes.OffsetSpecified
	if filenumSpecified != offsetSpecified {
		logger.Printf("filenum/offset must be specified together")
		os.Exit(2)
	}
	if (filenumSpecified || offsetSpecified) && cfg.StartFromMaster {
		logger.Printf("WARN start_from_master ignored because filenum/offset specified")
		cfg.StartFromMaster = false
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	ckptMgr := ckpt.NewManager(cfg.CheckpointPath, cfg.SourceID)

	if cfg.StartFromMaster {
		ri, err := info.FetchReplicationInfo(ctx, cfg.MasterIP, cfg.MasterPort, cfg.Passwd, 1500*time.Millisecond)
		if err != nil || !ri.HasBinlogOffset {
			logger.Printf("failed to fetch master binlog_offset: %v", err)
			os.Exit(2)
		}
		_ = ckptMgr.SetAckPosition(ckpt.Position{Seq: 0, Filenum: ri.Filenum, Offset: ri.Offset})
		logger.Printf("Start from master binlog_offset=%d:%d", ri.Filenum, ri.Offset)
		time.Sleep(time.Second)
	} else if filenumSpecified {
		start := ckpt.Position{Seq: 0, Filenum: *cfg.StartFilenum, Offset: *cfg.StartOffset}
		_ = ckptMgr.SetAckPosition(start)
		if start.Filenum == 0 && start.Offset == 0 {
			logger.Printf("WARN start offset forced to 0: full sync (DBSync/rsync) may be triggered")
		}
	}

	producer, err := kafka.NewProducer(kafka.ProducerConfig{
		Brokers:           cfg.KafkaBrokers,
		ClientID:          cfg.KafkaClientID,
		MessageMaxBytes:   cfg.KafkaMessageMaxBytes,
		EnableIdempotence: cfg.KafkaEnableIdempotence,
	}, ckptMgr, logger)
	if err != nil {
		logger.Printf("kafka producer init failed: %v", err)
		os.Exit(2)
	}
	defer producer.Close()

	go producer.RunDeliveryLoop(ctx)
	walWriter, err := wal.OpenWriter(wal.Config{
		Dir:            cfg.WalDir,
		SegmentBytes:   cfg.WalSegmentBytes,
		SyncEvery:      cfg.WalSyncEvery,
		RetainSegments: cfg.WalRetainSegments,
		AckPosProvider: func() (uint32, uint64) {
			if pos, ok := ckptMgr.Ack(); ok {
				return pos.Filenum, pos.Offset
			}
			return 0, 0
		},
	})
	if err != nil {
		logger.Printf("wal init failed: %v", err)
		os.Exit(2)
	}
	defer walWriter.Close()

	snapRunner := &snapshot.Runner{
		DBName:        cfg.DBName,
		SourceID:      cfg.SourceID,
		StreamMode:    cfg.KafkaStreamMode,
		TopicSnapshot: cfg.KafkaTopicSnapshot,
		TopicSingle:   cfg.KafkaTopicSingle,
		Filter:        filterObj,
		Producer:      producer,
		Parser:        snapshot.NewRDBParser(),
		Builder: event.Builder{
			ArgsEncoding:    cfg.ArgsEncoding,
			RawRespEncoding: cfg.RawRespEncoding,
			IncludeRawResp:  cfg.IncludeRawResp,
		},
		Workers:         cfg.SnapshotWorkers,
		BatchNum:        cfg.SnapshotBatchNum,
		CgoBatch:        cfg.SnapshotCgoBatch,
		ScanBatch:       cfg.SnapshotScanBatch,
		ScanStrategy:    snapshot.ParseScanStrategy(cfg.SnapshotScanStrategy),
		ReaderMode:      cfg.SnapshotReaderMode,
		ReaderTypes:     cfg.SnapshotReaderTypes,
		ReaderPrefixes:  cfg.SnapshotReaderPrefixes,
		WindowSlots:     cfg.WindowSlots,
		SenderBatchSize: cfg.SenderBatchSize,
		ListTailN:       cfg.SnapshotListTailN,
		Ckpt:            ckptMgr,
		Log:             logger,
	}

	client := &repl.Client{
		MasterHost:             cfg.MasterIP,
		MasterPort:             cfg.MasterPort,
		Password:               cfg.Passwd,
		LocalIP:                cfg.LocalIP,
		LocalPort:              cfg.LocalPort,
		DBName:                 cfg.DBName,
		DumpPath:               cfg.DumpPath,
		StreamMode:             cfg.KafkaStreamMode,
		TopicSnapshot:          cfg.KafkaTopicSnapshot,
		TopicBinlog:            cfg.KafkaTopicBinlog,
		TopicSingle:            cfg.KafkaTopicSingle,
		BinlogWorkers:          cfg.BinlogWorkers,
		BinlogQueueSize:        cfg.BinlogQueueSize,
		WindowSlots:            cfg.WindowSlots,
		SenderBatchSize:        cfg.SenderBatchSize,
		BinlogProgressInterval: cfg.BinlogProgressInterval,
		AckDelayWarn:           cfg.PBAckDelayWarn,
		IdleTimeout:            cfg.PBIdleTimeout,
		WaitBgsaveTimeout:      cfg.WaitBgsaveTimeout,
		Filter:                 filterObj,
		Builder: event.Builder{
			ArgsEncoding:    cfg.ArgsEncoding,
			RawRespEncoding: cfg.RawRespEncoding,
			IncludeRawResp:  cfg.IncludeRawResp,
		},
		Producer:      producer,
		Ckpt:          ckptMgr,
		Wal:           walWriter,
		Rsync2Timeout: 30 * time.Second,
		Snapshot:      snapRunner,
		Log:           logger,
	}

	if err := client.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		logger.Printf("pb repl exited: %v", err)
		os.Exit(1)
	}
}

func expandFilterGroupArgs(args []string) (specs []string, warnings []string) {
	for _, arg := range args {
		trimmed := strings.TrimSpace(arg)
		if trimmed == "" {
			continue
		}
		if strings.Contains(trimmed, "=") {
			specs = append(specs, trimmed)
			continue
		}
		data, err := os.ReadFile(trimmed)
		if err != nil {
			warnings = append(warnings, "filter file not found: "+trimmed)
			continue
		}
		lines := strings.Split(string(data), "\n")
		for _, line := range lines {
			line = strings.TrimSpace(strings.TrimSuffix(line, "\r"))
			if line == "" || strings.HasPrefix(line, "#") {
				continue
			}
			specs = append(specs, line)
		}
	}
	return specs, warnings
}
