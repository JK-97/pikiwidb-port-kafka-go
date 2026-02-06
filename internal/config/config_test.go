package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLoadFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "pika.conf")
	content := strings.Join([]string{
		"# comment",
		"master_ip=10.0.0.1",
		"master_port=9221",
		"dump_path=./dump",
		"filter=key=dev:*;type=list;action=lpush",
		"exclude=tmp:*",
		"sync_protocol=legacy",
		"unknown_key=1",
		"",
	}, "\n")
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	cfg := Default()
	result, err := LoadFile(path, &cfg)
	if err != nil {
		t.Fatalf("LoadFile: %v", err)
	}
	if cfg.MasterIP != "10.0.0.1" || cfg.MasterPort != 9221 {
		t.Fatalf("master=%s:%d", cfg.MasterIP, cfg.MasterPort)
	}
	if cfg.DumpPath != "./dump/" {
		t.Fatalf("dump_path=%q", cfg.DumpPath)
	}
	if len(cfg.FilterGroupSpecs) != 1 || len(cfg.FilterExcludeSpecs) != 1 {
		t.Fatalf("filters=%v excludes=%v", cfg.FilterGroupSpecs, cfg.FilterExcludeSpecs)
	}
	foundSyncWarn := false
	foundUnknownWarn := false
	for _, w := range result.Warnings {
		if strings.Contains(w, "sync_protocol ignored") {
			foundSyncWarn = true
		}
		if strings.Contains(w, "Unknown config key") {
			foundUnknownWarn = true
		}
	}
	if !foundSyncWarn {
		t.Fatalf("expected sync_protocol warning, got %v", result.Warnings)
	}
	if !foundUnknownWarn {
		t.Fatalf("expected unknown key warning, got %v", result.Warnings)
	}
}
