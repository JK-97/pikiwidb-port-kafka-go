# Usage (PB only)

## Run

Build first:

```bash
./scripts/build_go.sh
```

Run:

```bash
./bin/pika_port_go -C ./docs/pika-port-kafka.conf.example
```

Notes:

- This Go version is **PB only**. If your config contains `sync_protocol=...`, it will be **ignored** with a warning.
- Ports:
  - PB repl server: `master_port + 2000`
  - rsync2 service: `master_port + 10001`
- AckCP advances only after Kafka delivery success; DurableCP advances after WAL 持久化连续前缀。
- `source_id` 默认是 `<master_ip:master_port>`；`checkpoint.json` 的 `source_id` 必须与运行配置一致，否则会报 `source_id mismatch` 并拒绝加载。
- 若配置里显式指定 `filenum/offset`，将覆盖 `start_from_master` 的行为。

## 常用启动方式（手动）

### 1) 仅增量同步（从主库当前位点开始）

配置：

```
start_from_master=true
```

启动：

```bash
./bin/pika_port_go -C ./docs/pika-port-kafka.conf.example
```

### 2) 强制全量同步 → 自动切增量

关键点：**起始位点必须小于主库当前 binlog**，否则会出现 `kSyncPointLarger`。

推荐步骤：

1. 清理本地状态（可选但推荐）
   ```bash
   rm -f checkpoint.json checkpoint.json.tmp
   rm -rf wal rsync_dump
   ```

2. 确保配置：
   ```
   start_from_master=false
   ```

3. 写入“最小起点”的 checkpoint（让起点为 0，触发全量）：
   ```bash
   cat > checkpoint.json <<'EOF'
   {"version":1,"source_id":"<master_ip:port>","epoch":0,
    "ack_cp":{"seq":0,"filenum":0,"offset":0,"logic_id":0,"server_id":0,"term_id":0,"ts_ms":0},
    "durable_cp":{"seq":0,"filenum":0,"offset":0,"logic_id":0,"server_id":0,"term_id":0,"ts_ms":0,"wal_segment":0,"wal_offset":0},
    "wal":{"segment":0,"offset":0},
    "snapshot":{"state":"","bgsave_end_file_num":0,"bgsave_end_offset":0,"progress_file":"","progress_offset":0,"snapshot_seq":0}}
   EOF
   ```
   注意：`source_id` 必须与运行配置一致（默认是 `<master_ip:port>`）。

   可选替代方式：直接在配置文件中指定起点（`filenum` 与 `offset` 必须同时指定）：
   ```
   filenum=0
   offset=0
   ```

4. 启动：
   ```bash
   ./bin/pika_port_go -C ./docs/pika-port-kafka.conf.example
   ```

观察方式：
- 快照进度（每 5 秒）：`snapshot progress tag=running ...`
- 快照完成：`snapshot progress tag=done ...`
- 增量进度（间隔可配）：`binlog progress tag=running ...`
- 切增量后：会进入 binlogLoop（会有 `pb repl: enter binlog loop ...` 等日志）
- checkpoint 状态：
  ```bash
  cat checkpoint.json | sed 's/,/\\n/g' | rg 'snapshot|ack_cp|durable_cp'
  ```

常见错误：
- `kSyncPointLarger`：请求起点 **大于** 主库当前 binlog 位点。请清理/重置 checkpoint 或开启 `start_from_master=true`。
- `kSyncPointBePurged`：起点过旧，进入全量同步流程。

## Config

- Format: `key=value`
- Empty lines and lines starting with `#` or `;` are ignored.
- `filter=` is repeatable:
  - inline group: `filter=key=dev:*;type=list;action=rpush`
  - or a file path: `filter=/path/to/filters.conf` (non-empty, `#` comment lines ignored)
- `exclude=` / `exclude_keys=` are repeatable, comma-separated patterns.

See: `docs/pika-port-kafka.conf.example`.

## CLI 覆盖

- `-C /path/to/config`：指定配置文件。
- `-set key=value`：覆盖配置项（可重复）。
- `-F filter-spec-or-file`：追加过滤规则（可重复）。
- `-X exclude-patterns`：追加排除规则（可重复，逗号分隔）。
