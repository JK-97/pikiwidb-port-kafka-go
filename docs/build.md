# Build (Go)

This repo builds a **PB-only** pika-port-kafka implementation:

- `bin/pika_port_go` (Go main program)

## Prerequisites

- PikiwiDB source tree at `/tmp/pikiwidb` (used by snapshot parser Cgo build).
- PikiwiDB deps at `/tmp/pikiwidb/deps` (rocksdb/glog/etc).
- `librdkafka` development files available for `confluent-kafka-go`.
- `protoc` + `protoc-gen-go` only needed when regenerating `proto/*.proto`.
- Go toolchain (matches `go.mod` toolchain, currently `go1.24.9`).

### Environment dependencies

- GCC/G++ >= 9（C++17）
- CMake >= 3.18
- make
- librdkafka（开发包）
- protobuf（protoc + libprotobuf）
- 可选：libunwind / gperftools（若 PikiwiDB 构建默认启用）

> 说明：glog/gflags/rocksdb 等由 PikiwiDB 的 `deps` 目录提供，系统无需单独安装。

示例（Ubuntu）：

```bash
sudo apt-get update
sudo apt-get install -y build-essential cmake librdkafka-dev libprotobuf-dev protobuf-compiler \
  libunwind-dev libgoogle-perftools-dev
```

示例（CentOS/Rocky）：

```bash
yum install -y gcc gcc-c++ make cmake librdkafka-devel protobuf-devel protobuf-compiler \
  libunwind-devel gperftools-devel
```

## Build

```bash
cd /workspaces/pikiwidb-port-kafka-go
./scripts/build_go.sh
```

Artifacts:

- `bin/pika_port_go`

## Build all (include PikiwiDB deps)

One-click build (clone + build deps + build Go):

```bash
./scripts/build_all.sh --clone
```

If you already have `/tmp/pikiwidb`, just run:

```bash
./scripts/build_all.sh
```

### Custom paths

The build script supports overriding default paths:

- `OUT_DIR` (default: `./bin`)

Example:

```bash
PIKIWIDB_ROOT=/tmp/pikiwidb OUT_DIR=/tmp/out ./scripts/build_go.sh
```

## Regenerate protobuf (optional)

```bash
./scripts/gen_go_proto.sh
```
