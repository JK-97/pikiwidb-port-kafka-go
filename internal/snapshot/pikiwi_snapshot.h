#pragma once

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct SnapshotRecord {
  char* data_type;
  size_t data_type_len;
  char* key;
  size_t key_len;
  char* raw_resp;
  size_t raw_resp_len;
} SnapshotRecord;

typedef struct SnapshotOptions {
  int batch_num;
  int64_t scan_batch;
  const char* scan_pattern;
  uint32_t type_mask;
  int scan_strategy;
  int64_t list_tail_n;
} SnapshotOptions;

enum SnapshotScanStrategy {
  SNAPSHOT_SCAN_CURSOR = 0,
  SNAPSHOT_SCAN_STARTKEY = 1,
};

enum SnapshotTypeMask {
  SNAPSHOT_TYPE_STRINGS = 1u << 0,
  SNAPSHOT_TYPE_LISTS = 1u << 1,
  SNAPSHOT_TYPE_HASHES = 1u << 2,
  SNAPSHOT_TYPE_SETS = 1u << 3,
  SNAPSHOT_TYPE_ZSETS = 1u << 4,
  SNAPSHOT_TYPE_STREAMS = 1u << 5,
  SNAPSHOT_TYPE_ALL = 0xFFFFFFFFu,
};

typedef struct SnapshotIter SnapshotIter;

SnapshotIter* snapshot_open(const char* dump_path, const char* db_name, SnapshotOptions opts, char** err);
int snapshot_next(SnapshotIter* it, SnapshotRecord* out, char** err);
int snapshot_next_batch(SnapshotIter* it, SnapshotRecord** out, size_t* count, int max_count, char** err);
void snapshot_free_record(SnapshotRecord* rec);
void snapshot_free_batch(SnapshotRecord* recs, size_t count);
void snapshot_close(SnapshotIter* it);

#ifdef __cplusplus
}
#endif
