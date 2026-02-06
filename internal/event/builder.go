package event

import (
	"strconv"
	"time"
)

type BinlogItem struct {
	ExecTime uint32
	TermID   uint32
	LogicID  uint64
	Filenum  uint32
	Offset   uint64
}

type Builder struct {
	ArgsEncoding    PayloadEncoding
	RawRespEncoding PayloadEncoding
	IncludeRawResp  bool

	NowMillis func() uint64
}

func (b Builder) nowMillis() uint64 {
	if b.NowMillis != nil {
		return b.NowMillis()
	}
	return uint64(time.Now().UnixMilli())
}

func (b Builder) BuildSnapshotEventJSON(argv [][]byte, dbName, dataType, sourceID string, rawResp []byte, key []byte) []byte {
	var out []byte
	out = append(out, '{')
	first := true
	out, first = appendJSONQuotedString(out, "event_type", []byte("snapshot"), first)
	if len(argv) > 0 {
		out, first = appendJSONQuotedString(out, "op", toLowerBytes(argv[0]), first)
	}
	out, first = appendJSONQuotedString(out, "data_type", []byte(dataType), first)
	out, first = appendJSONQuotedString(out, "db", []byte(dbName), first)
	out, first = appendJSONNumber(out, "slot", 0, first)
	out, first = appendJSONQuotedString(out, "key", key, first)

	out, first = appendJSONRaw(out, "args", b.buildArgsJSON(argv), first)
	out, first = appendJSONQuotedString(out, "args_encoding", []byte(b.ArgsEncoding.String()), first)

	if b.IncludeRawResp {
		encoded := EncodePayload(rawResp, b.RawRespEncoding)
		out, first = appendJSONQuotedString(out, "raw_resp", encoded, first)
		out, first = appendJSONQuotedString(out, "raw_resp_encoding", []byte(b.RawRespEncoding.String()), first)
	}

	out, first = appendJSONNumber(out, "ts_ms", b.nowMillis(), first)

	eventID := make([]byte, 0, len("snapshot:")+len(dbName)+1+len(dataType)+1+len(key))
	eventID = append(eventID, "snapshot:"...)
	eventID = append(eventID, dbName...)
	eventID = append(eventID, ':')
	eventID = append(eventID, dataType...)
	eventID = append(eventID, ':')
	eventID = append(eventID, key...)
	out, first = appendJSONQuotedString(out, "event_id", eventID, first)

	out, first = appendJSONQuotedString(out, "source_id", []byte(sourceID), first)
	out, first = appendSourceObject(out, sourceID, first)

	out = append(out, '}')
	return out
}

func (b Builder) BuildBinlogEventJSON(argv [][]byte, item BinlogItem, dbName, dataType, sourceID string, rawResp []byte, key []byte) []byte {
	var out []byte
	out = append(out, '{')
	first := true
	out, first = appendJSONQuotedString(out, "event_type", []byte("binlog"), first)
	if len(argv) > 0 {
		out, first = appendJSONQuotedString(out, "op", toLowerBytes(argv[0]), first)
	}
	out, first = appendJSONQuotedString(out, "data_type", []byte(dataType), first)
	out, first = appendJSONQuotedString(out, "db", []byte(dbName), first)
	out, first = appendJSONNumber(out, "slot", 0, first)
	out, first = appendJSONQuotedString(out, "key", key, first)

	out, first = appendJSONRaw(out, "args", b.buildArgsJSON(argv), first)
	out, first = appendJSONQuotedString(out, "args_encoding", []byte(b.ArgsEncoding.String()), first)

	if b.IncludeRawResp {
		encoded := EncodePayload(rawResp, b.RawRespEncoding)
		out, first = appendJSONQuotedString(out, "raw_resp", encoded, first)
		out, first = appendJSONQuotedString(out, "raw_resp_encoding", []byte(b.RawRespEncoding.String()), first)
	}

	out, first = appendJSONNumber(out, "ts_ms", uint64(item.ExecTime)*1000, first)

	eventID := strconv.FormatUint(uint64(item.TermID), 10) + ":" +
		strconv.FormatUint(uint64(item.Filenum), 10) + ":" +
		strconv.FormatUint(item.Offset, 10) + ":" +
		strconv.FormatUint(item.LogicID, 10)
	out, first = appendJSONQuotedString(out, "event_id", []byte(eventID), first)
	out, first = appendJSONQuotedString(out, "source_id", []byte(sourceID), first)

	out = append(out, ',', '"', 'b', 'i', 'n', 'l', 'o', 'g', '"', ':', '{')
	binlogFirst := true
	out, binlogFirst = appendJSONNumber(out, "filenum", uint64(item.Filenum), binlogFirst)
	out, binlogFirst = appendJSONNumber(out, "offset", item.Offset, binlogFirst)
	out, binlogFirst = appendJSONNumber(out, "logic_id", item.LogicID, binlogFirst)
	out, binlogFirst = appendJSONNumber(out, "server_id", 0, binlogFirst)
	out, binlogFirst = appendJSONNumber(out, "term_id", uint64(item.TermID), binlogFirst)
	out = append(out, '}')

	out, first = appendSourceObject(out, sourceID, first)

	out = append(out, '}')
	return out
}

func (b Builder) buildArgsJSON(argv [][]byte) []byte {
	out := make([]byte, 0, 32*len(argv))
	out = append(out, '[')
	for i, arg := range argv {
		if i > 0 {
			out = append(out, ',')
		}
		encoded := EncodePayload(arg, b.ArgsEncoding)
		out = append(out, '"')
		out = AppendJsonEscaped(out, encoded)
		out = append(out, '"')
	}
	out = append(out, ']')
	return out
}

func appendJSONQuotedString(dst []byte, key string, value []byte, first bool) ([]byte, bool) {
	if !first {
		dst = append(dst, ',')
	}
	first = false
	dst = append(dst, '"')
	dst = append(dst, key...)
	dst = append(dst, '"', ':', '"')
	dst = AppendJsonEscaped(dst, value)
	dst = append(dst, '"')
	return dst, first
}

func appendJSONNumber(dst []byte, key string, value uint64, first bool) ([]byte, bool) {
	if !first {
		dst = append(dst, ',')
	}
	first = false
	dst = append(dst, '"')
	dst = append(dst, key...)
	dst = append(dst, '"', ':')
	dst = strconv.AppendUint(dst, value, 10)
	return dst, first
}

func appendJSONRaw(dst []byte, key string, rawJSON []byte, first bool) ([]byte, bool) {
	if !first {
		dst = append(dst, ',')
	}
	first = false
	dst = append(dst, '"')
	dst = append(dst, key...)
	dst = append(dst, '"', ':')
	dst = append(dst, rawJSON...)
	return dst, first
}

func appendSourceObject(dst []byte, sourceID string, first bool) ([]byte, bool) {
	if !first {
		dst = append(dst, ',')
	}
	first = false
	dst = append(dst, '"', 's', 'o', 'u', 'r', 'c', 'e', '"', ':', '{')
	if host, port, ok := splitHostPort(sourceID); ok {
		dst = append(dst, '"', 'h', 'o', 's', 't', '"', ':', '"')
		dst = AppendJsonEscaped(dst, []byte(host))
		dst = append(dst, '"', ',', '"', 'p', 'o', 'r', 't', '"', ':')
		dst = append(dst, port...)
	} else {
		dst = append(dst, '"', 'h', 'o', 's', 't', '"', ':', '"')
		dst = AppendJsonEscaped(dst, []byte(sourceID))
		dst = append(dst, '"')
	}
	dst = append(dst, '}')
	return dst, first
}

func splitHostPort(sourceID string) (host string, port []byte, ok bool) {
	for i := 0; i < len(sourceID); i++ {
		if sourceID[i] == ':' {
			return sourceID[:i], []byte(sourceID[i+1:]), true
		}
	}
	return "", nil, false
}

func toLowerBytes(b []byte) []byte {
	out := make([]byte, len(b))
	for i, c := range b {
		if c >= 'A' && c <= 'Z' {
			out[i] = c + ('a' - 'A')
		} else {
			out[i] = c
		}
	}
	return out
}
