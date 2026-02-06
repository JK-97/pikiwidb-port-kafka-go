package event

import "testing"

func TestBuildPartitionKey(t *testing.T) {
	got := string(BuildPartitionKey("db0", "string", []byte("k")))
	if got != "db0:string:k" {
		t.Fatalf("got %q", got)
	}
	got = string(BuildPartitionKey("db0", "unknown", nil))
	if got != "db0:unknown" {
		t.Fatalf("got %q", got)
	}
}

func TestCommandDataType(t *testing.T) {
	tests := []struct {
		cmd  string
		want string
	}{
		{"SET", "string"},
		{"hset", "hash"},
		{"LPUSH", "list"},
		{"sadd", "set"},
		{"zadd", "zset"},
		{"flushdb", "unknown"},
	}
	for _, tt := range tests {
		if got := CommandDataType(tt.cmd); got != tt.want {
			t.Fatalf("cmd=%q got=%q want=%q", tt.cmd, got, tt.want)
		}
	}
}

func TestBuildBinlogEventJSON_Golden(t *testing.T) {
	builder := Builder{
		ArgsEncoding:    PayloadEncodingBase64,
		RawRespEncoding: PayloadEncodingBase64,
		IncludeRawResp:  true,
		NowMillis:       func() uint64 { return 123 },
	}

	argv := [][]byte{[]byte("SET"), []byte("k"), []byte("v")}
	item := BinlogItem{
		ExecTime: 1700000000,
		TermID:   7,
		LogicID:  8899,
		Filenum:  12,
		Offset:   34567,
	}
	raw := []byte("*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n")
	out := string(builder.BuildBinlogEventJSON(argv, item, "db0", "string", "10.0.0.1:9221", raw, []byte("k")))

	expected := `{"event_type":"binlog","op":"set","data_type":"string","db":"db0","slot":0,"key":"k","args":["U0VU","aw==","dg=="],"args_encoding":"base64","raw_resp":"KjMNCiQzDQpTRVQNCiQxDQprDQokMQ0Kdg0K","raw_resp_encoding":"base64","ts_ms":1700000000000,"event_id":"7:12:34567:8899","source_id":"10.0.0.1:9221","binlog":{"filenum":12,"offset":34567,"logic_id":8899,"server_id":0,"term_id":7},"source":{"host":"10.0.0.1","port":9221}}`

	if out != expected {
		t.Fatalf("unexpected JSON:\n got: %s\nwant: %s", out, expected)
	}
}
