package filter

import "testing"

func TestFilter_ShouldSend_ExcludeOnly(t *testing.T) {
	f, _ := Build(nil, []string{"tmp:*"})
	if f.ShouldSend("tmp:1", "string", "set") {
		t.Fatalf("expected excluded")
	}
	if !f.ShouldSend("prod:1", "string", "set") {
		t.Fatalf("expected allowed")
	}
}

func TestFilter_ShouldSend_GroupsOR(t *testing.T) {
	f, _ := Build(
		[]string{
			"key=dev:*;type=list;action=lpush",
			"key=prod:*;type=set;action=sadd",
		},
		nil,
	)
	if !f.ShouldSend("dev:k", "list", "lpush") {
		t.Fatalf("expected match group 1")
	}
	if f.ShouldSend("dev:k", "list", "rpush") {
		t.Fatalf("expected reject action mismatch")
	}
	if !f.ShouldSend("prod:k", "set", "SADD") {
		t.Fatalf("expected case-insensitive match")
	}
	if f.ShouldSend("other:k", "set", "sadd") {
		t.Fatalf("expected reject (no group match)")
	}
}

func TestFilter_ShouldSend_UnknownTypeAllowed(t *testing.T) {
	f, _ := Build([]string{"type=string"}, nil)
	if !f.ShouldSend("k", "unknown", "whatever") {
		t.Fatalf("unknown type should be allowed")
	}
}

func TestBuild_SkipInvalidGroup(t *testing.T) {
	f, warnings := Build([]string{"foo=bar"}, nil)
	if f != nil {
		t.Fatalf("expected nil filter")
	}
	if len(warnings) == 0 {
		t.Fatalf("expected warnings")
	}
}
