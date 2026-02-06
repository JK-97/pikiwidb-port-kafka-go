package snapshot

import (
	"strings"

	"github.com/OpenAtomFoundation/pikiwidb-port-kafka-go/internal/filter"
)

type readerSpec struct {
	typeMask uint32
	pattern  string
}

func buildReaderSpecs(mode string, types []string, prefixes []string, f *filter.Filter) ([]readerSpec, []string) {
	var warnings []string

	mode = strings.ToLower(strings.TrimSpace(mode))
	if mode == "" {
		mode = "single"
	}
	if mode != "single" && mode != "type" {
		warnings = append(warnings, "snapshot_reader_mode invalid, fallback to single")
		mode = "single"
	}

	typeMask, ok := typeMaskFromNames(types)
	if !ok {
		typeMask = deriveTypeMaskFromFilter(f)
	}
	if typeMask == 0 {
		typeMask = TypeAll
	}

	patterns := normalizePatterns(prefixes)
	if len(patterns) == 0 {
		if prefix, ok := derivePrefixFromFilter(f); ok {
			patterns = []string{normalizePattern(prefix)}
		}
	}
	if len(patterns) == 0 {
		patterns = []string{"*"}
	}

	var masks []uint32
	if mode == "type" {
		masks = expandTypeMasks(typeMask)
	} else {
		masks = []uint32{typeMask}
	}

	specs := make([]readerSpec, 0, len(masks)*len(patterns))
	for _, mask := range masks {
		for _, pattern := range patterns {
			specs = append(specs, readerSpec{typeMask: mask, pattern: pattern})
		}
	}
	return specs, warnings
}

func typeMaskFromNames(names []string) (uint32, bool) {
	if len(names) == 0 {
		return 0, false
	}
	mask := uint32(0)
	for _, raw := range names {
		name := strings.ToLower(strings.TrimSpace(raw))
		if name == "" || name == "auto" {
			continue
		}
		switch name {
		case "string", "strings":
			mask |= TypeStrings
		case "list", "lists":
			mask |= TypeLists
		case "hash", "hashes":
			mask |= TypeHashes
		case "set", "sets":
			mask |= TypeSets
		case "zset", "zsets":
			mask |= TypeZSets
		case "stream", "streams":
			mask |= TypeStreams
		default:
		}
	}
	if mask == 0 {
		return 0, false
	}
	return mask, true
}

func deriveTypeMaskFromFilter(f *filter.Filter) uint32 {
	if f == nil || len(f.Groups) == 0 {
		return TypeAll
	}
	mask := uint32(0)
	for _, group := range f.Groups {
		groupMask := uint32(0)
		if len(group.Types) == 0 && len(group.Actions) == 0 {
			return TypeAll
		}
		for _, t := range group.Types {
			switch strings.ToLower(t) {
			case "string", "strings":
				groupMask |= TypeStrings
			case "list", "lists":
				groupMask |= TypeLists
			case "hash", "hashes":
				groupMask |= TypeHashes
			case "set", "sets":
				groupMask |= TypeSets
			case "zset", "zsets":
				groupMask |= TypeZSets
			case "stream", "streams":
				groupMask |= TypeStreams
			}
		}
		if groupMask == 0 && len(group.Actions) > 0 {
			for _, action := range group.Actions {
				switch strings.ToLower(action) {
				case "set":
					groupMask |= TypeStrings
				case "rpush", "lpush":
					groupMask |= TypeLists
				case "hmset", "hset":
					groupMask |= TypeHashes
				case "sadd":
					groupMask |= TypeSets
				case "zadd":
					groupMask |= TypeZSets
				case "xadd":
					groupMask |= TypeStreams
				case "expire":
					groupMask = TypeAll
				default:
					groupMask = TypeAll
				}
			}
		}
		if groupMask == 0 {
			groupMask = TypeAll
		}
		if groupMask == TypeAll {
			return TypeAll
		}
		mask |= groupMask
	}
	if mask == 0 {
		return TypeAll
	}
	return mask
}

func derivePrefixFromFilter(f *filter.Filter) (string, bool) {
	if f == nil || len(f.Groups) == 0 {
		return "", false
	}
	common := ""
	for _, group := range f.Groups {
		if len(group.KeyRegex) > 0 {
			return "", false
		}
		if len(group.KeyFast) == 0 {
			return "", false
		}
		groupPrefix := ""
		for _, matcher := range group.KeyFast {
			part := matcher.Text
			if part == "" {
				continue
			}
			if groupPrefix == "" {
				groupPrefix = part
			} else {
				groupPrefix = commonPrefix(groupPrefix, part)
			}
			if groupPrefix == "" {
				break
			}
		}
		if groupPrefix == "" {
			return "", false
		}
		if common == "" {
			common = groupPrefix
		} else {
			common = commonPrefix(common, groupPrefix)
		}
		if common == "" {
			return "", false
		}
	}
	return common, common != ""
}

func commonPrefix(a, b string) string {
	max := len(a)
	if len(b) < max {
		max = len(b)
	}
	i := 0
	for i < max && a[i] == b[i] {
		i++
	}
	return a[:i]
}

func normalizePatterns(prefixes []string) []string {
	var patterns []string
	for _, raw := range prefixes {
		p := strings.TrimSpace(raw)
		if p == "" || strings.EqualFold(p, "auto") {
			continue
		}
		patterns = append(patterns, normalizePattern(p))
	}
	return patterns
}

func normalizePattern(prefix string) string {
	if prefix == "" {
		return "*"
	}
	if strings.Contains(prefix, "*") {
		return prefix
	}
	return prefix + "*"
}

func expandTypeMasks(mask uint32) []uint32 {
	if mask == 0 || mask == TypeAll {
		return []uint32{TypeStrings, TypeLists, TypeHashes, TypeSets, TypeZSets, TypeStreams}
	}
	var out []uint32
	if mask&TypeStrings != 0 {
		out = append(out, TypeStrings)
	}
	if mask&TypeLists != 0 {
		out = append(out, TypeLists)
	}
	if mask&TypeHashes != 0 {
		out = append(out, TypeHashes)
	}
	if mask&TypeSets != 0 {
		out = append(out, TypeSets)
	}
	if mask&TypeZSets != 0 {
		out = append(out, TypeZSets)
	}
	if mask&TypeStreams != 0 {
		out = append(out, TypeStreams)
	}
	if len(out) == 0 {
		return []uint32{TypeStrings, TypeLists, TypeHashes, TypeSets, TypeZSets, TypeStreams}
	}
	return out
}
