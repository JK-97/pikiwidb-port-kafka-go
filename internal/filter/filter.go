package filter

import (
	"fmt"
	"regexp"
	"strings"
)

type KeyMatcherKind uint8

const (
	KeyMatcherExact KeyMatcherKind = iota
	KeyMatcherPrefix
	KeyMatcherRegex
)

type KeyMatcher struct {
	Kind KeyMatcherKind
	Raw  string
	Text string
	Re   *regexp.Regexp
}

func (m KeyMatcher) Match(key string) bool {
	switch m.Kind {
	case KeyMatcherExact:
		return key == m.Text
	case KeyMatcherPrefix:
		return strings.HasPrefix(key, m.Text)
	case KeyMatcherRegex:
		return m.Re != nil && m.Re.MatchString(key)
	default:
		return false
	}
}

type Group struct {
	KeyFast  []KeyMatcher
	KeyRegex []KeyMatcher
	Types    []string
	Actions  []string
}

type Filter struct {
	Groups       []Group
	ExcludeFast  []KeyMatcher
	ExcludeRegex []KeyMatcher
}

func Build(groupSpecs []string, excludeSpecs []string) (*Filter, []string) {
	var warnings []string
	var groups []Group
	var excludeFast []KeyMatcher
	var excludeRegex []KeyMatcher

	for _, spec := range groupSpecs {
		var group Group
		warning, ok := parseGroupSpec(spec, &group)
		if !ok {
			if warning != "" {
				warnings = append(warnings, "Filter group skipped: "+warning)
			}
			continue
		}
		if warning != "" {
			warnings = append(warnings, "Filter group notice: "+warning)
		}
		groups = append(groups, group)
	}

	for _, spec := range excludeSpecs {
		parseKeyList(spec, &excludeFast, &excludeRegex, &warnings)
	}

	if len(groups) == 0 && len(excludeFast) == 0 && len(excludeRegex) == 0 {
		return nil, warnings
	}
	return &Filter{
		Groups:       groups,
		ExcludeFast:  excludeFast,
		ExcludeRegex: excludeRegex,
	}, warnings
}

func (f *Filter) ShouldSend(key, dataType, action string) bool {
	if f == nil {
		return true
	}
	if matchKeys(key, f.ExcludeFast, f.ExcludeRegex) {
		return false
	}
	if len(f.Groups) == 0 {
		return true
	}
	for _, group := range f.Groups {
		keyOK := len(group.KeyFast) == 0 && len(group.KeyRegex) == 0 ||
			matchKeys(key, group.KeyFast, group.KeyRegex)
		typeOK := matchValues(dataType, group.Types, true)
		actionOK := matchValues(action, group.Actions, false)
		if keyOK && typeOK && actionOK {
			return true
		}
	}
	return false
}

func parseGroupSpec(spec string, out *Group) (warning string, ok bool) {
	if out == nil {
		return "", false
	}
	trimmed := trim(spec)
	if trimmed == "" {
		return "empty group spec", false
	}

	var group Group
	hasValue := false
	var warnings []string
	for _, token := range split(trimmed, ';') {
		part := trim(token)
		if part == "" {
			continue
		}
		pos := strings.IndexByte(part, '=')
		if pos < 0 {
			warnings = append(warnings, "missing '=' in '"+part+"'")
			continue
		}
		field := toLower(trim(part[:pos]))
		value := trim(part[pos+1:])
		switch field {
		case "key":
			before := len(group.KeyFast) + len(group.KeyRegex)
			parseKeyList(value, &group.KeyFast, &group.KeyRegex, &warnings)
			after := len(group.KeyFast) + len(group.KeyRegex)
			hasValue = hasValue || after > before
		case "type":
			items := split(value, ',')
			added := 0
			for _, item := range items {
				v := trim(item)
				if v == "" {
					continue
				}
				group.Types = append(group.Types, toLower(v))
				added++
			}
			if added == 0 {
				warnings = append(warnings, "empty type list")
			} else {
				hasValue = true
			}
		case "action":
			items := split(value, ',')
			added := 0
			for _, item := range items {
				v := trim(item)
				if v == "" {
					continue
				}
				group.Actions = append(group.Actions, toLower(v))
				added++
			}
			if added == 0 {
				warnings = append(warnings, "empty action list")
			} else {
				hasValue = true
			}
		default:
			warnings = append(warnings, "unknown field '"+field+"'")
		}
	}
	if !hasValue {
		return "no valid conditions in '" + trimmed + "'", false
	}
	if len(warnings) > 0 {
		warning = strings.Join(warnings, "; ")
	}
	*out = group
	return warning, true
}

func parseKeyList(value string, outFast, outRegex *[]KeyMatcher, warnings *[]string) {
	if outFast == nil || outRegex == nil {
		return
	}
	for _, item := range split(value, ',') {
		v := trim(item)
		if v == "" {
			continue
		}
		matcher, err := parseKeyMatcher(v)
		if err != nil {
			if warnings != nil {
				*warnings = append(*warnings, "key pattern '"+v+"' skipped: "+err.Error())
			}
			continue
		}
		if matcher.Kind == KeyMatcherRegex {
			*outRegex = append(*outRegex, matcher)
		} else {
			*outFast = append(*outFast, matcher)
		}
	}
}

func parseKeyMatcher(value string) (KeyMatcher, error) {
	trimmed := trim(value)
	if trimmed == "" {
		return KeyMatcher{}, fmt.Errorf("empty key pattern")
	}
	if strings.HasSuffix(trimmed, "*") {
		prefix := strings.TrimSuffix(trimmed, "*")
		if !hasRegexMeta(prefix) {
			return KeyMatcher{
				Kind: KeyMatcherPrefix,
				Raw:  trimmed,
				Text: prefix,
			}, nil
		}
	}
	if !hasRegexMeta(trimmed) {
		return KeyMatcher{
			Kind: KeyMatcherExact,
			Raw:  trimmed,
			Text: trimmed,
		}, nil
	}
	re, err := regexp.Compile(trimmed)
	if err != nil {
		return KeyMatcher{}, err
	}
	return KeyMatcher{
		Kind: KeyMatcherRegex,
		Raw:  trimmed,
		Re:   re,
	}, nil
}

func hasRegexMeta(value string) bool {
	for i := 0; i < len(value); i++ {
		switch value[i] {
		case '.', '+', '?', '^', '$', '(', ')', '[', ']', '{', '}', '|', '\\', '*':
			return true
		default:
		}
	}
	return false
}

func matchKeys(key string, fast, regex []KeyMatcher) bool {
	for _, m := range fast {
		if m.Match(key) {
			return true
		}
	}
	for _, m := range regex {
		if m.Match(key) {
			return true
		}
	}
	return false
}

func matchValues(value string, values []string, allowUnknown bool) bool {
	if len(values) == 0 {
		return true
	}
	if value == "" {
		return true
	}
	if allowUnknown && equalsIgnoreCase(value, "unknown") {
		return true
	}
	if isLowerASCII(value) {
		for _, candidate := range values {
			if value == candidate {
				return true
			}
		}
		return false
	}
	for _, candidate := range values {
		if equalsIgnoreCase(value, candidate) {
			return true
		}
	}
	return false
}

func isLowerASCII(value string) bool {
	for i := 0; i < len(value); i++ {
		if value[i] >= 'A' && value[i] <= 'Z' {
			return false
		}
	}
	return true
}

func equalsIgnoreCase(value, lowerValue string) bool {
	if len(value) != len(lowerValue) {
		return false
	}
	for i := 0; i < len(value); i++ {
		ch := value[i]
		if ch >= 'A' && ch <= 'Z' {
			ch = ch + ('a' - 'A')
		}
		if ch != lowerValue[i] {
			return false
		}
	}
	return true
}

func toLower(value string) string {
	out := make([]byte, 0, len(value))
	for i := 0; i < len(value); i++ {
		ch := value[i]
		if ch >= 'A' && ch <= 'Z' {
			ch = ch + ('a' - 'A')
		}
		out = append(out, ch)
	}
	return string(out)
}

func trim(value string) string {
	return strings.TrimSpace(value)
}

func split(value string, delim byte) []string {
	var out []string
	start := 0
	for start <= len(value) {
		pos := strings.IndexByte(value[start:], delim)
		if pos < 0 {
			out = append(out, value[start:])
			break
		}
		out = append(out, value[start:start+pos])
		start = start + pos + 1
	}
	return out
}
