package event

import (
	"strconv"
)

func AppendJsonEscaped(dst []byte, input []byte) []byte {
	for _, c := range input {
		switch c {
		case '"':
			dst = append(dst, '\\', '"')
		case '\\':
			dst = append(dst, '\\', '\\')
		case '\b':
			dst = append(dst, '\\', 'b')
		case '\f':
			dst = append(dst, '\\', 'f')
		case '\n':
			dst = append(dst, '\\', 'n')
		case '\r':
			dst = append(dst, '\\', 'r')
		case '\t':
			dst = append(dst, '\\', 't')
		default:
			if c < 0x20 {
				dst = append(dst, '\\', 'u', '0', '0')
				hex := strconv.FormatUint(uint64(c), 16)
				if len(hex) == 1 {
					dst = append(dst, '0', hex[0])
				} else {
					dst = append(dst, hex[0], hex[1])
				}
			} else {
				dst = append(dst, c)
			}
		}
	}
	return dst
}
