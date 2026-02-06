package event

import (
	"encoding/base64"
)

type PayloadEncoding uint8

const (
	PayloadEncodingBase64 PayloadEncoding = iota
	PayloadEncodingNone
)

func (e PayloadEncoding) String() string {
	switch e {
	case PayloadEncodingBase64:
		return "base64"
	case PayloadEncodingNone:
		return "none"
	default:
		return "none"
	}
}

func EncodePayload(input []byte, encoding PayloadEncoding) []byte {
	if encoding != PayloadEncodingBase64 {
		return input
	}
	encoded := make([]byte, base64.StdEncoding.EncodedLen(len(input)))
	base64.StdEncoding.Encode(encoded, input)
	return encoded
}
