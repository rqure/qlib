package data

import (
	"encoding/base64"
	"strings"

	"github.com/rqure/qlib/pkg/log"
)

func FileEncode(content []byte) string {
	prefix := "data:application/octet-stream;base64,"
	return prefix + base64.StdEncoding.EncodeToString(content)
}

func FileDecode(encoded string) []byte {
	prefix := "data:application/octet-stream;base64,"
	if !strings.HasPrefix(encoded, prefix) {
		log.Error("Invalid prefix: %v", encoded)
		return []byte{}
	}

	decoded, err := base64.StdEncoding.DecodeString(strings.TrimPrefix(encoded, prefix))
	if err != nil {
		log.Error("Failed to decode: %v", err)
		return []byte{}
	}

	return decoded
}
