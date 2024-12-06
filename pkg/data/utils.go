package data

import (
	"encoding/base64"
	"strings"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

func FileEncode(content []byte) string {
	prefix := "data:application/octet-stream;base64,"
	return prefix + base64.StdEncoding.EncodeToString(content)
}

func FileDecode(encoded string) []byte {
	prefix := "data:application/octet-stream;base64,"
	if !strings.HasPrefix(encoded, prefix) {
		Error("[FileDecode] Invalid prefix: %v", encoded)
		return []byte{}
	}

	decoded, err := base64.StdEncoding.DecodeString(strings.TrimPrefix(encoded, prefix))
	if err != nil {
		Error("[FileDecode] Failed to decode: %v", err)
		return []byte{}
	}

	return decoded
}

func ValueCast[T proto.Message](value *anypb.Any) T {
	p, err := value.UnmarshalNew()

	if err != nil {
		Error("[ValueCast] Failed to unmarshal: %v", err)
	}

	c, ok := p.(T)
	if !ok {
		Error("[ValueCast] Failed to cast: %v", value)
	}

	return c
}
