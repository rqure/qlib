package qvalue

import (
	"encoding/base64"
	"strings"

	"github.com/rqure/qlib/pkg/qdata"
	"github.com/rqure/qlib/pkg/qlog"
	"github.com/rqure/qlib/pkg/qprotobufs"
	"google.golang.org/protobuf/types/known/anypb"
)

type BinaryFile struct {
	Value string
}

func NewBinaryFile(v ...string) *qdata.Value {
	me := &BinaryFile{
		Value: "", // Empty path as default
	}

	if len(v) > 0 {
		me.Value = v[0]
	}

	return &qdata.Value{
		ValueTypeProvider: &ValueTypeProvider{
			ValueType: qdata.BinaryFile,
		},
		ValueConstructor:   me,
		AnyPbConverter:     me,
		RawProvider:        me,
		RawReceiver:        me,
		BinaryFileProvider: me,
		BinaryFileReceiver: me,
	}
}

func (me *BinaryFile) GetBinaryFile() string {
	return me.Value
}

func (me *BinaryFile) SetBinaryFile(value string) {
	me.Value = value
}

func (me *BinaryFile) GetRaw() interface{} {
	return me.Value
}

func (me *BinaryFile) SetRaw(value interface{}) {
	if v, ok := value.(string); ok {
		me.Value = v
	}
}

func (me *BinaryFile) Clone() *qdata.Value {
	return NewBinaryFile(me.Value)
}

func FileEncode(content []byte) string {
	prefix := "data:application/octet-stream;base64,"
	return prefix + base64.StdEncoding.EncodeToString(content)
}

func FileDecode(encoded string) []byte {
	prefix := "data:application/octet-stream;base64,"
	if !strings.HasPrefix(encoded, prefix) {
		qlog.Error("Invalid prefix: %v", encoded)
		return []byte{}
	}

	decoded, err := base64.StdEncoding.DecodeString(strings.TrimPrefix(encoded, prefix))
	if err != nil {
		qlog.Error("Failed to decode: %v", err)
		return []byte{}
	}

	return decoded
}

func (me *BinaryFile) AsAnyPb() *anypb.Any {
	a, err := anypb.New(&qprotobufs.BinaryFile{
		Raw: me.Value,
	})

	if err != nil {
		return nil
	}

	return a
}
