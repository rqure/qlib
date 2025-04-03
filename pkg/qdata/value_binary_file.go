package qdata

import (
	"encoding/base64"
	"strings"

	"github.com/rqure/qlib/pkg/qlog"
	"github.com/rqure/qlib/pkg/qprotobufs"
	"google.golang.org/protobuf/types/known/anypb"
)

type ValueBinaryFile struct {
	Value []byte
}

func NewBinaryFile(v ...string) *Value {
	me := &ValueBinaryFile{
		Value: []byte{},
	}

	if len(v) > 0 {
		me.SetBinaryFile(v[0])
	}

	return &Value{
		ValueTypeProvider:  new(ValueType).As(VTBinaryFile),
		ValueConstructor:   me,
		AnyPbConverter:     me,
		StringConverter:    me,
		RawProvider:        me,
		RawReceiver:        me,
		BinaryFileProvider: me,
		BinaryFileReceiver: me,
	}
}

func (me *ValueBinaryFile) GetBinaryFile() string {
	return FileEncode(me.Value)
}

func (me *ValueBinaryFile) SetBinaryFile(value string) {
	me.Value = FileDecode(value)
}

func (me *ValueBinaryFile) GetRaw() interface{} {
	return me.Value
}

func (me *ValueBinaryFile) SetRaw(value interface{}) {
	switch v := value.(type) {
	case []byte:
		me.Value = v
	case string:
		me.Value = FileDecode(v)
	default:
		qlog.Error("Invalid type for SetRaw: %T", v)
	}
}

func (me *ValueBinaryFile) Clone() *Value {
	return NewBinaryFile(me.GetBinaryFile())
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

func (me *ValueBinaryFile) AsAnyPb() *anypb.Any {
	a, err := anypb.New(&qprotobufs.BinaryFile{
		Raw: me.GetBinaryFile(),
	})

	if err != nil {
		return nil
	}

	return a
}

func (me *ValueBinaryFile) AsString() string {
	return me.GetBinaryFile()
}
