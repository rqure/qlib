package binding

import (
	"time"

	"github.com/rqure/qlib/pkg/data"
	"github.com/rqure/qlib/pkg/data/field"
	"github.com/rqure/qlib/pkg/data/request"
)

type Field struct {
	store data.Store
	req   data.Request
}

func NewField(store data.Store, entityId, fieldName string) data.FieldBinding {
	r := request.New().SetEntityId(entityId).SetFieldName(fieldName)
	return &Field{
		store: store,
		req:   r,
	}
}

func (b *Field) GetEntityId() string {
	return b.req.GetEntityId()
}

func (b *Field) GetFieldName() string {
	return b.req.GetFieldName()
}

func (b *Field) GetWriteTime() time.Time {
	return field.FromRequest(b.req).GetWriteTime()
}

func (b *Field) GetWriter() string {
	return field.FromRequest(b.req).GetWriter()
}

func (b *Field) GetValue() data.Value {
	return b.req.GetValue()
}

func (b *Field) WriteValue(v data.Value) data.FieldBinding {
	b.req.SetValue(v)
	b.store.Write(b.req)

	return b
}

func (b *Field) WriteInt(args ...interface{}) data.FieldBinding {
	v := args[0]
	b.req.SetValue(field.NewValue().SetInt(v))

	if len(args) > 1 {
		if opt, ok := args[1].(data.WriteOpt); ok {
			b.req.SetWriteOpt(opt)
		}
	}

	if len(args) > 2 {
		if wt, ok := args[2].(time.Time); ok {
			b.req.SetWriteTime(&wt)
		}
	}

	if len(args) > 3 {
		if writer, ok := args[3].(string); ok {
			b.req.SetWriter(&writer)
		}
	}

	b.store.Write(b.req)

	// Clear settings for future use
	b.req.SetWriteTime(nil).SetWriter(nil).SetWriteOpt(data.WriteNormal)

	return b
}

func (b *Field) WriteFloat(args ...interface{}) data.FieldBinding {
	v := args[0]
	b.req.SetValue(field.NewValue().SetFloat(v))

	if len(args) > 1 {
		if opt, ok := args[1].(data.WriteOpt); ok {
			b.req.SetWriteOpt(opt)
		}
	}

	if len(args) > 2 {
		if wt, ok := args[2].(time.Time); ok {
			b.req.SetWriteTime(&wt)
		}
	}

	if len(args) > 3 {
		if writer, ok := args[3].(string); ok {
			b.req.SetWriter(&writer)
		}
	}

	b.store.Write(b.req)

	// Clear settings for future use
	b.req.SetWriteTime(nil).SetWriter(nil).SetWriteOpt(data.WriteNormal)

	return b
}

func (b *Field) WriteString(args ...interface{}) data.FieldBinding {
	v := args[0]
	b.req.SetValue(field.NewValue().SetString(v))

	if len(args) > 1 {
		if opt, ok := args[1].(data.WriteOpt); ok {
			b.req.SetWriteOpt(opt)
		}
	}

	if len(args) > 2 {
		if wt, ok := args[2].(time.Time); ok {
			b.req.SetWriteTime(&wt)
		}
	}

	if len(args) > 3 {
		if writer, ok := args[3].(string); ok {
			b.req.SetWriter(&writer)
		}
	}

	b.store.Write(b.req)

	// Clear settings for future use
	b.req.SetWriteTime(nil).SetWriter(nil).SetWriteOpt(data.WriteNormal)

	return b
}

func (b *Field) WriteBool(args ...interface{}) data.FieldBinding {
	v := args[0]
	b.req.SetValue(field.NewValue().SetBool(v))

	if len(args) > 1 {
		if opt, ok := args[1].(data.WriteOpt); ok {
			b.req.SetWriteOpt(opt)
		}
	}

	if len(args) > 2 {
		if wt, ok := args[2].(time.Time); ok {
			b.req.SetWriteTime(&wt)
		}
	}

	if len(args) > 3 {
		if writer, ok := args[3].(string); ok {
			b.req.SetWriter(&writer)
		}
	}

	b.store.Write(b.req)

	// Clear settings for future use
	b.req.SetWriteTime(nil).SetWriter(nil).SetWriteOpt(data.WriteNormal)

	return b
}

func (b *Field) WriteBinaryFile(args ...interface{}) data.FieldBinding {
	v := args[0]
	b.req.SetValue(field.NewValue().SetBinaryFile(v))

	if len(args) > 1 {
		if opt, ok := args[1].(data.WriteOpt); ok {
			b.req.SetWriteOpt(opt)
		}
	}

	if len(args) > 2 {
		if wt, ok := args[2].(time.Time); ok {
			b.req.SetWriteTime(&wt)
		}
	}

	if len(args) > 3 {
		if writer, ok := args[3].(string); ok {
			b.req.SetWriter(&writer)
		}
	}

	b.store.Write(b.req)

	// Clear settings for future use
	b.req.SetWriteTime(nil).SetWriter(nil).SetWriteOpt(data.WriteNormal)

	return b
}

func (b *Field) WriteEntityReference(args ...interface{}) data.FieldBinding {
	v := args[0]
	b.req.SetValue(field.NewValue().SetEntityReference(v))

	if len(args) > 1 {
		if opt, ok := args[1].(data.WriteOpt); ok {
			b.req.SetWriteOpt(opt)
		}
	}

	if len(args) > 2 {
		if wt, ok := args[2].(time.Time); ok {
			b.req.SetWriteTime(&wt)
		}
	}

	if len(args) > 3 {
		if writer, ok := args[3].(string); ok {
			b.req.SetWriter(&writer)
		}
	}

	b.store.Write(b.req)

	// Clear settings for future use
	b.req.SetWriteTime(nil).SetWriter(nil).SetWriteOpt(data.WriteNormal)

	return b
}

func (b *Field) WriteTimestamp(args ...interface{}) data.FieldBinding {
	v := args[0]
	b.req.SetValue(field.NewValue().SetTimestamp(v))

	if len(args) > 1 {
		if opt, ok := args[1].(data.WriteOpt); ok {
			b.req.SetWriteOpt(opt)
		}
	}

	if len(args) > 2 {
		if wt, ok := args[2].(time.Time); ok {
			b.req.SetWriteTime(&wt)
		}
	}

	if len(args) > 3 {
		if writer, ok := args[3].(string); ok {
			b.req.SetWriter(&writer)
		}
	}

	b.store.Write(b.req)

	// Clear settings for future use
	b.req.SetWriteTime(nil).SetWriter(nil).SetWriteOpt(data.WriteNormal)

	return b
}

func (b *Field) WriteTransformation(args ...interface{}) data.FieldBinding {
	v := args[0]
	b.req.SetValue(field.NewValue().SetTransformation(v))

	if len(args) > 1 {
		if opt, ok := args[1].(data.WriteOpt); ok {
			b.req.SetWriteOpt(opt)
		}
	}

	if len(args) > 2 {
		if wt, ok := args[2].(time.Time); ok {
			b.req.SetWriteTime(&wt)
		}
	}

	if len(args) > 3 {
		if writer, ok := args[3].(string); ok {
			b.req.SetWriter(&writer)
		}
	}

	b.store.Write(b.req)

	// Clear settings for future use
	b.req.SetWriteTime(nil).SetWriter(nil).SetWriteOpt(data.WriteNormal)

	return b
}

func (b *Field) ReadValue() data.Value {
	b.store.Read(b.req)
	return b.req.GetValue()
}

func (b *Field) ReadInt() int64 {
	b.store.Read(b.req)
	return b.req.GetValue().GetInt()
}

func (b *Field) ReadFloat() float64 {
	b.store.Read(b.req)
	return b.req.GetValue().GetFloat()
}

func (b *Field) ReadString() string {
	b.store.Read(b.req)
	return b.req.GetValue().GetString()
}

func (b *Field) ReadBool() bool {
	b.store.Read(b.req)
	return b.req.GetValue().GetBool()
}

func (b *Field) ReadBinaryFile() string {
	b.store.Read(b.req)
	return b.req.GetValue().GetBinaryFile()
}

func (b *Field) ReadEntityReference() string {
	b.store.Read(b.req)
	return b.req.GetValue().GetEntityReference()
}

func (b *Field) ReadTimestamp() time.Time {
	b.store.Read(b.req)
	return b.req.GetValue().GetTimestamp()
}

func (b *Field) ReadTransformation() string {
	b.store.Read(b.req)
	return b.req.GetValue().GetTransformation()
}

func (b *Field) IsInt() bool {
	return b.req.GetValue().IsInt()
}

func (b *Field) IsFloat() bool {
	return b.req.GetValue().IsFloat()
}

func (b *Field) IsString() bool {
	return b.req.GetValue().IsString()
}

func (b *Field) IsBool() bool {
	return b.req.GetValue().IsBool()
}

func (b *Field) IsBinaryFile() bool {
	return b.req.GetValue().IsBinaryFile()
}

func (b *Field) IsEntityReference() bool {
	return b.req.GetValue().IsEntityReference()
}

func (b *Field) IsTimestamp() bool {
	return b.req.GetValue().IsTimestamp()
}

func (b *Field) IsTransformation() bool {
	return b.req.GetValue().IsTransformation()
}

func (b *Field) GetInt() int64 {
	return b.req.GetValue().GetInt()
}

func (b *Field) GetFloat() float64 {
	return b.req.GetValue().GetFloat()
}

func (b *Field) GetString() string {
	return b.req.GetValue().GetString()
}

func (b *Field) GetBool() bool {
	return b.req.GetValue().GetBool()
}

func (b *Field) GetBinaryFile() string {
	return b.req.GetValue().GetBinaryFile()
}

func (b *Field) GetEntityReference() string {
	return b.req.GetValue().GetEntityReference()
}

func (b *Field) GetTimestamp() time.Time {
	return b.req.GetValue().GetTimestamp()
}

func (b *Field) GetTransformation() string {
	return b.req.GetValue().GetTransformation()
}
