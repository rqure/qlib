package binding

import (
	"context"
	"time"

	"github.com/rqure/qlib/pkg/data"
	"github.com/rqure/qlib/pkg/data/field"
	"github.com/rqure/qlib/pkg/data/request"
)

type Field struct {
	store *data.Store
	req   data.Request
}

func NewField(store *data.Store, entityId, fieldName string) data.FieldBinding {
	r := request.New().SetEntityId(entityId).SetFieldName(fieldName)
	return &Field{
		store: store,
		req:   r,
	}
}

func (me *Field) GetEntityId() string {
	return me.req.GetEntityId()
}

func (me *Field) GetFieldName() string {
	return me.req.GetFieldName()
}

func (me *Field) GetWriteTime() time.Time {
	return field.FromRequest(me.req).GetWriteTime()
}

func (me *Field) GetWriter() string {
	return field.FromRequest(me.req).GetWriter()
}

func (me *Field) GetValue() data.Value {
	return me.req.GetValue()
}

func (me *Field) WriteValue(ctx context.Context, v data.Value) data.FieldBinding {
	me.req.SetValue(v)
	me.withStore().Write(ctx, me.req)

	return me
}

func (me *Field) WriteInt(ctx context.Context, args ...interface{}) data.FieldBinding {
	var v interface{}
	v = 0

	if len(args) > 0 {
		v = args[0]
	}

	me.req.SetValue(field.NewValue().SetInt(v))

	if len(args) > 1 {
		if opt, ok := args[1].(data.WriteOpt); ok {
			me.req.SetWriteOpt(opt)
		}
	}

	if len(args) > 2 {
		if wt, ok := args[2].(time.Time); ok {
			me.req.SetWriteTime(&wt)
		}
	}

	if len(args) > 3 {
		if writer, ok := args[3].(string); ok {
			me.req.SetWriter(&writer)
		}
	}

	me.withStore().Write(ctx, me.req)

	// Clear settings for future use
	me.req.SetWriteTime(nil).SetWriter(nil).SetWriteOpt(data.WriteNormal)

	return me
}

func (me *Field) WriteFloat(ctx context.Context, args ...interface{}) data.FieldBinding {
	var v interface{}
	v = 0.0

	if len(args) > 0 {
		v = args[0]
	}

	me.req.SetValue(field.NewValue().SetFloat(v))

	if len(args) > 1 {
		if opt, ok := args[1].(data.WriteOpt); ok {
			me.req.SetWriteOpt(opt)
		}
	}

	if len(args) > 2 {
		if wt, ok := args[2].(time.Time); ok {
			me.req.SetWriteTime(&wt)
		}
	}

	if len(args) > 3 {
		if writer, ok := args[3].(string); ok {
			me.req.SetWriter(&writer)
		}
	}

	me.withStore().Write(ctx, me.req)

	// Clear settings for future use
	me.req.SetWriteTime(nil).SetWriter(nil).SetWriteOpt(data.WriteNormal)

	return me
}

func (me *Field) WriteString(ctx context.Context, args ...interface{}) data.FieldBinding {
	var v interface{}
	v = ""

	if len(args) > 0 {
		v = args[0]
	}

	me.req.SetValue(field.NewValue().SetString(v))

	if len(args) > 1 {
		if opt, ok := args[1].(data.WriteOpt); ok {
			me.req.SetWriteOpt(opt)
		}
	}

	if len(args) > 2 {
		if wt, ok := args[2].(time.Time); ok {
			me.req.SetWriteTime(&wt)
		}
	}

	if len(args) > 3 {
		if writer, ok := args[3].(string); ok {
			me.req.SetWriter(&writer)
		}
	}

	me.withStore().Write(ctx, me.req)

	// Clear settings for future use
	me.req.SetWriteTime(nil).SetWriter(nil).SetWriteOpt(data.WriteNormal)

	return me
}

func (me *Field) WriteBool(ctx context.Context, args ...interface{}) data.FieldBinding {
	var v interface{}
	v = false

	if len(args) > 0 {
		v = args[0]
	}

	me.req.SetValue(field.NewValue().SetBool(v))

	if len(args) > 1 {
		if opt, ok := args[1].(data.WriteOpt); ok {
			me.req.SetWriteOpt(opt)
		}
	}

	if len(args) > 2 {
		if wt, ok := args[2].(time.Time); ok {
			me.req.SetWriteTime(&wt)
		}
	}

	if len(args) > 3 {
		if writer, ok := args[3].(string); ok {
			me.req.SetWriter(&writer)
		}
	}

	me.withStore().Write(ctx, me.req)

	// Clear settings for future use
	me.req.SetWriteTime(nil).SetWriter(nil).SetWriteOpt(data.WriteNormal)

	return me
}

func (me *Field) WriteBinaryFile(ctx context.Context, args ...interface{}) data.FieldBinding {
	var v interface{}
	v = ""

	if len(args) > 0 {
		v = args[0]
	}

	me.req.SetValue(field.NewValue().SetBinaryFile(v))

	if len(args) > 1 {
		if opt, ok := args[1].(data.WriteOpt); ok {
			me.req.SetWriteOpt(opt)
		}
	}

	if len(args) > 2 {
		if wt, ok := args[2].(time.Time); ok {
			me.req.SetWriteTime(&wt)
		}
	}

	if len(args) > 3 {
		if writer, ok := args[3].(string); ok {
			me.req.SetWriter(&writer)
		}
	}

	me.withStore().Write(ctx, me.req)

	// Clear settings for future use
	me.req.SetWriteTime(nil).SetWriter(nil).SetWriteOpt(data.WriteNormal)

	return me
}

func (me *Field) WriteEntityReference(ctx context.Context, args ...interface{}) data.FieldBinding {
	var v interface{}
	v = ""

	if len(args) > 0 {
		v = args[0]
	}

	me.req.SetValue(field.NewValue().SetEntityReference(v))

	if len(args) > 1 {
		if opt, ok := args[1].(data.WriteOpt); ok {
			me.req.SetWriteOpt(opt)
		}
	}

	if len(args) > 2 {
		if wt, ok := args[2].(time.Time); ok {
			me.req.SetWriteTime(&wt)
		}
	}

	if len(args) > 3 {
		if writer, ok := args[3].(string); ok {
			me.req.SetWriter(&writer)
		}
	}

	me.withStore().Write(ctx, me.req)

	// Clear settings for future use
	me.req.SetWriteTime(nil).SetWriter(nil).SetWriteOpt(data.WriteNormal)

	return me
}

func (me *Field) WriteTimestamp(ctx context.Context, args ...interface{}) data.FieldBinding {
	var v interface{}
	v = time.Time{}

	if len(args) > 0 {
		v = args[0]
	}

	me.req.SetValue(field.NewValue().SetTimestamp(v))

	if len(args) > 1 {
		if opt, ok := args[1].(data.WriteOpt); ok {
			me.req.SetWriteOpt(opt)
		}
	}

	if len(args) > 2 {
		if wt, ok := args[2].(time.Time); ok {
			me.req.SetWriteTime(&wt)
		}
	}

	if len(args) > 3 {
		if writer, ok := args[3].(string); ok {
			me.req.SetWriter(&writer)
		}
	}

	me.withStore().Write(ctx, me.req)

	// Clear settings for future use
	me.req.SetWriteTime(nil).SetWriter(nil).SetWriteOpt(data.WriteNormal)

	return me
}

func (me *Field) WriteTransformation(ctx context.Context, args ...interface{}) data.FieldBinding {
	var v interface{}
	v = ""

	if len(args) > 0 {
		v = args[0]
	}

	me.req.SetValue(field.NewValue().SetTransformation(v))

	if len(args) > 1 {
		if opt, ok := args[1].(data.WriteOpt); ok {
			me.req.SetWriteOpt(opt)
		}
	}

	if len(args) > 2 {
		if wt, ok := args[2].(time.Time); ok {
			me.req.SetWriteTime(&wt)
		}
	}

	if len(args) > 3 {
		if writer, ok := args[3].(string); ok {
			me.req.SetWriter(&writer)
		}
	}

	me.withStore().Write(ctx, me.req)

	// Clear settings for future use
	me.req.SetWriteTime(nil).SetWriter(nil).SetWriteOpt(data.WriteNormal)

	return me
}

func (me *Field) WriteChoice(ctx context.Context, args ...interface{}) data.FieldBinding {
	var selectedIndex interface{} = 0

	if len(args) > 0 {
		selectedIndex = args[0]
	}

	if choice, ok := selectedIndex.(string); ok {
		entity := me.withStore().GetEntity(ctx, me.req.GetEntityId())
		schema := me.withStore().GetFieldSchema(ctx, me.req.GetFieldName(), entity.GetType())
		if schema.IsChoice() {
			choices := schema.AsChoiceFieldSchema().GetChoices()
			for i, c := range choices {
				if c == choice {
					selectedIndex = i
					break
				}
			}
		}
	}

	me.req.SetValue(field.NewValue().SetChoice(selectedIndex))

	// Set write options if provided
	if len(args) > 1 {
		if opt, ok := args[1].(data.WriteOpt); ok {
			me.req.SetWriteOpt(opt)
		}
	}

	if len(args) > 2 {
		if wt, ok := args[2].(time.Time); ok {
			me.req.SetWriteTime(&wt)
		}
	}

	if len(args) > 3 {
		if writer, ok := args[3].(string); ok {
			me.req.SetWriter(&writer)
		}
	}

	me.withStore().Write(ctx, me.req)

	// Clear settings for future use
	me.req.SetWriteTime(nil).SetWriter(nil).SetWriteOpt(data.WriteNormal)

	return me
}

func (me *Field) WriteEntityList(ctx context.Context, args ...interface{}) data.FieldBinding {
	var entities interface{}
	entities = []string{}

	if len(args) > 0 {
		entities = args[0]
	}

	me.req.SetValue(field.NewValue().SetEntityList(entities))

	// Set write options if provided
	if len(args) > 1 {
		if opt, ok := args[1].(data.WriteOpt); ok {
			me.req.SetWriteOpt(opt)
		}
	}

	if len(args) > 2 {
		if wt, ok := args[2].(time.Time); ok {
			me.req.SetWriteTime(&wt)
		}
	}

	if len(args) > 3 {
		if writer, ok := args[3].(string); ok {
			me.req.SetWriter(&writer)
		}
	}

	me.withStore().Write(ctx, me.req)

	// Clear settings for future use
	me.req.SetWriteTime(nil).SetWriter(nil).SetWriteOpt(data.WriteNormal)

	return me
}

func (me *Field) ReadValue(ctx context.Context) data.Value {
	me.withStore().Read(ctx, me.req)
	return me.req.GetValue()
}

func (me *Field) ReadInt(ctx context.Context) int64 {
	me.withStore().Read(ctx, me.req)
	return me.req.GetValue().GetInt()
}

func (me *Field) ReadFloat(ctx context.Context) float64 {
	me.withStore().Read(ctx, me.req)
	return me.req.GetValue().GetFloat()
}

func (me *Field) ReadString(ctx context.Context) string {
	me.withStore().Read(ctx, me.req)
	return me.req.GetValue().GetString()
}

func (me *Field) ReadBool(ctx context.Context) bool {
	me.withStore().Read(ctx, me.req)
	return me.req.GetValue().GetBool()
}

func (me *Field) ReadBinaryFile(ctx context.Context) string {
	me.withStore().Read(ctx, me.req)
	return me.req.GetValue().GetBinaryFile()
}

func (me *Field) ReadEntityReference(ctx context.Context) string {
	me.withStore().Read(ctx, me.req)
	return me.req.GetValue().GetEntityReference()
}

func (me *Field) ReadTimestamp(ctx context.Context) time.Time {
	me.withStore().Read(ctx, me.req)
	return me.req.GetValue().GetTimestamp()
}

func (me *Field) ReadTransformation(ctx context.Context) string {
	me.withStore().Read(ctx, me.req)
	return me.req.GetValue().GetTransformation()
}

func (me *Field) ReadChoice(ctx context.Context) data.Choice {
	me.withStore().Read(ctx, me.req)
	return me.req.GetValue().GetChoice()
}

func (me *Field) ReadEntityList(ctx context.Context) data.EntityList {
	me.withStore().Read(ctx, me.req)
	return me.req.GetValue().GetEntityList()
}

func (me *Field) IsInt() bool {
	return me.req.GetValue().IsInt()
}

func (me *Field) IsFloat() bool {
	return me.req.GetValue().IsFloat()
}

func (me *Field) IsString() bool {
	return me.req.GetValue().IsString()
}

func (me *Field) IsBool() bool {
	return me.req.GetValue().IsBool()
}

func (me *Field) IsBinaryFile() bool {
	return me.req.GetValue().IsBinaryFile()
}

func (me *Field) IsEntityReference() bool {
	return me.req.GetValue().IsEntityReference()
}

func (me *Field) IsTimestamp() bool {
	return me.req.GetValue().IsTimestamp()
}

func (me *Field) IsTransformation() bool {
	return me.req.GetValue().IsTransformation()
}

func (me *Field) IsChoice() bool {
	return me.req.GetValue().IsChoice()
}

func (me *Field) IsEntityList() bool {
	return me.req.GetValue().IsEntityList()
}

func (me *Field) GetInt() int64 {
	return me.req.GetValue().GetInt()
}

func (me *Field) GetFloat() float64 {
	return me.req.GetValue().GetFloat()
}

func (me *Field) GetString() string {
	return me.req.GetValue().GetString()
}

func (me *Field) GetBool() bool {
	return me.req.GetValue().GetBool()
}

func (me *Field) GetBinaryFile() string {
	return me.req.GetValue().GetBinaryFile()
}

func (me *Field) GetEntityReference() string {
	return me.req.GetValue().GetEntityReference()
}

func (me *Field) GetTimestamp() time.Time {
	return me.req.GetValue().GetTimestamp()
}

func (me *Field) GetTransformation() string {
	return me.req.GetValue().GetTransformation()
}

func (me *Field) GetChoice() data.Choice {
	return me.req.GetValue().GetChoice()
}

func (me *Field) GetEntityList() data.EntityList {
	return me.req.GetValue().GetEntityList()
}

func (me *Field) SetValue(v data.Value) data.FieldBinding {
	me.req.SetValue(v)
	return me
}

func (me *Field) withStore() data.Store {
	return *me.store
}
