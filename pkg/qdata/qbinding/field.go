package qbinding

import (
	"context"
	"time"

	"github.com/rqure/qlib/pkg/qdata"
	"github.com/rqure/qlib/pkg/qdata/qfield"
	"github.com/rqure/qlib/pkg/qdata/qrequest"
	"github.com/rqure/qlib/pkg/qlog"
)

type Field struct {
	fieldOperator qdata.FieldOperator
	entityManager qdata.EntityManager
	schemaManager qdata.SchemaManager
	req           qdata.Request
}

func NewField(fieldOperator qdata.FieldOperator, entityManager qdata.EntityManager, schemaManager qdata.SchemaManager, entityId, fieldName string) qdata.FieldBinding {
	r := qrequest.New().SetEntityId(entityId).SetFieldName(fieldName)
	return &Field{
		fieldOperator: fieldOperator,
		entityManager: entityManager,
		schemaManager: schemaManager,
		req:           r,
	}
}

func (me *Field) GetEntityId() string {
	return me.req.GetEntityId()
}

func (me *Field) GetFieldName() string {
	return me.req.GetFieldName()
}

func (me *Field) GetWriteTime() time.Time {
	return qfield.FromRequest(me.req).GetWriteTime()
}

func (me *Field) GetWriter() string {
	return qfield.FromRequest(me.req).GetWriter()
}

func (me *Field) GetValue() qdata.ValueTypeProvider {
	return me.req.GetValue()
}

func (me *Field) WriteValue(ctx context.Context, v qdata.ValueTypeProvider) {
	me.req.SetValue(v)
	me.doWrite(ctx, me.req)
}

func (me *Field) WriteInt(ctx context.Context, args ...interface{}) {
	var v interface{}
	v = 0

	if len(args) > 0 {
		v = args[0]
	}

	me.req.SetValue(qfield.NewValue().SetInt(v))

	if len(args) > 1 {
		if opt, ok := args[1].(qdata.WriteOpt); ok {
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

	me.doWrite(ctx, me.req)

	// Clear settings for future use
	me.req.SetWriteTime(nil).SetWriter(nil).SetWriteOpt(qdata.WriteNormal)
}

func (me *Field) WriteFloat(ctx context.Context, args ...interface{}) {
	var v interface{}
	v = 0.0

	if len(args) > 0 {
		v = args[0]
	}

	me.req.SetValue(qfield.NewValue().SetFloat(v))

	if len(args) > 1 {
		if opt, ok := args[1].(qdata.WriteOpt); ok {
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

	me.doWrite(ctx, me.req)

	// Clear settings for future use
	me.req.SetWriteTime(nil).SetWriter(nil).SetWriteOpt(qdata.WriteNormal)
}

func (me *Field) WriteString(ctx context.Context, args ...interface{}) {
	var v interface{}
	v = ""

	if len(args) > 0 {
		v = args[0]
	}

	me.req.SetValue(qfield.NewValue().SetString(v))

	if len(args) > 1 {
		if opt, ok := args[1].(qdata.WriteOpt); ok {
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

	me.doWrite(ctx, me.req)

	// Clear settings for future use
	me.req.SetWriteTime(nil).SetWriter(nil).SetWriteOpt(qdata.WriteNormal)
}

func (me *Field) WriteBool(ctx context.Context, args ...interface{}) {
	var v interface{}
	v = false

	if len(args) > 0 {
		v = args[0]
	}

	me.req.SetValue(qfield.NewValue().SetBool(v))

	if len(args) > 1 {
		if opt, ok := args[1].(qdata.WriteOpt); ok {
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

	me.doWrite(ctx, me.req)

	// Clear settings for future use
	me.req.SetWriteTime(nil).SetWriter(nil).SetWriteOpt(qdata.WriteNormal)
}

func (me *Field) WriteBinaryFile(ctx context.Context, args ...interface{}) {
	var v interface{}
	v = ""

	if len(args) > 0 {
		v = args[0]
	}

	me.req.SetValue(qfield.NewValue().SetBinaryFile(v))

	if len(args) > 1 {
		if opt, ok := args[1].(qdata.WriteOpt); ok {
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

	me.doWrite(ctx, me.req)

	// Clear settings for future use
	me.req.SetWriteTime(nil).SetWriter(nil).SetWriteOpt(qdata.WriteNormal)
}

func (me *Field) WriteEntityReference(ctx context.Context, args ...interface{}) {
	var v interface{}
	v = ""

	if len(args) > 0 {
		v = args[0]
	}

	me.req.SetValue(qfield.NewValue().SetEntityReference(v))

	if len(args) > 1 {
		if opt, ok := args[1].(qdata.WriteOpt); ok {
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

	me.doWrite(ctx, me.req)

	// Clear settings for future use
	me.req.SetWriteTime(nil).SetWriter(nil).SetWriteOpt(qdata.WriteNormal)
}

func (me *Field) WriteTimestamp(ctx context.Context, args ...interface{}) {
	var v interface{}
	v = time.Time{}

	if len(args) > 0 {
		v = args[0]
	}

	me.req.SetValue(qfield.NewValue().SetTimestamp(v))

	if len(args) > 1 {
		if opt, ok := args[1].(qdata.WriteOpt); ok {
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

	me.doWrite(ctx, me.req)

	// Clear settings for future use
	me.req.SetWriteTime(nil).SetWriter(nil).SetWriteOpt(qdata.WriteNormal)
}

func (me *Field) WriteChoice(ctx context.Context, args ...interface{}) {
	var selectedIndex interface{} = 0

	if len(args) > 0 {
		selectedIndex = args[0]
	}

	if choice, ok := selectedIndex.(string); ok {
		entity := me.entityManager.GetEntity(ctx, me.req.GetEntityId())
		schema := me.schemaManager.GetFieldSchema(ctx, entity.GetType(), me.req.GetFieldName())

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

	me.req.SetValue(qfield.NewValue().SetChoice(selectedIndex))

	// Set write options if provided
	if len(args) > 1 {
		if opt, ok := args[1].(qdata.WriteOpt); ok {
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

	me.doWrite(ctx, me.req)

	// Clear settings for future use
	me.req.SetWriteTime(nil).SetWriter(nil).SetWriteOpt(qdata.WriteNormal)
}

func (me *Field) WriteEntityList(ctx context.Context, args ...interface{}) {
	var entities interface{}
	entities = []string{}

	if len(args) > 0 {
		entities = args[0]
	}

	me.req.SetValue(qfield.NewValue().SetEntityList(entities))

	// Set write options if provided
	if len(args) > 1 {
		if opt, ok := args[1].(qdata.WriteOpt); ok {
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

	me.doWrite(ctx, me.req)

	// Clear settings for future use
	me.req.SetWriteTime(nil).SetWriter(nil).SetWriteOpt(qdata.WriteNormal)
}

func (me *Field) ReadValue(ctx context.Context) qdata.ValueTypeProvider {
	me.fieldOperator.Read(ctx, me.req)
	return me.GetValue()
}

func (me *Field) ReadInt(ctx context.Context) int64 {
	me.fieldOperator.Read(ctx, me.req)
	return me.GetInt()
}

func (me *Field) ReadFloat(ctx context.Context) float64 {
	me.fieldOperator.Read(ctx, me.req)
	return me.GetFloat()
}

func (me *Field) ReadString(ctx context.Context) string {
	me.fieldOperator.Read(ctx, me.req)
	return me.GetString()
}

func (me *Field) ReadBool(ctx context.Context) bool {
	me.fieldOperator.Read(ctx, me.req)
	return me.GetBool()
}

func (me *Field) ReadBinaryFile(ctx context.Context) string {
	me.fieldOperator.Read(ctx, me.req)
	return me.GetBinaryFile()
}

func (me *Field) ReadEntityReference(ctx context.Context) string {
	me.fieldOperator.Read(ctx, me.req)
	return me.GetEntityReference()
}

func (me *Field) ReadTimestamp(ctx context.Context) time.Time {
	me.fieldOperator.Read(ctx, me.req)
	return me.GetTimestamp()
}

func (me *Field) ReadChoice(ctx context.Context) qdata.CompleteChoice {
	me.fieldOperator.Read(ctx, me.req)
	return me.GetCompleteChoice(ctx)
}

func (me *Field) ReadEntityList(ctx context.Context) qdata.EntityList {
	me.fieldOperator.Read(ctx, me.req)
	return me.GetEntityList()
}

func (me *Field) IsInt() bool {
	return me.GetValue().IsInt()
}

func (me *Field) IsFloat() bool {
	return me.GetValue().IsFloat()
}

func (me *Field) IsString() bool {
	return me.GetValue().IsString()
}

func (me *Field) IsBool() bool {
	return me.GetValue().IsBool()
}

func (me *Field) IsBinaryFile() bool {
	return me.GetValue().IsBinaryFile()
}

func (me *Field) IsEntityReference() bool {
	return me.GetValue().IsEntityReference()
}

func (me *Field) IsTimestamp() bool {
	return me.GetValue().IsTimestamp()
}

func (me *Field) IsChoice() bool {
	return me.GetValue().IsChoice()
}

func (me *Field) IsEntityList() bool {
	return me.GetValue().IsEntityList()
}

func (me *Field) GetInt() int64 {
	return me.GetValue().GetInt()
}

func (me *Field) GetFloat() float64 {
	return me.GetValue().GetFloat()
}

func (me *Field) GetString() string {
	return me.GetValue().GetString()
}

func (me *Field) GetBool() bool {
	return me.GetValue().GetBool()
}

func (me *Field) GetBinaryFile() string {
	return me.GetValue().GetBinaryFile()
}

func (me *Field) GetEntityReference() string {
	return me.GetValue().GetEntityReference()
}

func (me *Field) GetTimestamp() time.Time {
	return me.GetValue().GetTimestamp()
}

func (me *Field) GetChoice() qdata.Choice {
	return me.GetValue().GetChoice()
}

func (me *Field) GetCompleteChoice(ctx context.Context) qdata.CompleteChoice {
	choice, ok := me.GetValue().GetChoice().(qdata.CompleteChoice)
	if !ok {
		qlog.Error("Choice is not a CompleteChoice")
		return nil
	}

	entity := me.entityManager.GetEntity(ctx, me.req.GetEntityId())
	schema := me.schemaManager.GetFieldSchema(ctx, entity.GetType(), me.req.GetFieldName())
	if schema.IsChoice() {
		choices := schema.AsChoiceFieldSchema().GetChoices()
		choice.SetOptions(choices)
	}

	return choice
}

func (me *Field) GetEntityList() qdata.EntityList {
	return me.GetValue().GetEntityList()
}

func (me *Field) SetValue(v qdata.ValueTypeProvider) {
	me.req.SetValue(v)
}

func (me *Field) doWrite(ctx context.Context, reqs ...qdata.Request) {
	fieldOperator := ctx.Value(qdata.TransactionKey).(qdata.FieldOperator)

	if fieldOperator == nil {
		fieldOperator = me.fieldOperator
	}

	fieldOperator.Write(ctx, reqs...)
}
