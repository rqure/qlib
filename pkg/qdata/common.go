package qdata

import (
	"time"

	"github.com/rqure/qlib/pkg/qprotobufs"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type WriteOpt int

type EntityId string
type EntityType string
type FieldType string

type WriteTime time.Time

func (me *WriteTime) AsTimestampPb() *qprotobufs.Timestamp {
	if me == nil {
		return nil
	}

	return &qprotobufs.Timestamp{
		Raw: timestamppb.New(time.Time(*me)),
	}
}

func (me *EntityId) AsStringPb() *qprotobufs.String {
	if me == nil {
		return nil
	}

	return &qprotobufs.String{
		Raw: string(*me),
	}
}

const (
	WriteNormal WriteOpt = iota
	WriteChanges
)

type Entity struct {
	EntityId   EntityId
	EntityType EntityType
}

type EntitySchema struct {
	EntityType EntityType
	Fields     []*FieldSchema
}

type Field struct {
	EntityId  EntityId
	FieldType FieldType
	Value     *Value
	WriteTime WriteTime
	WriterId  EntityId
}

type FieldSchema struct {
	EntityType EntityType
	FieldType  FieldType
	ValueType  ValueType

	ReadPermissions  []EntityId
	WritePermissions []EntityId
}

type Request struct {
	EntityId  EntityId
	FieldType FieldType
	Value     *Value
	WriteOpt  WriteOpt   // optional
	WriteTime *WriteTime // optional
	WriterId  *EntityId  // optional
	Success   bool
}

func (me *Entity) Clone() *Entity {
	return &Entity{
		EntityId:   me.EntityId,
		EntityType: me.EntityType,
	}
}

func (me *EntitySchema) Clone() *EntitySchema {
	return &EntitySchema{
		EntityType: me.EntityType,
		Fields:     me.Fields,
	}
}

func (me *Field) Clone() *Field {
	return &Field{
		EntityId:  me.EntityId,
		FieldType: me.FieldType,
		Value:     me.Value.Clone(),
		WriteTime: me.WriteTime,
		WriterId:  me.WriterId,
	}
}

func (me *FieldSchema) Clone() *FieldSchema {
	return &FieldSchema{
		EntityType:       me.EntityType,
		FieldType:        me.FieldType,
		ValueType:        me.ValueType,
		ReadPermissions:  append([]EntityId{}, me.ReadPermissions...),
		WritePermissions: append([]EntityId{}, me.WritePermissions...),
	}
}

func (me *Request) Clone() *Request {
	var wt *WriteTime
	if me.WriteTime != nil {
		wt = new(WriteTime)
		*wt = *me.WriteTime
	}

	var wId *EntityId
	if me.WriterId != nil {
		wId = new(EntityId)
		*wId = *me.WriterId
	}

	return &Request{
		EntityId:  me.EntityId,
		FieldType: me.FieldType,
		Value:     me.Value.Clone(),
		WriteOpt:  me.WriteOpt,
		WriteTime: wt,
		WriterId:  wId,
		Success:   me.Success,
	}
}

func (me *Request) AsField() *Field {
	wt := new(WriteTime)
	if me.WriteTime != nil {
		*wt = *me.WriteTime
	}

	wId := new(EntityId)
	if me.WriterId != nil {
		*wId = *me.WriterId
	}

	return &Field{
		EntityId:  me.EntityId,
		FieldType: me.FieldType,
		Value:     me.Value.Clone(),
		WriteTime: *wt,
		WriterId:  *wId,
	}
}

func (me *Field) AsRequest() *Request {
	wt := new(WriteTime)
	*wt = me.WriteTime

	wId := new(EntityId)
	*wId = me.WriterId

	return &Request{
		EntityId:  me.EntityId,
		FieldType: me.FieldType,
		Value:     me.Value.Clone(),
		WriteOpt:  WriteNormal,
		WriteTime: wt,
		WriterId:  wId,
		Success:   false,
	}
}

func (me *Request) AsRequestPb() *qprotobufs.DatabaseRequest {
	return &qprotobufs.DatabaseRequest{
		Id:        string(me.EntityId),
		Field:     string(me.FieldType),
		Value:     me.Value.AsAnyPb(),
		WriteTime: me.WriteTime.AsTimestampPb(),
		WriterId:  me.WriterId.AsStringPb(),
	}
}

func (me *Entity) FromEntityPb(pb *qprotobufs.DatabaseEntity) *Entity {
	me.EntityId = EntityId(pb.Id)
	me.EntityType = EntityType(pb.Type)

	return me
}
