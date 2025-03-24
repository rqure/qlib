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
	EId   EntityId
	EType EntityType
}

type EntitySchema struct {
	EType  EntityType
	Fields []*FieldSchema
}

type Field struct {
	EId   EntityId
	FType FieldType
	V     *Value
	WT    WriteTime
	WId   EntityId
}

type FieldSchema struct {
	EType EntityType
	FType FieldType
	VType ValueType

	ReadPermissions  []EntityId
	WritePermissions []EntityId
}

type Request struct {
	EId     EntityId
	FType   FieldType
	V       *Value
	WO      WriteOpt
	WT      *WriteTime // optional
	WId     *EntityId  // optional
	Success bool
}

func (me *Entity) Clone() *Entity {
	return &Entity{
		EId:   me.EId,
		EType: me.EType,
	}
}

func (me *EntitySchema) Clone() *EntitySchema {
	return &EntitySchema{
		EType:  me.EType,
		Fields: me.Fields,
	}
}

func (me *Field) Clone() *Field {
	return &Field{
		EId:   me.EId,
		FType: me.FType,
		V:     me.V.Clone(),
		WT:    me.WT,
		WId:   me.WId,
	}
}

func (me *FieldSchema) Clone() *FieldSchema {
	return &FieldSchema{
		EType:            me.EType,
		FType:            me.FType,
		VType:            me.VType,
		ReadPermissions:  append([]EntityId{}, me.ReadPermissions...),
		WritePermissions: append([]EntityId{}, me.WritePermissions...),
	}
}

func (me *Request) Clone() *Request {
	var wt *WriteTime
	if me.WT != nil {
		wt = new(WriteTime)
		*wt = *me.WT
	}

	var wId *EntityId
	if me.WId != nil {
		wId = new(EntityId)
		*wId = *me.WId
	}

	return &Request{
		EId:     me.EId,
		FType:   me.FType,
		V:       me.V.Clone(),
		WO:      me.WO,
		WT:      wt,
		WId:     wId,
		Success: me.Success,
	}
}

func (me *Request) AsField() *Field {
	wt := new(WriteTime)
	if me.WT != nil {
		*wt = *me.WT
	}

	wId := new(EntityId)
	if me.WId != nil {
		*wId = *me.WId
	}

	return &Field{
		EId:   me.EId,
		FType: me.FType,
		V:     me.V.Clone(),
		WT:    *wt,
		WId:   *wId,
	}
}

func (me *Field) AsRequest() *Request {
	wt := new(WriteTime)
	*wt = me.WT

	wId := new(EntityId)
	*wId = me.WId

	return &Request{
		EId:     me.EId,
		FType:   me.FType,
		V:       me.V.Clone(),
		WO:      WriteNormal,
		WT:      wt,
		WId:     wId,
		Success: false,
	}
}

func (me *Request) AsRequestPb() *qprotobufs.DatabaseRequest {
	return &qprotobufs.DatabaseRequest{
		Id:        string(me.EId),
		Field:     string(me.FType),
		Value:     me.V.AsAnyPb(),
		WriteTime: me.WT.AsTimestampPb(),
		WriterId:  me.WId.AsStringPb(),
	}
}

func (me *Entity) FromEntityPb(pb *qprotobufs.DatabaseEntity) *Entity {
	me.EId = EntityId(pb.Id)
	me.EType = EntityType(pb.Type)

	return me
}
