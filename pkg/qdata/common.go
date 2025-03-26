package qdata

import (
	"strconv"
	"strings"
	"time"

	"github.com/rqure/qlib/pkg/qprotobufs"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type WriteOpt int

type EntityId string
type EntityType string
type FieldType string

type WriteTime time.Time

func (me *EntityType) AsString() string {
	return string(*me)
}

func (me *EntityId) AsString() string {
	return string(*me)
}

func (me *FieldType) AsString() string {
	return string(*me)
}

func (me *FieldType) FromString(s string) FieldType {
	*me = FieldType(s)
	return *me
}

func (me *EntityId) FromString(s string) EntityId {
	*me = EntityId(s)
	return *me
}

func (me *EntityType) FromString(s string) EntityType {
	*me = EntityType(s)
	return *me
}

func (me *EntityId) IsEmpty() bool {
	return *me == ""
}

func (me *FieldType) IsListIndex() bool {
	if _, err := strconv.Atoi(me.AsString()); err == nil {
		return true
	}

	return false
}

func (me *FieldType) AsListIndex() int {
	i, _ := strconv.Atoi(me.AsString())
	return i
}

func (me *FieldType) IsIndirection() bool {
	return strings.Contains(me.AsString(), "->")
}

func (me *FieldType) AsIndirectionArray() []FieldType {
	fields := strings.Split(me.AsString(), "->")
	result := make([]FieldType, 0, len(fields))

	for _, f := range fields {
		result = append(result, FieldType(f))
	}

	return result
}

func (me *WriteTime) AsTimestampPb() *qprotobufs.Timestamp {
	if me == nil {
		return nil
	}

	return &qprotobufs.Timestamp{
		Raw: timestamppb.New(time.Time(*me)),
	}
}

func (me *WriteTime) Update(t time.Time) {
	if me == nil {
		return
	}

	*me = WriteTime(t)
}

func (me *EntityId) Update(id string) {
	if me == nil {
		return
	}

	*me = EntityId(id)
}

func (me *EntityId) AsStringPb() *qprotobufs.String {
	if me == nil {
		return nil
	}

	return &qprotobufs.String{
		Raw: me.AsString(),
	}
}

func (me *EntityId) AsEntityReferencePb() *qprotobufs.EntityReference {
	if me == nil {
		return nil
	}

	return &qprotobufs.EntityReference{
		Raw: me.AsString(),
	}
}

const (
	WriteNormal WriteOpt = iota
	WriteChanges
)

type Entity struct {
	EntityId   EntityId
	EntityType EntityType

	Fields map[FieldType]*Field
}

type EntitySchema struct {
	EntityType EntityType
	Fields     map[FieldType]*FieldSchema
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
	Rank       int

	ReadPermissions  []EntityId
	WritePermissions []EntityId

	Choices []string
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
	fields := make(map[FieldType]*Field)
	for k, v := range me.Fields {
		fields[k] = v.Clone()
	}

	return &Entity{
		EntityId:   me.EntityId,
		EntityType: me.EntityType,
		Fields:     fields,
	}
}

func (me *EntitySchema) Clone() *EntitySchema {
	fields := make(map[FieldType]*FieldSchema)
	for k, v := range me.Fields {
		fields[k] = v.Clone()
	}

	return &EntitySchema{
		EntityType: me.EntityType,
		Fields:     fields,
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
	return &Request{
		EntityId:  me.EntityId,
		FieldType: me.FieldType,
		Value:     me.Value, // Don't clone so the field is updated
		WriteOpt:  WriteNormal,
		WriteTime: &me.WriteTime,
		WriterId:  &me.WriterId,
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

func (me *EntitySchema) AsEntitySchemaPb() *qprotobufs.DatabaseEntitySchema {
	fields := make([]*qprotobufs.DatabaseFieldSchema, 0, len(me.Fields))
	for _, f := range me.Fields {
		fields = append(fields, f.AsFieldSchemaPb())
	}

	return &qprotobufs.DatabaseEntitySchema{
		Name:   string(me.EntityType),
		Fields: fields,
	}
}

func (me *FieldSchema) AsFieldSchemaPb() *qprotobufs.DatabaseFieldSchema {
	readPermissions := make([]string, len(me.ReadPermissions))
	writePermissions := make([]string, len(me.WritePermissions))

	for i, p := range me.ReadPermissions {
		readPermissions[i] = string(p)
	}

	for i, p := range me.WritePermissions {
		writePermissions[i] = string(p)
	}

	return &qprotobufs.DatabaseFieldSchema{
		Name:             string(me.FieldType),
		Type:             me.ValueType.ProtobufName(),
		ReadPermissions:  readPermissions,
		WritePermissions: writePermissions,
	}
}

func (me *Entity) FromEntityPb(pb *qprotobufs.DatabaseEntity) *Entity {
	me.EntityId = EntityId(pb.Id)
	me.EntityType = EntityType(pb.Type)

	return me
}

func (me *Entity) Field(fieldType FieldType) *Field {
	if me.Fields == nil {
		me.Fields = make(map[FieldType]*Field)
	}

	if f, ok := me.Fields[fieldType]; ok {
		return f
	}

	f := &Field{
		EntityId:  me.EntityId,
		FieldType: fieldType,
		Value:     new(Value),
	}

	me.Fields[fieldType] = f

	return f
}

func (me *Field) FromFieldPb(pb *qprotobufs.DatabaseField) *Field {
	me.EntityId = EntityId(pb.Id)
	me.FieldType = FieldType(pb.Name)
	me.Value = new(Value).FromAnyPb(pb.Value)
	me.WriteTime = WriteTime(pb.WriteTime.AsTime())
	me.WriterId = EntityId(pb.WriterId)

	return me
}

type RequestOpts func(*Request)

func ROWriteNormal() RequestOpts {
	return func(r *Request) {
		r.WriteOpt = WriteNormal
	}
}

func ROWriteChanges() RequestOpts {
	return func(r *Request) {
		r.WriteOpt = WriteChanges
	}
}

func ROWriteTime(t time.Time) RequestOpts {
	return func(r *Request) {
		wt := WriteTime(t)
		r.WriteTime = &wt
	}
}

func ROWriterId(id EntityId) RequestOpts {
	return func(r *Request) {
		r.WriterId = &id
	}
}

func ROValue(v *Value) RequestOpts {
	return func(r *Request) {
		r.Value.Update(v)
	}
}

func ROEntityId(id EntityId) RequestOpts {
	return func(r *Request) {
		r.EntityId = id
	}
}

func ROFieldType(ft FieldType) RequestOpts {
	return func(r *Request) {
		r.FieldType = ft
	}
}

func (me *Request) Init(entityId EntityId, fieldType FieldType, opts ...RequestOpts) *Request {
	me.EntityId = entityId
	me.FieldType = fieldType
	me.Value = new(Value)
	me.WriteOpt = WriteNormal
	me.WriteTime = nil
	me.WriterId = nil
	me.Success = false
	return me.ApplyOpts(opts...)
}

func (me *Request) ApplyOpts(opts ...RequestOpts) *Request {
	for _, o := range opts {
		o(me)
	}

	return me
}
