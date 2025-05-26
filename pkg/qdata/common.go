package qdata

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/bwmarrin/snowflake"
	"github.com/cespare/xxhash/v2"
	"github.com/rqure/qlib/pkg/qlog"
	"github.com/rqure/qlib/pkg/qprotobufs"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var snownode *snowflake.Node

type WriteOpt int

type EntityId string
type EntityType string
type FieldType string

type EntityIdSlice []EntityId
type EntityTypeSlice []EntityType
type FieldTypeSlice []FieldType

type WriteTime time.Time

const IndirectionDelimiter = "->"

func (me EntityType) AsString() string {
	return string(me)
}

func (me EntityType) AsInt() int64 {
	return int64(xxhash.Sum64String(me.AsString()))
}

func (me EntityId) AsString() string {
	return string(me)
}

func (me FieldType) AsString() string {
	return string(me)
}

func (me EntityId) AsInt() int64 {
	if me.IsEmpty() {
		qlog.Error("EntityId is empty")
		return -1
	}

	parts := strings.Split(me.AsString(), "$")
	if len(parts) != 2 {
		qlog.Error("Invalid EntityId format: %s", me.AsString())
		return -1
	}

	id, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		qlog.Error("Failed to parse EntityId: %s", err)
		return -1
	}

	return id
}

func GenerateEntityId(entityType EntityType) EntityId {
	if snownode == nil {
		var err error
		snownode, err = snowflake.NewNode(1)
		if err != nil {
			qlog.Panic("Failed to create snowflake node: %v", err)
		}
	}

	return EntityId(fmt.Sprintf("%s$%s", entityType.AsString(), snownode.Generate().String()))
}

func CastSlice[I any, O any](i []I, convert func(I) O) []O {
	o := make([]O, 0, len(i))
	for _, v := range i {
		o = append(o, convert(v))
	}

	return o
}

func CopySlice[I any](i []I) []I {
	o := make([]I, 0, len(i))
	o = append(o, i...)
	return o
}

func CastStringSliceToEntityIdSlice(i []string) []EntityId {
	return CastSlice(i, func(s string) EntityId {
		return EntityId(s)
	})
}

func CastEntityIdSliceToStringSlice(i []EntityId) []string {
	return CastSlice(i, func(e EntityId) string {
		return string(e)
	})
}

func CastToInterfaceSlice[T any](i []T) []any {
	return CastSlice(i, func(t T) any {
		return t
	})
}

func (me EntityIdSlice) AsStringSlice() []string {
	return CastSlice(me, func(e EntityId) string {
		return string(e)
	})
}

func (me EntityIdSlice) FromStringSlice(i []string) EntityIdSlice {
	if me == nil {
		return nil
	}

	for _, s := range i {
		me = append(me, EntityId(s))
	}

	return me
}

func (me EntityTypeSlice) AsStringSlice() []string {
	return CastSlice(me, func(e EntityType) string {
		return string(e)
	})
}

func (me FieldTypeSlice) AsStringSlice() []string {
	return CastSlice(me, func(e FieldType) string {
		return string(e)
	})
}

func (me FieldTypeSlice) FromStringSlice(i []string) FieldTypeSlice {
	if me == nil {
		return nil
	}

	for _, s := range i {
		me = append(me, FieldType(s))
	}

	return me
}

func (me EntityTypeSlice) FromStringSlice(i []string) EntityTypeSlice {
	if me == nil {
		return nil
	}

	for _, s := range i {
		me = append(me, EntityType(s))
	}

	return me
}

func (me EntityId) GetEntityType() EntityType {
	return EntityType(strings.Split(me.AsString(), "$")[0])
}

func (me *FieldType) FromString(s string) *FieldType {
	if me != nil {
		*me = FieldType(s)
	}

	return me
}

func (me *EntityId) FromString(s string) *EntityId {
	if me != nil {
		*me = EntityId(s)
	}

	return me
}

func (me *EntityType) FromString(s string) *EntityType {
	if me != nil {
		*me = EntityType(s)
	}

	return me
}

func (me *EntityId) IsEmpty() bool {
	return *me == ""
}

func (me FieldType) IsListIndex() bool {
	if _, err := strconv.Atoi(me.AsString()); err == nil {
		return true
	}

	return false
}

func (me FieldType) AsListIndex() int {
	i, _ := strconv.Atoi(me.AsString())
	return i
}

func (me FieldType) IsIndirection() bool {
	return strings.Contains(me.AsString(), IndirectionDelimiter)
}

func (me FieldType) AsIndirectionArray() []FieldType {
	fields := strings.Split(me.AsString(), IndirectionDelimiter)
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

func (me *WriteTime) AsTimestampPb2() *timestamppb.Timestamp {
	if me == nil {
		return nil
	}

	return timestamppb.New(time.Time(*me))
}

func (me *WriteTime) FromTime(t time.Time) *WriteTime {
	if me == nil {
		return nil
	}

	*me = WriteTime(t)

	return me
}

func (me *WriteTime) FromUnixNanos(t int64) *WriteTime {
	if me == nil {
		return nil
	}

	*me = WriteTime(time.Unix(0, t))

	return me
}

func (me *WriteTime) AsTime() time.Time {
	if me == nil {
		return time.Unix(0, 0)
	}

	return time.Time(*me)
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

	ReadPermissions  EntityIdSlice
	WritePermissions EntityIdSlice

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
	Err       error
}

func (me *Entity) Clone() *Entity {
	fields := make(map[FieldType]*Field)
	for k, v := range me.Fields {
		fields[k] = v.Clone()
	}

	return new(Entity).Init(me.EntityId, EOFields(fields))
}

func (me *EntitySchema) Clone() *EntitySchema {
	fields := make(map[FieldType]*FieldSchema)
	for k, v := range me.Fields {
		fields[k] = v.Clone()
	}

	return new(EntitySchema).Init(me.EntityType, ESOFields(fields))
}

func (me *Field) Clone() *Field {
	return new(Field).Init(me.EntityId, me.FieldType, FOValue(me.Value.Clone()), FOWriteTime(me.WriteTime), FOWriterId(me.WriterId))
}

func (me *FieldSchema) Clone() *FieldSchema {
	return new(FieldSchema).Init(
		me.EntityType,
		me.FieldType,
		me.ValueType,
		FSOReadPermissions(CopySlice(me.ReadPermissions)),
		FSOWritePermissions(CopySlice(me.WritePermissions)),
		FSOChoices(CopySlice(me.Choices)...),
		FSORank(me.Rank),
	)
}

func (me *Request) Clone() *Request {
	r := new(Request).Init(me.EntityId, me.FieldType, ROValue(me.Value.Clone()))

	if me.WriteTime != nil {
		r = r.ApplyOpts(ROWriteTime(*me.WriteTime))
	}

	if me.WriterId != nil {
		r = r.ApplyOpts(ROWriterId(*me.WriterId))
	}

	return r
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

	return new(Field).Init(me.EntityId, me.FieldType, FOValue(me.Value), FOWriteTime(*wt), FOWriterId(*wId))
}

func (me *Field) AsReadRequest(opts ...RequestOpts) *Request {
	// Typically for read requests, we want to give a direct reference to the value, write time and writer id
	// so it can be properly updated
	return new(Request).Init(me.EntityId, me.FieldType, ROValuePtr(me.Value), ROWriteTimePtr(&me.WriteTime), ROWriterIdPtr(&me.WriterId)).ApplyOpts(opts...)
}

func (me *Field) AsWriteRequest(opts ...RequestOpts) *Request {
	// Typically for write requests, we want to give a direct reference to the write time and writer id
	// We are giving a direct reference to the value for performance reasons such as large binary data
	return new(Request).Init(me.EntityId, me.FieldType, ROValuePtr(me.Value)).ApplyOpts(opts...)
}

func (me *Request) AsRequestPb() *qprotobufs.DatabaseRequest {
	err := ""
	if me.Err != nil {
		err = me.Err.Error()
	}

	var anyValue *anypb.Any
	if me.Value != nil && !me.Value.IsNil() {
		anyValue = me.Value.AsAnyPb()
	}

	return &qprotobufs.DatabaseRequest{
		Id:        string(me.EntityId),
		Field:     string(me.FieldType),
		Value:     anyValue,
		WriteTime: me.WriteTime.AsTimestampPb(),
		WriterId:  me.WriterId.AsStringPb(),
		Err:       err,
		Success:   me.Success,
	}
}

func (me *Request) FromRequestPb(pb *qprotobufs.DatabaseRequest) *Request {
	me.EntityId = EntityId(pb.Id)
	me.FieldType = FieldType(pb.Field)

	me.Value = new(Value).FromAnyPb(pb.Value)

	if pb.WriteTime != nil {
		me.WriteTime = new(WriteTime).FromTime(pb.WriteTime.Raw.AsTime())
	} else {
		me.WriteTime = nil
	}

	if pb.WriterId != nil {
		me.WriterId = new(EntityId).FromString(pb.WriterId.Raw)
	} else {
		me.WriterId = nil
	}

	if pb.Err != "" {
		me.Err = fmt.Errorf("%s", pb.Err)
	} else {
		me.Err = nil
	}

	return me
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

func (me *EntitySchema) FromEntitySchemaPb(pb *qprotobufs.DatabaseEntitySchema) *EntitySchema {
	me.EntityType = EntityType(pb.Name)
	me.Fields = make(map[FieldType]*FieldSchema)

	for _, f := range pb.Fields {
		field := new(FieldSchema).FromFieldSchemaPb(me.EntityType, f)
		me.Fields[field.FieldType] = field
	}

	return me
}

func (me *EntitySchema) AsBytes() ([]byte, error) {
	b, err := proto.Marshal(me.AsEntitySchemaPb())

	// This should never happen
	if err != nil {
		return nil, fmt.Errorf("failed to marshal EntitySchema: %v", err)
	}

	return b, nil
}

func (me *EntitySchema) FromBytes(data []byte) (*EntitySchema, error) {
	pb := new(qprotobufs.DatabaseEntitySchema)
	if err := proto.Unmarshal(data, pb); err != nil {
		return nil, fmt.Errorf("failed to unmarshal EntitySchema: %v", err)
	}

	return me.FromEntitySchemaPb(pb), nil
}

func (me *FieldSchema) AsFieldSchemaPb() *qprotobufs.DatabaseFieldSchema {
	return &qprotobufs.DatabaseFieldSchema{
		Name:             string(me.FieldType),
		Type:             me.ValueType.AsString(),
		Rank:             int32(me.Rank),
		ReadPermissions:  CastEntityIdSliceToStringSlice(me.ReadPermissions),
		WritePermissions: CastEntityIdSliceToStringSlice(me.WritePermissions),
		ChoiceOptions:    me.Choices,
	}
}

func (me *Entity) FromEntityPb(pb *qprotobufs.DatabaseEntity) *Entity {
	me.EntityId = EntityId(pb.Id)
	me.EntityType = EntityType(pb.Type)
	me.Fields = make(map[FieldType]*Field)
	for _, f := range pb.Fields {
		field := new(Field).FromFieldPb(f)
		me.Fields[field.FieldType] = field
	}

	return me
}

func (me *Entity) AsEntityPb() *qprotobufs.DatabaseEntity {
	pb := &qprotobufs.DatabaseEntity{
		Id:   string(me.EntityId),
		Type: string(me.EntityType),
	}

	fields := make([]*qprotobufs.DatabaseField, 0, len(me.Fields))
	for _, f := range me.Fields {
		fields = append(fields, f.AsFieldPb())
	}

	pb.Fields = fields

	return pb
}

func (me *Entity) Field(fieldType FieldType, opts ...FieldOpts) *Field {
	if me.Fields == nil {
		me.Fields = make(map[FieldType]*Field)
	}

	if f, ok := me.Fields[fieldType]; ok {
		return f
	}

	f := new(Field).Init(me.EntityId, fieldType).ApplyOpts(opts...)

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

func (me *Field) FromBytes(data []byte) (*Field, error) {
	pb := new(qprotobufs.DatabaseField)
	if err := proto.Unmarshal(data, pb); err != nil {
		qlog.Error("Failed to unmarshal Field: %v", err)
		return nil, err
	}

	return me.FromFieldPb(pb), nil
}

func (me *Field) AsBytes() ([]byte, error) {
	b, err := proto.Marshal(me.AsFieldPb())

	// This should never happen
	if err != nil {
		qlog.Error("Failed to marshal Field: %v", err)
		return nil, err
	}

	return b, nil
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

func ROWriteTime[T time.Time | WriteTime](t T) RequestOpts {
	return func(r *Request) {
		wt := WriteTime(t)
		r.WriteTime = &wt
	}
}

func ROWriteTimePtr(t *WriteTime) RequestOpts {
	return func(r *Request) {
		r.WriteTime = t
	}
}

func ROWriterId(id EntityId) RequestOpts {
	return func(r *Request) {
		r.WriterId = &id
	}
}

func ROWriterIdPtr(id *EntityId) RequestOpts {
	return func(r *Request) {
		r.WriterId = id
	}
}

func ROValue(v *Value) RequestOpts {
	return func(r *Request) {
		r.Value.FromValue(v)
	}
}

func ROValuePtr(v *Value) RequestOpts {
	return func(r *Request) {
		r.Value = v
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
	me.Value = new(Value).Init()
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

type FieldOpts func(*Field)

func FOEntityId(id EntityId) FieldOpts {
	return func(f *Field) {
		f.EntityId = id
	}
}

func FOFieldType(ft FieldType) FieldOpts {
	return func(f *Field) {
		f.FieldType = ft
	}
}

func FOValue(v *Value) FieldOpts {
	return func(f *Field) {
		f.Value = v
	}
}

func FOWriteTime[T time.Time | WriteTime](t T) FieldOpts {
	return func(f *Field) {
		f.WriteTime = WriteTime(t)
	}
}

func FOWriterId(id EntityId) FieldOpts {
	return func(f *Field) {
		f.WriterId = id
	}
}

type EntityOpts func(*Entity)

func EOField(fieldType FieldType, opts ...FieldOpts) EntityOpts {
	return func(e *Entity) {
		e.Field(fieldType).ApplyOpts(opts...)
	}
}

func EOFields(fields map[FieldType]*Field) EntityOpts {
	return func(e *Entity) {
		e.Fields = fields
	}
}

func EOEntityType(et EntityType) EntityOpts {
	return func(e *Entity) {
		e.EntityType = et
	}
}

func EOEntityId(eid EntityId) EntityOpts {
	return func(e *Entity) {
		e.EntityId = eid
	}
}

type EntitySchemaOpts func(*EntitySchema)

func ESOField(fieldType FieldType, opts ...FieldSchemaOpts) EntitySchemaOpts {
	return func(es *EntitySchema) {
		es.Field(fieldType).ApplyOpts(opts...)
	}
}

func ESOFields(fields map[FieldType]*FieldSchema) EntitySchemaOpts {
	return func(es *EntitySchema) {
		es.Fields = fields
	}
}

func ESOEntityType(et EntityType) EntitySchemaOpts {
	return func(es *EntitySchema) {
		es.EntityType = et
	}
}

type FieldSchemaOpts func(*FieldSchema)

func FSOValueType(vt ValueType) FieldSchemaOpts {
	return func(f *FieldSchema) {
		f.ValueType = vt
	}
}

func FSOReadPermissions(permissions []EntityId) FieldSchemaOpts {
	return func(f *FieldSchema) {
		f.ReadPermissions = permissions
	}
}

func FSOWritePermissions(permissions []EntityId) FieldSchemaOpts {
	return func(f *FieldSchema) {
		f.WritePermissions = permissions
	}
}

func FSOChoices(choices ...string) FieldSchemaOpts {
	return func(f *FieldSchema) {
		f.Choices = choices
	}
}

func FSORank(rank int) FieldSchemaOpts {
	return func(f *FieldSchema) {
		f.Rank = rank
	}
}

func FSOEntityType(et EntityType) FieldSchemaOpts {
	return func(f *FieldSchema) {
		f.EntityType = et
	}
}

func FSOFieldType(ft FieldType) FieldSchemaOpts {
	return func(f *FieldSchema) {
		f.FieldType = ft
	}
}

func (me *Entity) Init(entityId EntityId, opts ...EntityOpts) *Entity {
	me.EntityId = entityId
	me.EntityType = entityId.GetEntityType()
	me.Fields = make(map[FieldType]*Field)
	return me.ApplyOpts(opts...)
}

func (me *Entity) ApplyOpts(opts ...EntityOpts) *Entity {
	for _, o := range opts {
		o(me)
	}

	return me
}

func (me *EntitySchema) Init(entityType EntityType, opts ...EntitySchemaOpts) *EntitySchema {
	me.EntityType = entityType
	me.Fields = make(map[FieldType]*FieldSchema)
	return me.ApplyOpts(opts...)
}

func (me *EntitySchema) ApplyOpts(opts ...EntitySchemaOpts) *EntitySchema {
	for _, o := range opts {
		o(me)
	}

	return me
}

func (me *EntitySchema) Field(fieldType FieldType, opts ...FieldSchemaOpts) *FieldSchema {
	if f, ok := me.Fields[fieldType]; ok {
		return f
	}

	f := new(FieldSchema).Init(me.EntityType, fieldType, ValueType("")).ApplyOpts(opts...)

	me.Fields[fieldType] = f

	return f
}

func (me *Field) Init(entityId EntityId, fieldType FieldType, opts ...FieldOpts) *Field {
	me.EntityId = entityId
	me.FieldType = fieldType
	me.Value = new(Value).Init()
	me.WriteTime = WriteTime(time.Time{})
	me.WriterId = EntityId("")
	return me.ApplyOpts(opts...)
}

func (me *Field) ApplyOpts(opts ...FieldOpts) *Field {
	for _, o := range opts {
		o(me)
	}

	return me
}

func (me *Field) AsFieldPb() *qprotobufs.DatabaseField {
	return &qprotobufs.DatabaseField{
		Id:        string(me.EntityId),
		Name:      string(me.FieldType),
		Value:     me.Value.AsAnyPb(),
		WriteTime: me.WriteTime.AsTimestampPb2(),
		WriterId:  me.WriterId.AsString(),
	}
}

func (me *FieldSchema) Init(entityType EntityType, fieldType FieldType, valueType ValueType, opts ...FieldSchemaOpts) *FieldSchema {
	me.EntityType = entityType
	me.FieldType = fieldType
	me.ValueType = valueType
	me.ReadPermissions = make([]EntityId, 0)
	me.WritePermissions = make([]EntityId, 0)
	return me.ApplyOpts(opts...)
}

func (me *FieldSchema) ApplyOpts(opts ...FieldSchemaOpts) *FieldSchema {
	for _, o := range opts {
		o(me)
	}

	return me
}

func (me *FieldSchema) FromFieldSchemaPb(entityType EntityType, pb *qprotobufs.DatabaseFieldSchema) *FieldSchema {
	me.EntityType = EntityType(entityType)
	me.FieldType = FieldType(pb.Name)
	me.ValueType = ValueType(pb.Type)
	me.ReadPermissions = CastStringSliceToEntityIdSlice(pb.ReadPermissions)
	me.WritePermissions = CastStringSliceToEntityIdSlice(pb.WritePermissions)
	me.Rank = int(pb.Rank)
	me.Choices = pb.ChoiceOptions

	return me
}

func AccumulateErrors(errs ...error) error {
	var errStr []string
	for _, err := range errs {
		if err != nil {
			errStr = append(errStr, err.Error())
		}
	}

	if len(errStr) == 0 {
		return nil
	}

	return fmt.Errorf("one or more errors occurred:\n%s", strings.Join(errStr, ";"))
}
