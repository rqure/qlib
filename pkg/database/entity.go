package entity

import (
	"cmp"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type PushOpt int

const (
	PushNormal PushOpt = iota
	PushIfNotEqual
)

type IField interface {
	PullValue(m proto.Message) proto.Message
	PullInt() int64
	PullFloat() float64
	PullString() string
	PullBool() bool
	PullBinaryFile() string
	PullEntityReference() string
	PullTimestamp() time.Time
	PullTransformation() string
	PullWriteTime() time.Time
	PullWriter() string

	GetValue(m proto.Message) proto.Message
	GetInt() int64
	GetFloat() float64
	GetString() string
	GetBool() bool
	GetBinaryFile() string
	GetEntityReference() string
	GetTimestamp() time.Time
	GetTransformation() string
	GetWriteTime() time.Time
	GetWriter() string
	GetId() string
	GetName() string

	PushValue(m proto.Message) bool
	PushInt(...interface{}) bool
	PushFloat(...interface{}) bool
	PushString(...interface{}) bool
	PushBool(...interface{}) bool
	PushBinaryFile(...interface{}) bool
	PushEntityReference(...interface{}) bool
	PushTimestamp(...interface{}) bool
	PushTransformation(...interface{}) bool
}

type Field struct {
	db  IDatabase
	req *DatabaseRequest
}

func NewField(db IDatabase, entityId string, fieldName string) *Field {
	return &Field{
		db: db,
		req: &DatabaseRequest{
			Id:    entityId,
			Field: fieldName,
		},
	}
}

func NewStringValue(value string) *anypb.Any {
	a, err := anypb.New(&String{Raw: value})
	if err != nil {
		Error("[NewStringValue] Failed to create Any: %s", err.Error())
		return nil
	}

	return a
}

func NewIntValue(value int64) *anypb.Any {
	a, err := anypb.New(&Int{Raw: value})
	if err != nil {
		Error("[NewIntValue] Failed to create Any: %s", err.Error())
		return nil
	}

	return a
}

func NewFloatValue(value float64) *anypb.Any {
	a, err := anypb.New(&Float{Raw: value})
	if err != nil {
		Error("[NewFloatValue] Failed to create Any: %s", err.Error())
		return nil
	}

	return a
}

func NewBoolValue(value bool) *anypb.Any {
	a, err := anypb.New(&Bool{Raw: value})
	if err != nil {
		Error("[NewBoolValue] Failed to create Any: %s", err.Error())
		return nil
	}

	return a
}

func NewBinaryFileValue(value string) *anypb.Any {
	a, err := anypb.New(&BinaryFile{Raw: value})
	if err != nil {
		Error("[NewBinaryFileValue] Failed to create Any: %s", err.Error())
		return nil
	}

	return a
}

func NewEntityReferenceValue(value string) *anypb.Any {
	a, err := anypb.New(&EntityReference{Raw: value})
	if err != nil {
		Error("[NewEntityReferenceValue] Failed to create Any: %s", err.Error())
		return nil
	}

	return a
}

func NewTimestampValue(value time.Time) *anypb.Any {
	a, err := anypb.New(&Timestamp{Raw: timestamppb.New(value)})
	if err != nil {
		Error("[NewTimestampValue] Failed to create Any: %s", err.Error())
		return nil
	}

	return a
}

func NewTransformationValue(value string) *anypb.Any {
	a, err := anypb.New(&Transformation{Raw: value})
	if err != nil {
		Error("[NewTransformationValue] Failed to create Any: %s", err.Error())
		return nil
	}

	return a
}

func (f *Field) PullValue(m proto.Message) proto.Message {
	f.db.Read([]*DatabaseRequest{f.req})

	return f.GetValue(m)
}

func (f *Field) PushValue(m proto.Message) bool {
	a, err := anypb.New(m)
	if err != nil {
		return false
	}

	f.req.Value = a
	f.req.WriteTime = &Timestamp{Raw: timestamppb.Now()}

	f.db.Write([]*DatabaseRequest{f.req})

	return f.req.Success
}

func (f *Field) PullInt() int64 {
	return f.PullValue(new(Int)).(*Int).GetRaw()
}

func (f *Field) PullFloat() float64 {
	return f.PullValue(new(Float)).(*Float).GetRaw()
}

func (f *Field) PullString() string {
	return f.PullValue(new(String)).(*String).GetRaw()
}

func (f *Field) PullBool() bool {
	return f.PullValue(new(Bool)).(*Bool).GetRaw()
}

func (f *Field) PullBinaryFile() string {
	return f.PullValue(new(BinaryFile)).(*BinaryFile).GetRaw()
}

func (f *Field) PullEntityReference() string {
	return f.PullValue(new(EntityReference)).(*EntityReference).GetRaw()
}

func (f *Field) PullTimestamp() time.Time {
	return f.PullValue(new(Timestamp)).(*Timestamp).GetRaw().AsTime()
}

func (f *Field) PullTransformation() string {
	return f.PullValue(new(Transformation)).(*Transformation).GetRaw()
}

func (f *Field) PushInt(args ...interface{}) bool {
	value := int64(0)

	if len(args) > 0 {
		switch v := args[0].(type) {
		case int:
			value = int64(v)
		case int8:
			value = int64(v)
		case int16:
			value = int64(v)
		case int32:
			value = int64(v)
		case int64:
			value = v
		case uint:
			value = int64(v)
		case uint8:
			value = int64(v)
		case uint16:
			value = int64(v)
		case uint32:
			value = int64(v)
		case uint64:
			value = int64(v)
		case float32:
			value = int64(v)
		case float64:
			value = int64(v)
		case bool:
			if v {
				value = 1
			}
		default:
			Error("[Field::PushInt] Unsupported type: %T", v)
			return false
		}
	}

	if len(args) > 1 {
		pushIfNotEqual := args[1].(PushOpt) == PushIfNotEqual

		if pushIfNotEqual && f.PullInt() == value {
			return false
		}
	}

	return f.PushValue(&Int{Raw: value})
}

func (f *Field) PushFloat(args ...interface{}) bool {
	value := float64(0)

	if len(args) > 0 {
		switch v := args[0].(type) {
		case int:
			value = float64(v)
		case int8:
			value = float64(v)
		case int16:
			value = float64(v)
		case int32:
			value = float64(v)
		case int64:
			value = float64(v)
		case uint:
			value = float64(v)
		case uint8:
			value = float64(v)
		case uint16:
			value = float64(v)
		case uint32:
			value = float64(v)
		case uint64:
			value = float64(v)
		case float32:
			value = float64(v)
		case float64:
			value = v
		case bool:
			if v {
				value = 1
			}
		default:
			Error("[Field::PushFloat] Unsupported type: %T", v)
			return false
		}
	}

	if len(args) > 1 {
		pushIfNotEqual := args[1].(PushOpt) == PushIfNotEqual

		if pushIfNotEqual && f.PullFloat() == value {
			return false
		}
	}

	return f.PushValue(&Float{Raw: value})
}

func (f *Field) PushString(args ...interface{}) bool {
	value := ""

	if len(args) > 0 {
		switch v := args[0].(type) {
		case string:
			value = v
		default:
			Error("[Field::PushString] Unsupported type: %T", v)
			return false
		}
	}

	if len(args) > 1 {
		pushIfNotEqual := args[1].(PushOpt) == PushIfNotEqual

		if pushIfNotEqual && f.PullString() == value {
			return false
		}
	}

	return f.PushValue(&String{Raw: value})
}

func (f *Field) PushBool(args ...interface{}) bool {
	value := false

	if len(args) > 0 {
		switch v := args[0].(type) {
		case bool:
			value = v
		case int:
			value = v != 0
		case int8:
			value = v != 0
		case int16:
			value = v != 0
		case int32:
			value = v != 0
		case int64:
			value = v != 0
		case uint:
			value = v != 0
		case uint8:
			value = v != 0
		case uint16:
			value = v != 0
		case uint32:
			value = v != 0
		case uint64:
			value = v != 0
		case float32:
			value = v != 0
		case float64:
			value = v != 0
		default:
			Error("[Field::PushBool] Unsupported type: %T", v)
			return false
		}
	}

	if len(args) > 1 {
		pushIfNotEqual := args[1].(PushOpt) == PushIfNotEqual

		if pushIfNotEqual && f.PullBool() == value {
			return false
		}
	}

	return f.PushValue(&Bool{Raw: value})
}

func (f *Field) PushBinaryFile(args ...interface{}) bool {
	value := ""

	if len(args) > 0 {
		switch v := args[0].(type) {
		case string:
			value = v
		default:
			Error("[Field::PushBinaryFile] Unsupported type: %T", v)
			return false
		}
	}

	if len(args) > 1 {
		pushIfNotEqual := args[1].(PushOpt) == PushIfNotEqual

		if pushIfNotEqual && f.PullBinaryFile() == value {
			return false
		}
	}

	return f.PushValue(&BinaryFile{Raw: value})
}

func (f *Field) PushEntityReference(args ...interface{}) bool {
	value := ""

	if len(args) > 0 {
		switch v := args[0].(type) {
		case string:
			value = v
		default:
			Error("[Field::PushEntityReference] Unsupported type: %T", v)
			return false
		}
	}

	if len(args) > 1 {
		pushIfNotEqual := args[1].(PushOpt) == PushIfNotEqual

		if pushIfNotEqual && f.PullEntityReference() == value {
			return false
		}
	}

	// Check if entity exists
	if value != "" && f.db.GetEntity(value) == nil {
		Error("[Field::PushEntityReference] Entity does not exist: %s", value)
		return false
	}

	return f.PushValue(&EntityReference{Raw: value})
}

func (f *Field) PushTimestamp(args ...interface{}) bool {
	value := time.Now()

	if len(args) > 0 {
		switch v := args[0].(type) {
		case int:
			value = time.Unix(int64(v), 0)
		case uint:
			value = time.Unix(int64(v), 0)
		case int64:
			value = time.Unix(v, 0)
		case uint64:
			value = time.Unix(int64(v), 0)
		case float32:
			value = time.Unix(int64(v), 0)
		case float64:
			value = time.Unix(int64(v), 0)
		case string:
			t, err := time.Parse(time.RFC3339, v)
			if err != nil {
				Error("[Field::PushTimestamp] Failed to parse time: %s", err.Error())
				return false
			}
			value = t
		case time.Time:
			value = v
		default:
			Error("[Field::PushTimestamp] Unsupported type: %T", v)
			return false
		}
	}

	if len(args) > 1 {
		pushIfNotEqual := args[1].(PushOpt) == PushIfNotEqual

		if pushIfNotEqual && f.PullTimestamp() == value {
			return false
		}
	}

	return f.PushValue(&Timestamp{Raw: timestamppb.New(value)})
}

func (f *Field) PushTransformation(args ...interface{}) bool {
	value := ""

	if len(args) > 0 {
		switch v := args[0].(type) {
		case string:
			value = v
		default:
			Error("[Field::PushTransformation] Unsupported type: %T", v)
			return false
		}
	}

	if len(args) > 1 {
		pushIfNotEqual := args[1].(PushOpt) == PushIfNotEqual

		if pushIfNotEqual && f.PullTransformation() == value {
			return false
		}
	}

	return f.PushValue(&Transformation{Raw: value})
}

func (f *Field) PullWriteTime() time.Time {
	f.db.Read([]*DatabaseRequest{f.req})

	return f.GetWriteTime()
}

func (f *Field) PullWriter() string {
	f.db.Read([]*DatabaseRequest{f.req})

	return f.GetWriter()
}

func (f *Field) GetValue(m proto.Message) proto.Message {
	if !f.req.Success {
		return m
	}

	if err := f.req.Value.UnmarshalTo(m); err != nil {
		return m
	}

	return m
}

func (f *Field) GetInt() int64 {
	return f.GetValue(new(Int)).(*Int).GetRaw()
}

func (f *Field) GetFloat() float64 {
	return f.GetValue(new(Float)).(*Float).GetRaw()
}

func (f *Field) GetString() string {
	return f.GetValue(new(String)).(*String).GetRaw()
}

func (f *Field) GetBool() bool {
	return f.GetValue(new(Bool)).(*Bool).GetRaw()
}

func (f *Field) GetBinaryFile() string {
	return f.GetValue(new(BinaryFile)).(*BinaryFile).GetRaw()
}

func (f *Field) GetEntityReference() string {
	return f.GetValue(new(EntityReference)).(*EntityReference).GetRaw()
}

func (f *Field) GetTimestamp() time.Time {
	return f.GetValue(new(Timestamp)).(*Timestamp).GetRaw().AsTime()
}

func (f *Field) GetTransformation() string {
	return f.GetValue(new(Transformation)).(*Transformation).GetRaw()
}

func (f *Field) GetWriteTime() time.Time {
	if !f.req.Success {
		return time.Time{}
	}

	return f.req.WriteTime.GetRaw().AsTime()
}

func (f *Field) GetWriter() string {
	if !f.req.Success {
		return ""
	}

	return f.req.WriterId.GetRaw()
}

func (f *Field) GetId() string {
	return f.req.Id
}

func (f *Field) GetName() string {
	return f.req.Field
}

type IEntity interface {
	GetId() string
	GetType() string
	GetName() string
	GetChildren() []*EntityReference
	GetParent() *EntityReference
	GetField(string) IField
}

type Entity struct {
	db     IDatabase
	entity *DatabaseEntity
}

func NewEntity(db IDatabase, entityId string) *Entity {
	return &Entity{
		db:     db,
		entity: db.GetEntity(entityId),
	}
}

func (e *Entity) GetId() string {
	return e.entity.Id
}

func (e *Entity) GetType() string {
	return e.entity.Type
}

func (e *Entity) GetName() string {
	return e.entity.Name
}

func (e *Entity) GetChildren() []*EntityReference {
	return e.entity.Children
}

func (e *Entity) GetParent() *EntityReference {
	return e.entity.Parent
}

func (e *Entity) GetField(fieldName string) IField {
	return NewField(e.db, e.GetId(), fieldName)
}

type IFieldProto[T comparable] interface {
	protoreflect.ProtoMessage
	GetRaw() T
}

func DefaultCaster[A any, B any](in A) B {
	return any(in).(B)
}

type FieldConditionEval func(IDatabase, string) bool
type FieldCondition[T IFieldProto[K], C cmp.Ordered, K comparable] struct {
	Lhs      string
	LhsValue T
	Caster   func(K) C
}

func (f *FieldCondition[T, C, K]) Where(lhs string) *FieldCondition[T, C, K] {
	f.Lhs = lhs
	return f
}

func (f *FieldCondition[T, C, K]) IsEqualTo(rhs T) FieldConditionEval {
	return func(db IDatabase, entityId string) bool {
		request := &DatabaseRequest{
			Id:    entityId,
			Field: f.Lhs,
		}
		db.Read([]*DatabaseRequest{request})

		if !request.Success {
			return false
		}

		if !request.Value.MessageIs(f.LhsValue) {
			return false
		}

		lhsValue, err := request.Value.UnmarshalNew()
		if err != nil {
			Error("[FieldCondition::IsEqualTo] Failed to unmarshal value: %s", err.Error())
			return false
		}
		f.LhsValue = lhsValue.(T)

		return f.Caster(f.LhsValue.GetRaw()) == f.Caster(rhs.GetRaw())
	}
}

func (f *FieldCondition[T, C, K]) IsNotEqualTo(rhs T) FieldConditionEval {
	return func(db IDatabase, entityId string) bool {
		request := &DatabaseRequest{
			Id:    entityId,
			Field: f.Lhs,
		}
		db.Read([]*DatabaseRequest{request})

		if !request.Success {
			return false
		}

		if !request.Value.MessageIs(f.LhsValue) {
			return false
		}

		lhsValue, err := request.Value.UnmarshalNew()
		if err != nil {
			Error("[FieldCondition::IsNotEqualTo] Failed to unmarshal value: %s", err.Error())
			return false
		}
		f.LhsValue = lhsValue.(T)

		return f.Caster(f.LhsValue.GetRaw()) != f.Caster(rhs.GetRaw())
	}
}

func (f *FieldCondition[T, C, K]) IsGreaterThan(rhs T) FieldConditionEval {
	return func(db IDatabase, entityId string) bool {
		request := &DatabaseRequest{
			Id:    entityId,
			Field: f.Lhs,
		}
		db.Read([]*DatabaseRequest{request})

		if !request.Success {
			return false
		}

		if !request.Value.MessageIs(f.LhsValue) {
			return false
		}

		lhsValue, err := request.Value.UnmarshalNew()
		if err != nil {
			Error("[FieldCondition::IsGreaterThan] Failed to unmarshal value: %s", err.Error())
			return false
		}
		f.LhsValue = lhsValue.(T)

		return f.Caster(f.LhsValue.GetRaw()) > f.Caster(rhs.GetRaw())
	}
}

func (f *FieldCondition[T, C, K]) IsLessThan(rhs T) FieldConditionEval {
	return func(db IDatabase, entityId string) bool {
		request := &DatabaseRequest{
			Id:    entityId,
			Field: f.Lhs,
		}
		db.Read([]*DatabaseRequest{request})

		if !request.Success {
			return false
		}

		if !request.Value.MessageIs(f.LhsValue) {
			return false
		}

		lhsValue, err := request.Value.UnmarshalNew()
		if err != nil {
			Error("[FieldCondition::IsLessThan] Failed to unmarshal value: %s", err.Error())
			return false
		}
		f.LhsValue = lhsValue.(T)

		return f.Caster(f.LhsValue.GetRaw()) < f.Caster(rhs.GetRaw())
	}
}

func (f *FieldCondition[T, C, K]) IsGreaterThanOrEqualTo(rhs T) FieldConditionEval {
	return func(db IDatabase, entityId string) bool {
		request := &DatabaseRequest{
			Id:    entityId,
			Field: f.Lhs,
		}
		db.Read([]*DatabaseRequest{request})

		if !request.Success {
			return false
		}

		if !request.Value.MessageIs(f.LhsValue) {
			return false
		}

		lhsValue, err := request.Value.UnmarshalNew()
		if err != nil {
			Error("[FieldCondition::IsGreaterThanOrEqualTo] Failed to unmarshal value: %s", err.Error())
			return false
		}
		f.LhsValue = lhsValue.(T)

		return f.Caster(f.LhsValue.GetRaw()) >= f.Caster(rhs.GetRaw())
	}
}

func (f *FieldCondition[T, C, K]) IsLessThanOrEqualTo(rhs T) FieldConditionEval {
	return func(db IDatabase, entityId string) bool {
		request := &DatabaseRequest{
			Id:    entityId,
			Field: f.Lhs,
		}
		db.Read([]*DatabaseRequest{request})

		if !request.Success {
			return false
		}

		if !request.Value.MessageIs(f.LhsValue) {
			return false
		}

		lhsValue, err := request.Value.UnmarshalNew()
		if err != nil {
			return false
		}
		f.LhsValue = lhsValue.(T)

		return f.Caster(f.LhsValue.GetRaw()) <= f.Caster(rhs.GetRaw())
	}
}

func (f *FieldCondition[T, C, K]) IsBetween(lower T, upper T) FieldConditionEval {
	return func(db IDatabase, entityId string) bool {
		request := &DatabaseRequest{
			Id:    entityId,
			Field: f.Lhs,
		}
		db.Read([]*DatabaseRequest{request})

		if !request.Success {
			return false
		}

		if !request.Value.MessageIs(f.LhsValue) {
			return false
		}

		lhsValue, err := request.Value.UnmarshalNew()
		if err != nil {
			Error("[FieldCondition::IsBetween] Failed to unmarshal value: %s", err.Error())
			return false
		}
		f.LhsValue = lhsValue.(T)

		return f.Caster(f.LhsValue.GetRaw()) >= f.Caster(lower.GetRaw()) && f.Caster(f.LhsValue.GetRaw()) <= f.Caster(upper.GetRaw())
	}
}

func (f *FieldCondition[T, C, K]) IsIn(values []T) FieldConditionEval {
	return func(db IDatabase, entityId string) bool {
		request := &DatabaseRequest{
			Id:    entityId,
			Field: f.Lhs,
		}
		db.Read([]*DatabaseRequest{request})

		if !request.Success {
			return false
		}

		if !request.Value.MessageIs(f.LhsValue) {
			return false
		}

		lhsValue, err := request.Value.UnmarshalNew()
		if err != nil {
			Error("[FieldCondition::IsIn] Failed to unmarshal value: %s", err.Error())
			return false
		}
		f.LhsValue = lhsValue.(T)

		for _, value := range values {
			if f.Caster(f.LhsValue.GetRaw()) == f.Caster(value.GetRaw()) {
				return true
			}
		}

		return false
	}
}

func (f *FieldCondition[T, C, K]) IsNotIn(values []T) FieldConditionEval {
	return func(db IDatabase, entityId string) bool {
		request := &DatabaseRequest{
			Id:    entityId,
			Field: f.Lhs,
		}
		db.Read([]*DatabaseRequest{request})

		if !request.Success {
			return false
		}

		if !request.Value.MessageIs(f.LhsValue) {
			return false
		}

		lhsValue, err := request.Value.UnmarshalNew()
		if err != nil {
			Error("[FieldCondition::IsNotIn] Failed to unmarshal value: %s", err.Error())
			return false
		}
		f.LhsValue = lhsValue.(T)

		for _, value := range values {
			if f.Caster(f.LhsValue.GetRaw()) == f.Caster(value.GetRaw()) {
				return false
			}
		}

		return true
	}
}

type SearchCriteria struct {
	EntityType string
	Conditions []FieldConditionEval
}

type IEntityFinder interface {
	Find(SearchCriteria) []IEntity
}

type EntityFinder struct {
	db IDatabase
}

func NewEntityFinder(db IDatabase) *EntityFinder {
	return &EntityFinder{
		db: db,
	}
}

func (f *EntityFinder) Find(criteria SearchCriteria) []IEntity {
	entities := make([]IEntity, 0)

	for _, entityId := range f.db.FindEntities(criteria.EntityType) {
		allConditionsMet := true

		for _, condition := range criteria.Conditions {
			if !condition(f.db, entityId) {
				allConditionsMet = false
				break
			}
		}

		if allConditionsMet {
			entities = append(entities, NewEntity(f.db, entityId))
		}
	}

	return entities
}

type FCString = FieldCondition[*String, string, string]
type FCBool = FieldCondition[*Bool, int, bool]
type FCInt = FieldCondition[*Int, int64, int64]
type FCFloat = FieldCondition[*Float, float64, float64]
type FCEnum[T ~int32] struct {
	FieldCondition[IFieldProto[T], T, T]
}
type FCTimestamp = FieldCondition[*Timestamp, int64, *timestamppb.Timestamp]
type FCReference = FieldCondition[*EntityReference, string, string]

func NewStringCondition() *FCString {
	return &FCString{
		Caster: DefaultCaster[string, string],
	}
}

func NewBoolCondition() *FCBool {
	return &FCBool{
		Caster: func(b bool) int {
			if b {
				return 1
			}
			return 0
		},
	}
}

func NewIntCondition() *FCInt {
	return &FCInt{
		Caster: DefaultCaster[int64, int64],
	}
}

func NewFloatCondition() *FCFloat {
	return &FCFloat{
		Caster: DefaultCaster[float64, float64],
	}
}

func NewEnumCondition[T ~int32]() *FCEnum[T] {
	return &FCEnum[T]{
		FieldCondition: FieldCondition[IFieldProto[T], T, T]{
			Caster: DefaultCaster[T, T],
		},
	}
}

func NewTimestampCondition() *FCTimestamp {
	return &FCTimestamp{
		Caster: func(t *timestamppb.Timestamp) int64 {
			return t.AsTime().UnixMilli()
		},
	}
}

func NewReferenceCondition() *FCReference {
	return &FCReference{
		Caster: DefaultCaster[string, string],
	}
}
