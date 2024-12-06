package data

import (
	"google.golang.org/protobuf/types/known/timestamppb"
)

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
