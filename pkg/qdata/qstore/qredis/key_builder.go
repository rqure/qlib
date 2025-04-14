package qredis

import (
	"strings"

	"github.com/rqure/qlib/pkg/qdata"
)

type KeyBuilder struct {
	Prefix string
}

func NewKeyBuilder(prefix string) *KeyBuilder {
	return &KeyBuilder{
		Prefix: prefix,
	}
}

func (kb *KeyBuilder) BuildKey(parts ...string) string {
	return strings.Join(append([]string{kb.Prefix}, parts...), ":")
}

func (me *KeyBuilder) GetEntityKey(entityId qdata.EntityId) string {
	return me.BuildKey("entity", entityId.AsString())
}

func (me *KeyBuilder) GetSchemaKey(entityType qdata.EntityType) string {
	return me.BuildKey("schema", entityType.AsString())
}

func (me *KeyBuilder) GetAllEntityTypesKey() string {
	return me.BuildKey("all-entitytypes")
}

func (me *KeyBuilder) GetAllEntitiesKey(entityType qdata.EntityType) string {
	return me.BuildKey("all-entities", entityType.AsString())
}

func (me *KeyBuilder) GetReverseReferenceFieldKey(entityId qdata.EntityId) string {
	return me.BuildKey("reverse-reference", "field", entityId.AsString())
}

func (me *KeyBuilder) GetReverseReferenceSchemaKey(entityId string) string {
	return me.BuildKey("reverse-reference", "schema", entityId)
}
