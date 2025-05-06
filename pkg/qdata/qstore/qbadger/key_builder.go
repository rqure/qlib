package qbadger

import (
	"fmt"
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

func (me *KeyBuilder) GetEntityPrefix() string {
	return "entity"
}

func (me *KeyBuilder) GetFieldPrefix() string {
	return "field"
}

func (me *KeyBuilder) GetSchemaPrefix() string {
	return "schema"
}

func (me *KeyBuilder) GetEntityKey(entityId qdata.EntityId) string {
	return me.BuildKey(me.GetEntityPrefix(), entityId.AsString())
}

func (me *KeyBuilder) GetEntityFieldKey(entityId qdata.EntityId, fieldType qdata.FieldType) string {
	return me.BuildKey(me.GetFieldPrefix(), entityId.AsString(), fieldType.AsString())
}

func (me *KeyBuilder) GetSchemaKey(entityType qdata.EntityType) string {
	return me.BuildKey(me.GetSchemaPrefix(), entityType.AsString(), fmt.Sprintf("%d", entityType.AsInt()))
}

// ExtractEntityIdFromKey extracts the EntityId from a key
func (me *KeyBuilder) ExtractEntityIdFromKey(key string) (qdata.EntityId, error) {
	parts := strings.Split(key, ":")
	if len(parts) < 3 {
		return "", fmt.Errorf("invalid key format: %s", key)
	}
	return qdata.EntityId(parts[2]), nil
}

// ExtractEntityTypeFromKey extracts the EntityType from a schema key
func (me *KeyBuilder) ExtractEntityTypeFromKey(key string) (qdata.EntityType, error) {
	parts := strings.Split(key, ":")
	if len(parts) < 3 {
		return "", fmt.Errorf("invalid key format: %s", key)
	}

	return qdata.EntityType(parts[2]), nil
}
