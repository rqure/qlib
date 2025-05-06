package qbadger

import (
	"fmt"
	"strconv"
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
	return me.BuildKey(me.GetSchemaPrefix(), strconv.FormatInt(entityType.AsInt(), 10), entityType.AsString())
}

// ExtractEntityIdFromKey extracts the EntityId from a key
func (me *KeyBuilder) ExtractEntityIdFromKey(key string) (qdata.EntityId, error) {
	parts := strings.Split(key, ":")
	if len(parts) < 3 {
		return "", fmt.Errorf("invalid key format: %s", key)
	}

	// If it's an entity key, it has format prefix:entity:entityId
	if parts[1] == me.GetEntityPrefix() && len(parts) >= 3 {
		return qdata.EntityId(parts[2]), nil
	}

	// If it's a field key, it has format prefix:field:entityId:fieldType
	if parts[1] == me.GetFieldPrefix() && len(parts) >= 3 {
		return qdata.EntityId(parts[2]), nil
	}

	return "", fmt.Errorf("invalid key format: %s", key)
}

// ExtractEntityTypeFromKey extracts the EntityType from a schema key
func (me *KeyBuilder) ExtractEntityTypeFromKey(key string) (qdata.EntityType, error) {
	parts := strings.Split(key, ":")
	// Schema keys have format prefix:schema:entityTypeId:entityTypeName
	if len(parts) < 4 || parts[1] != me.GetSchemaPrefix() {
		return "", fmt.Errorf("invalid schema key format: %s", key)
	}

	return qdata.EntityType(parts[3]), nil
}
