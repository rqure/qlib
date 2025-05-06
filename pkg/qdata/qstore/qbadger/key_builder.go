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

func (me *KeyBuilder) GetEntityKey(entityId qdata.EntityId) string {
	return me.BuildKey("entity", entityId.AsString())
}

func (me *KeyBuilder) GetEntityFieldKey(entityId qdata.EntityId, fieldType qdata.FieldType) string {
	return me.BuildKey("entity", entityId.AsString(), fieldType.AsString())
}

func (me *KeyBuilder) GetSchemaKey(entityType qdata.EntityType) string {
	return me.BuildKey("schema", entityType.AsString())
}

// GetNumericSchemaKey returns a schema key with numeric entity type ID for ordering
func (me *KeyBuilder) GetNumericSchemaKey(entityType qdata.EntityType) string {
	return me.BuildKey("schema", fmt.Sprintf("%s:%d", entityType.AsString(), entityType.AsInt()))
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

	// Check if the format includes numeric ID
	typeParts := strings.Split(parts[2], ":")
	if len(typeParts) > 0 {
		return qdata.EntityType(typeParts[0]), nil
	}

	return qdata.EntityType(parts[2]), nil
}
