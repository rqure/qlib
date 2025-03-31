package qpostgres

import (
	"encoding/json"

	"github.com/rqure/qlib/pkg/qdata"
	"github.com/rqure/qlib/pkg/qlog"
)

// SerializableEntity is a simplified version of Entity for serialization
type SerializableEntity struct {
	ID   string `json:"id"`
	Type string `json:"type"`
}

// SerializableValue wraps Value for serialization
type SerializableValue struct {
	Raw       interface{} `json:"raw"`
	ValueType string      `json:"type"`
}

// SerializableFieldData contains field data for serialization
type SerializableFieldData struct {
	EntityID  string            `json:"entity_id"`
	FieldType string            `json:"field_type"`
	Value     SerializableValue `json:"value"`
	WriteTime int64             `json:"write_time"` // Unix timestamp
	WriterID  string            `json:"writer_id"`
}

// SerializableFieldSchema represents a field schema for serialization
type SerializableFieldSchema struct {
	EntityType       string   `json:"entity_type"`
	FieldType        string   `json:"field_type"`
	ValueType        string   `json:"value_type"`
	Rank             int      `json:"rank"`
	ReadPermissions  []string `json:"read_permissions"`
	WritePermissions []string `json:"write_permissions"`
	Choices          []string `json:"choices,omitempty"`
}

// SerializeEntity converts an Entity to a byte array
func SerializeEntity(entity *qdata.Entity) ([]byte, error) {
	se := SerializableEntity{
		ID:   string(entity.EntityId),
		Type: string(entity.EntityType),
	}
	return json.Marshal(se)
}

// DeserializeEntity converts a byte array to an Entity
func DeserializeEntity(data []byte) (*qdata.Entity, error) {
	var se SerializableEntity
	if err := json.Unmarshal(data, &se); err != nil {
		return nil, err
	}

	return &qdata.Entity{
		EntityId:   qdata.EntityId(se.ID),
		EntityType: qdata.EntityType(se.Type),
	}, nil
}

// SerializeFieldSchema converts a FieldSchema to a byte array
func SerializeFieldSchema(schema *qdata.FieldSchema) ([]byte, error) {
	readPerms := make([]string, len(schema.ReadPermissions))
	for i, p := range schema.ReadPermissions {
		readPerms[i] = string(p)
	}

	writePerms := make([]string, len(schema.WritePermissions))
	for i, p := range schema.WritePermissions {
		writePerms[i] = string(p)
	}

	sfs := SerializableFieldSchema{
		EntityType:       string(schema.EntityType),
		FieldType:        string(schema.FieldType),
		ValueType:        string(schema.ValueType),
		Rank:             schema.Rank,
		ReadPermissions:  readPerms,
		WritePermissions: writePerms,
		Choices:          schema.Choices,
	}

	return json.Marshal(sfs)
}

// DeserializeFieldSchema converts a byte array to a FieldSchema
func DeserializeFieldSchema(data []byte) (*qdata.FieldSchema, error) {
	var sfs SerializableFieldSchema
	if err := json.Unmarshal(data, &sfs); err != nil {
		return nil, err
	}

	readPerms := make([]qdata.EntityId, len(sfs.ReadPermissions))
	for i, p := range sfs.ReadPermissions {
		readPerms[i] = qdata.EntityId(p)
	}

	writePerms := make([]qdata.EntityId, len(sfs.WritePermissions))
	for i, p := range sfs.WritePermissions {
		writePerms[i] = qdata.EntityId(p)
	}

	return &qdata.FieldSchema{
		EntityType:       qdata.EntityType(sfs.EntityType),
		FieldType:        qdata.FieldType(sfs.FieldType),
		ValueType:        qdata.ValueType(sfs.ValueType),
		Rank:             sfs.Rank,
		ReadPermissions:  readPerms,
		WritePermissions: writePerms,
		Choices:          sfs.Choices,
	}, nil
}

// SerializeFieldData serializes field data for caching
func SerializeFieldData(entityId qdata.EntityId, fieldType qdata.FieldType, value *qdata.Value, writeTime qdata.WriteTime, writerId qdata.EntityId) ([]byte, error) {
	sfd := SerializableFieldData{
		EntityID:  string(entityId),
		FieldType: string(fieldType),
		Value: SerializableValue{
			Raw:       value.GetRaw(),
			ValueType: string(value.Type()),
		},
		WriteTime: writeTime.AsTime().UnixNano(),
		WriterID:  string(writerId),
	}

	return json.Marshal(sfd)
}

// DeserializeFieldData deserializes field data from cache
func DeserializeFieldData(data []byte) (qdata.EntityId, qdata.FieldType, *qdata.Value, qdata.WriteTime, qdata.EntityId, error) {
	var sfd SerializableFieldData
	if err := json.Unmarshal(data, &sfd); err != nil {
		return "", "", nil, qdata.WriteTime{}, "", err
	}

	entityId := qdata.EntityId(sfd.EntityID)
	fieldType := qdata.FieldType(sfd.FieldType)

	// Create value
	valueType := qdata.ValueType(sfd.Value.ValueType)
	value := valueType.NewValue(sfd.Value.Raw)
	if value == nil {
		qlog.Error("Failed to create value from cache for %s->%s", entityId, fieldType)
		return entityId, fieldType, nil, qdata.WriteTime{}, "", nil
	}

	// Convert back to WriteTime
	writeTime := *new(qdata.WriteTime).FromUnixNanos(sfd.WriteTime)
	writerId := qdata.EntityId(sfd.WriterID)

	return entityId, fieldType, value, writeTime, writerId, nil
}
