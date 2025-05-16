package qmap

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/rqure/qlib/pkg/qdata"
	"github.com/rqure/qlib/pkg/qss"
)

// MapConfig holds the map store configuration
type MapConfig struct {
	SnapshotInterval  time.Duration // Interval for creating DB snapshots
	SnapshotRetention int           // Number of snapshots to retain
}

// MapCore provides the core map-based storage functionality
type MapCore interface {
	// Connection management
	Connect(ctx context.Context)
	Disconnect(ctx context.Context)
	IsConnected() bool
	Connected() qss.Signal[qdata.ConnectedArgs]
	Disconnected() qss.Signal[qdata.DisconnectedArgs]

	// Transaction handling
	WithReadLock(ctx context.Context, fn func() error) error
	WithWriteLock(ctx context.Context, fn func() error) error

	// Data access
	GetField(entityId qdata.EntityId, fieldType qdata.FieldType) ([]byte, bool)
	SetField(entityId qdata.EntityId, fieldType qdata.FieldType, data []byte) error
	DeleteField(entityId qdata.EntityId, fieldType qdata.FieldType) error

	// Entity management
	EntityExists(entityId qdata.EntityId) bool
	CreateEntity(entityId qdata.EntityId) error
	DeleteEntity(entityId qdata.EntityId) error

	// Schema management
	GetSchema(entityType qdata.EntityType) ([]byte, bool)
	SetSchema(entityType qdata.EntityType, data []byte) error
	DeleteSchema(entityType qdata.EntityType) error

	// Iteration helpers
	ListEntities(prefix string) ([]qdata.EntityId, error)
	ListSchemas() ([]qdata.EntityType, error)
	ListEntityFields(entityId qdata.EntityId) ([]qdata.FieldType, error)

	// Snapshot
	CreateMapSnapshot() (*MapSnapshot, error)
	RestoreMapSnapshot(snapshot *MapSnapshot) error

	// Configuration
	GetConfig() MapConfig
}

// MapSnapshot represents the entire state of the storage
type MapSnapshot struct {
	Schemas  map[string][]byte            // Maps entity type string to schema data
	Entities map[string]bool              // Maps entity ID to existence flag
	Fields   map[string]map[string][]byte // Maps entity ID to a map of field type to field data
}

// mapCore implements the MapCore interface
type mapCore struct {
	schemas         map[string][]byte            // Maps entity type string to schema data
	entities        map[string]bool              // Maps entity ID to existence flag
	fields          map[string]map[string][]byte // Maps entity ID to a map of field type to field data
	mutex           sync.RWMutex                 // Global lock for all maps
	config          MapConfig
	isConnected     bool
	connected       qss.Signal[qdata.ConnectedArgs]
	disconnected    qss.Signal[qdata.DisconnectedArgs]
	cancelFunctions []func()
}

// NewCore creates a new map storage core
func NewCore(config MapConfig) MapCore {
	// Apply default configuration if needed
	if config.SnapshotInterval == 0 {
		config.SnapshotInterval = 1 * time.Hour
	}
	if config.SnapshotRetention == 0 {
		config.SnapshotRetention = 3
	}

	return &mapCore{
		schemas:         make(map[string][]byte),
		entities:        make(map[string]bool),
		fields:          make(map[string]map[string][]byte),
		config:          config,
		connected:       qss.New[qdata.ConnectedArgs](),
		disconnected:    qss.New[qdata.DisconnectedArgs](),
		cancelFunctions: make([]func(), 0),
	}
}

// Connect establishes a "connection" to the map store
func (c *mapCore) Connect(ctx context.Context) {
	if c.isConnected {
		return
	}

	// No actual connection needed, just set flag and emit signal
	c.isConnected = true
	c.connected.Emit(qdata.ConnectedArgs{Ctx: ctx})
}

// Disconnect closes the "connection" to the map store
func (c *mapCore) Disconnect(ctx context.Context) {
	if !c.isConnected {
		return
	}

	// Stop any background tasks
	for _, cancel := range c.cancelFunctions {
		cancel()
	}
	c.cancelFunctions = make([]func(), 0)

	c.isConnected = false
	c.disconnected.Emit(qdata.DisconnectedArgs{Ctx: ctx})
}

// IsConnected returns whether the store is connected
func (c *mapCore) IsConnected() bool {
	return c.isConnected
}

// Connected returns the signal emitted on connection
func (c *mapCore) Connected() qss.Signal[qdata.ConnectedArgs] {
	return c.connected
}

// Disconnected returns the signal emitted on disconnection
func (c *mapCore) Disconnected() qss.Signal[qdata.DisconnectedArgs] {
	return c.disconnected
}

// WithReadLock executes a function with a read lock
func (c *mapCore) WithReadLock(ctx context.Context, fn func() error) error {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	return fn()
}

// WithWriteLock executes a function with a write lock
func (c *mapCore) WithWriteLock(ctx context.Context, fn func() error) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	return fn()
}

// GetField retrieves a field value
func (c *mapCore) GetField(entityId qdata.EntityId, fieldType qdata.FieldType) ([]byte, bool) {
	entityFields, exists := c.fields[entityId.AsString()]
	if !exists {
		return nil, false
	}

	value, exists := entityFields[fieldType.AsString()]
	return value, exists
}

// SetField sets a field value
func (c *mapCore) SetField(entityId qdata.EntityId, fieldType qdata.FieldType, data []byte) error {
	entityIdStr := entityId.AsString()

	// Ensure the entity exists in the fields map
	if _, exists := c.fields[entityIdStr]; !exists {
		c.fields[entityIdStr] = make(map[string][]byte)
	}

	// Store the field data
	c.fields[entityIdStr][fieldType.AsString()] = data

	return nil
}

// DeleteField removes a field
func (c *mapCore) DeleteField(entityId qdata.EntityId, fieldType qdata.FieldType) error {
	entityIdStr := entityId.AsString()

	// Check if the entity exists in the fields map
	if entityFields, exists := c.fields[entityIdStr]; exists {
		// Delete the field
		delete(entityFields, fieldType.AsString())
	}

	return nil
}

// EntityExists checks if an entity exists
func (c *mapCore) EntityExists(entityId qdata.EntityId) bool {
	_, exists := c.entities[entityId.AsString()]
	return exists
}

// CreateEntity creates an entity
func (c *mapCore) CreateEntity(entityId qdata.EntityId) error {
	entityIdStr := entityId.AsString()
	c.entities[entityIdStr] = true

	// Initialize the entity's fields map if it doesn't exist yet
	if _, exists := c.fields[entityIdStr]; !exists {
		c.fields[entityIdStr] = make(map[string][]byte)
	}

	return nil
}

// DeleteEntity removes an entity and all its fields
func (c *mapCore) DeleteEntity(entityId qdata.EntityId) error {
	entityIdStr := entityId.AsString()

	// Delete from entities map
	delete(c.entities, entityIdStr)

	// Delete all fields for this entity
	delete(c.fields, entityIdStr)

	return nil
}

// GetSchema retrieves a schema
func (c *mapCore) GetSchema(entityType qdata.EntityType) ([]byte, bool) {
	value, exists := c.schemas[entityType.AsString()]
	return value, exists
}

// SetSchema sets a schema
func (c *mapCore) SetSchema(entityType qdata.EntityType, data []byte) error {
	c.schemas[entityType.AsString()] = data
	return nil
}

// DeleteSchema removes a schema
func (c *mapCore) DeleteSchema(entityType qdata.EntityType) error {
	delete(c.schemas, entityType.AsString())
	return nil
}

// ListEntities lists all entities with a specific prefix
func (c *mapCore) ListEntities(prefix string) ([]qdata.EntityId, error) {
	entities := make([]qdata.EntityId, 0)

	for entityIdStr := range c.entities {
		// If a prefix is specified, check if the entity ID starts with it
		if prefix == "" || hasPrefix(entityIdStr, prefix) {
			entities = append(entities, qdata.EntityId(entityIdStr))
		}
	}

	return entities, nil
}

// hasPrefix checks if a string has the given prefix, considering the entity ID format (type$id)
func hasPrefix(entityIdStr string, prefix string) bool {
	// In entity IDs of format "type$id", check if the type part matches the prefix
	typePart := ""
	for i, char := range entityIdStr {
		if char == '$' {
			typePart = entityIdStr[:i]
			break
		}
	}
	return typePart == prefix
}

// ListSchemas lists all entity types
func (c *mapCore) ListSchemas() ([]qdata.EntityType, error) {
	schemas := make([]qdata.EntityType, 0, len(c.schemas))

	for schemaKey := range c.schemas {
		schemas = append(schemas, qdata.EntityType(schemaKey))
	}

	return schemas, nil
}

// ListEntityFields lists all fields for an entity
func (c *mapCore) ListEntityFields(entityId qdata.EntityId) ([]qdata.FieldType, error) {
	entityIdStr := entityId.AsString()
	entityFields, exists := c.fields[entityIdStr]

	if !exists {
		return make([]qdata.FieldType, 0), nil
	}

	fields := make([]qdata.FieldType, 0, len(entityFields))
	for fieldKey := range entityFields {
		fields = append(fields, qdata.FieldType(fieldKey))
	}

	return fields, nil
}

// CreateMapSnapshot creates a copy of the current data state
func (c *mapCore) CreateMapSnapshot() (*MapSnapshot, error) {
	// Create deep copies of all maps
	schemasCopy := make(map[string][]byte, len(c.schemas))
	for k, v := range c.schemas {
		dataCopy := make([]byte, len(v))
		copy(dataCopy, v)
		schemasCopy[k] = dataCopy
	}

	entitiesCopy := make(map[string]bool, len(c.entities))
	for k, v := range c.entities {
		entitiesCopy[k] = v
	}

	fieldsCopy := make(map[string]map[string][]byte, len(c.fields))
	for entityId, entityFields := range c.fields {
		fieldsCopy[entityId] = make(map[string][]byte, len(entityFields))
		for fieldType, fieldData := range entityFields {
			dataCopy := make([]byte, len(fieldData))
			copy(dataCopy, fieldData)
			fieldsCopy[entityId][fieldType] = dataCopy
		}
	}

	return &MapSnapshot{
		Schemas:  schemasCopy,
		Entities: entitiesCopy,
		Fields:   fieldsCopy,
	}, nil
}

// RestoreMapSnapshot restores data from a snapshot
func (c *mapCore) RestoreMapSnapshot(snapshot *MapSnapshot) error {
	if snapshot == nil {
		return fmt.Errorf("cannot restore from nil snapshot")
	}

	// Replace all maps with the snapshot data
	c.schemas = make(map[string][]byte, len(snapshot.Schemas))
	for k, v := range snapshot.Schemas {
		dataCopy := make([]byte, len(v))
		copy(dataCopy, v)
		c.schemas[k] = dataCopy
	}

	c.entities = make(map[string]bool, len(snapshot.Entities))
	for k, v := range snapshot.Entities {
		c.entities[k] = v
	}

	c.fields = make(map[string]map[string][]byte, len(snapshot.Fields))
	for entityId, entityFields := range snapshot.Fields {
		c.fields[entityId] = make(map[string][]byte, len(entityFields))
		for fieldType, fieldData := range entityFields {
			dataCopy := make([]byte, len(fieldData))
			copy(dataCopy, fieldData)
			c.fields[entityId][fieldType] = dataCopy
		}
	}

	return nil
}

// GetConfig returns the core configuration
func (c *mapCore) GetConfig() MapConfig {
	return c.config
}

// SerializeSnapshot serializes snapshot to JSON
func SerializeSnapshot(snapshot *MapSnapshot) ([]byte, error) {
	return json.Marshal(snapshot)
}

// DeserializeSnapshot deserializes snapshot from JSON
func DeserializeSnapshot(data []byte) (*MapSnapshot, error) {
	snapshot := &MapSnapshot{}
	err := json.Unmarshal(data, snapshot)
	if err != nil {
		return nil, fmt.Errorf("failed to deserialize snapshot: %w", err)
	}
	return snapshot, nil
}
