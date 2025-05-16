package qmap

import (
	"context"
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
	GetField(entityId qdata.EntityId, fieldType qdata.FieldType) (*qdata.Field, bool)
	SetField(entityId qdata.EntityId, fieldType qdata.FieldType, field *qdata.Field) error
	DeleteField(entityId qdata.EntityId, fieldType qdata.FieldType) error

	// Entity management
	EntityExists(entityId qdata.EntityId) bool
	CreateEntity(entityId qdata.EntityId) error
	DeleteEntity(entityId qdata.EntityId) error

	// Schema management
	GetSchema(entityType qdata.EntityType) (*qdata.EntitySchema, bool)
	SetSchema(entityType qdata.EntityType, schema *qdata.EntitySchema) error
	DeleteSchema(entityType qdata.EntityType) error

	// Iteration helpers
	ListEntities(entityType qdata.EntityType) ([]qdata.EntityId, error)
	ListEntityTypes() ([]qdata.EntityType, error)
	ListEntityFields(entityId qdata.EntityId) ([]qdata.FieldType, error)

	// Snapshot
	CreateMapSnapshot() (*MapSnapshot, error)
	RestoreMapSnapshot(snapshot *MapSnapshot) error

	// Configuration
	GetConfig() MapConfig
}

// MapSnapshot represents the entire state of the storage
type MapSnapshot struct {
	Schemas  map[string]*qdata.EntitySchema     // Maps entity type string to schema
	Entities map[string]map[string]bool         // Maps entity type to map of entity IDs
	Fields   map[string]map[string]*qdata.Field // Maps entity ID to map of field type to field
}

// mapCore implements the MapCore interface
type mapCore struct {
	schemas         map[string]*qdata.EntitySchema     // Maps entity type string to schema
	entities        map[string]map[string]bool         // Maps entity type to map of entity IDs
	fields          map[string]map[string]*qdata.Field // Maps entity ID to map of field type to field
	mutex           sync.RWMutex                       // Global lock for all maps
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
		schemas:         make(map[string]*qdata.EntitySchema),
		entities:        make(map[string]map[string]bool),
		fields:          make(map[string]map[string]*qdata.Field),
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
func (c *mapCore) GetField(entityId qdata.EntityId, fieldType qdata.FieldType) (*qdata.Field, bool) {
	entityIdStr := entityId.AsString()
	entityFields, exists := c.fields[entityIdStr]
	if !exists {
		return nil, false
	}

	field, exists := entityFields[fieldType.AsString()]
	return field, exists
}

// SetField sets a field value
func (c *mapCore) SetField(entityId qdata.EntityId, fieldType qdata.FieldType, field *qdata.Field) error {
	entityIdStr := entityId.AsString()

	// Ensure the entity exists in the fields map
	if _, exists := c.fields[entityIdStr]; !exists {
		c.fields[entityIdStr] = make(map[string]*qdata.Field)
	}

	// Store the field
	c.fields[entityIdStr][fieldType.AsString()] = field.Clone() // Store a clone to prevent external modification

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
	entityType := entityId.GetEntityType()
	entityTypeStr := entityType.AsString()
	entityIdStr := entityId.AsString()

	// Check if this entity type exists in our map
	if entityMap, exists := c.entities[entityTypeStr]; exists {
		// Check if this entity ID exists for this type
		return entityMap[entityIdStr]
	}

	return false
}

// CreateEntity creates an entity
func (c *mapCore) CreateEntity(entityId qdata.EntityId) error {
	entityType := entityId.GetEntityType()
	entityTypeStr := entityType.AsString()
	entityIdStr := entityId.AsString()

	// Initialize the entity type map if it doesn't exist
	if _, exists := c.entities[entityTypeStr]; !exists {
		c.entities[entityTypeStr] = make(map[string]bool)
	}

	// Add the entity to the entity type map
	c.entities[entityTypeStr][entityIdStr] = true

	// Initialize the entity's fields map if it doesn't exist yet
	if _, exists := c.fields[entityIdStr]; !exists {
		c.fields[entityIdStr] = make(map[string]*qdata.Field)
	}

	return nil
}

// DeleteEntity removes an entity and all its fields
func (c *mapCore) DeleteEntity(entityId qdata.EntityId) error {
	entityType := entityId.GetEntityType()
	entityTypeStr := entityType.AsString()
	entityIdStr := entityId.AsString()

	// Remove from entities map
	if entityMap, exists := c.entities[entityTypeStr]; exists {
		delete(entityMap, entityIdStr)
	}

	// Delete all fields for this entity
	delete(c.fields, entityIdStr)

	return nil
}

// GetSchema retrieves a schema
func (c *mapCore) GetSchema(entityType qdata.EntityType) (*qdata.EntitySchema, bool) {
	schema, exists := c.schemas[entityType.AsString()]
	if exists {
		return schema.Clone(), exists // Return a clone to prevent external modification
	}
	return nil, false
}

// SetSchema sets a schema
func (c *mapCore) SetSchema(entityType qdata.EntityType, schema *qdata.EntitySchema) error {
	c.schemas[entityType.AsString()] = schema.Clone() // Store a clone to prevent external modification
	return nil
}

// DeleteSchema removes a schema
func (c *mapCore) DeleteSchema(entityType qdata.EntityType) error {
	delete(c.schemas, entityType.AsString())
	return nil
}

// ListEntities lists all entities of a specific type
func (c *mapCore) ListEntities(entityType qdata.EntityType) ([]qdata.EntityId, error) {
	entityTypeStr := entityType.AsString()
	entities := make([]qdata.EntityId, 0)

	// Get the entity map for this type
	if entityMap, exists := c.entities[entityTypeStr]; exists {
		for entityIdStr := range entityMap {
			entities = append(entities, qdata.EntityId(entityIdStr))
		}
	}

	return entities, nil
}

// ListEntityTypes lists all entity types
func (c *mapCore) ListEntityTypes() ([]qdata.EntityType, error) {
	entityTypes := make([]qdata.EntityType, 0, len(c.schemas))

	for entityTypeStr := range c.schemas {
		entityTypes = append(entityTypes, qdata.EntityType(entityTypeStr))
	}

	return entityTypes, nil
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
	schemasCopy := make(map[string]*qdata.EntitySchema, len(c.schemas))
	for k, v := range c.schemas {
		schemasCopy[k] = v.Clone()
	}

	entitiesCopy := make(map[string]map[string]bool, len(c.entities))
	for entityType, entityMap := range c.entities {
		entitiesCopy[entityType] = make(map[string]bool, len(entityMap))
		for entityId, exists := range entityMap {
			entitiesCopy[entityType][entityId] = exists
		}
	}

	fieldsCopy := make(map[string]map[string]*qdata.Field, len(c.fields))
	for entityId, entityFields := range c.fields {
		fieldsCopy[entityId] = make(map[string]*qdata.Field, len(entityFields))
		for fieldType, field := range entityFields {
			fieldsCopy[entityId][fieldType] = field.Clone()
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

	// Replace all maps with the snapshot data (deep copies)
	c.schemas = make(map[string]*qdata.EntitySchema, len(snapshot.Schemas))
	for k, v := range snapshot.Schemas {
		c.schemas[k] = v.Clone()
	}

	c.entities = make(map[string]map[string]bool, len(snapshot.Entities))
	for entityType, entityMap := range snapshot.Entities {
		c.entities[entityType] = make(map[string]bool, len(entityMap))
		for entityId, exists := range entityMap {
			c.entities[entityType][entityId] = exists
		}
	}

	c.fields = make(map[string]map[string]*qdata.Field, len(snapshot.Fields))
	for entityId, entityFields := range snapshot.Fields {
		c.fields[entityId] = make(map[string]*qdata.Field, len(entityFields))
		for fieldType, field := range entityFields {
			c.fields[entityId][fieldType] = field.Clone()
		}
	}

	return nil
}

// GetConfig returns the core configuration
func (c *mapCore) GetConfig() MapConfig {
	return c.config
}
