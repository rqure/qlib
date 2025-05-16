package qmap

import (
	"context"
	"encoding/gob"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/rqure/qlib/pkg/qdata"
	"github.com/rqure/qlib/pkg/qlog"
	"github.com/rqure/qlib/pkg/qss"
)

// MapConfig holds the map store configuration
type MapConfig struct {
	SnapshotInterval   time.Duration // Interval for creating DB snapshots
	SnapshotRetention  int           // Number of snapshots to retain
	SnapshotDirectory  string        // Directory to store snapshots
	DisablePersistence bool          // Set to true to disable all persistence
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
	WithReadLock(ctx context.Context, fn func(context.Context) error) error
	WithWriteLock(ctx context.Context, fn func(context.Context) error) error

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
	SaveSnapshotToFile(snapshot *MapSnapshot, path string) error
	LoadSnapshotFromFile(path string) (*MapSnapshot, error)
	StartBackgroundTasks(ctx context.Context)
	StopBackgroundTasks()

	// Configuration
	GetConfig() MapConfig
}

// MapSnapshot represents the entire state of the storage
type MapSnapshot struct {
	Schemas     map[string]*qdata.EntitySchema     // Maps entity type string to schema
	Entities    map[string][]qdata.EntityId        // Maps entity type string to slice of entity IDs
	EntityTypes []qdata.EntityType                 // Ordered list of entity types for pagination
	Fields      map[string]map[string]*qdata.Field // Maps entity ID to map of field type to field
}

// mapCore implements the MapCore interface
type mapCore struct {
	schemas         map[string]*qdata.EntitySchema     // Maps entity type string to schema
	entities        map[string][]qdata.EntityId        // Maps entity type string to slice of entity IDs
	entityTypes     []qdata.EntityType                 // Ordered list of entity types for pagination
	fields          map[string]map[string]*qdata.Field // Maps entity ID to map of field type to field
	mutex           sync.RWMutex                       // Global lock for all maps
	config          MapConfig
	isConnected     bool
	connected       qss.Signal[qdata.ConnectedArgs]
	disconnected    qss.Signal[qdata.DisconnectedArgs]
	cancelFunctions []context.CancelFunc
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
	if config.SnapshotDirectory == "" {
		// Default to current directory
		config.SnapshotDirectory = "qmap_snapshots"
	}

	return &mapCore{
		schemas:         make(map[string]*qdata.EntitySchema),
		entities:        make(map[string][]qdata.EntityId),
		entityTypes:     make([]qdata.EntityType, 0),
		fields:          make(map[string]map[string]*qdata.Field),
		config:          config,
		connected:       qss.New[qdata.ConnectedArgs](),
		disconnected:    qss.New[qdata.DisconnectedArgs](),
		cancelFunctions: make([]context.CancelFunc, 0),
	}
}

// Connect establishes a "connection" to the map store
func (c *mapCore) Connect(ctx context.Context) {
	if c.isConnected {
		return
	}

	// Try to load most recent snapshot if we have a directory configured
	if !c.config.DisablePersistence && c.config.SnapshotDirectory != "" {
		latestSnapshot := c.findLatestSnapshot()
		if latestSnapshot != "" {
			qlog.Info("Found map snapshot: %s", latestSnapshot)
			snapshot, err := c.LoadSnapshotFromFile(latestSnapshot)
			if err != nil {
				qlog.Error("Failed to load snapshot file: %v", err)
			} else {
				err = c.RestoreMapSnapshot(snapshot)
				if err != nil {
					qlog.Error("Failed to restore from snapshot: %v", err)
				} else {
					qlog.Info("Successfully restored data from snapshot: %s", latestSnapshot)
				}
			}
		}
	}

	// Set connected status and emit signal
	c.isConnected = true
	c.connected.Emit(qdata.ConnectedArgs{Ctx: ctx})

	// Start background tasks (like snapshot creation)
	c.StartBackgroundTasks(ctx)
}

// Disconnect closes the "connection" to the map store
func (c *mapCore) Disconnect(ctx context.Context) {
	if !c.isConnected {
		return
	}

	// Stop any background tasks
	c.StopBackgroundTasks()

	// Take a final snapshot before disconnecting
	if !c.config.DisablePersistence && c.config.SnapshotDirectory != "" {
		qlog.Info("Taking final snapshot before disconnecting...")

		if err := os.MkdirAll(c.config.SnapshotDirectory, 0755); err != nil {
			qlog.Error("Failed to create snapshot directory: %v", err)
		} else {
			c.mutex.RLock()
			timestamp := time.Now().Format("20060102_150405")
			snapshotFile := filepath.Join(c.config.SnapshotDirectory, fmt.Sprintf("qmap_snapshot_%s.gob", timestamp))
			snapshot, err := c.CreateMapSnapshot()
			c.mutex.RUnlock()

			if err != nil {
				qlog.Error("Failed to create map snapshot: %v", err)
			} else {
				err = c.SaveSnapshotToFile(snapshot, snapshotFile)
				if err != nil {
					qlog.Error("Failed to save snapshot to file: %v", err)
				} else {
					qlog.Info("Final snapshot created successfully: %s", snapshotFile)
				}
			}
		}
	}

	c.isConnected = false
	c.disconnected.Emit(qdata.DisconnectedArgs{Ctx: ctx})
}

// StartBackgroundTasks starts periodic tasks for snapshot creation
func (c *mapCore) StartBackgroundTasks(ctx context.Context) {
	c.StopBackgroundTasks()

	// Skip if persistence is disabled
	if c.config.DisablePersistence {
		return
	}

	// Create snapshot directory if needed
	if c.config.SnapshotDirectory != "" {
		if err := os.MkdirAll(c.config.SnapshotDirectory, 0755); err != nil {
			qlog.Error("Failed to create snapshot directory: %v", err)
		}
	}

	// Start snapshot task
	if c.config.SnapshotDirectory != "" && c.config.SnapshotInterval > 0 {
		snapshotCtx, snapshotCancel := context.WithCancel(ctx)
		c.cancelFunctions = append(c.cancelFunctions, snapshotCancel)
		go c.runSnapshotTask(snapshotCtx)
	}
}

// StopBackgroundTasks stops all background tasks
func (c *mapCore) StopBackgroundTasks() {
	for _, cancel := range c.cancelFunctions {
		cancel()
	}
	c.cancelFunctions = make([]context.CancelFunc, 0)
}

// runSnapshotTask creates periodic snapshots of the data
func (c *mapCore) runSnapshotTask(ctx context.Context) {
	ticker := time.NewTicker(c.config.SnapshotInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			c.mutex.RLock()
			timestamp := time.Now().Format("20060102_150405")
			snapshotFile := filepath.Join(c.config.SnapshotDirectory, fmt.Sprintf("qmap_snapshot_%s.gob", timestamp))

			snapshot, err := c.CreateMapSnapshot()
			c.mutex.RUnlock()

			if err != nil {
				qlog.Error("Failed to create map snapshot: %v", err)
				continue
			}

			err = c.SaveSnapshotToFile(snapshot, snapshotFile)
			if err != nil {
				qlog.Error("Failed to save snapshot to file: %v", err)
				continue
			}

			qlog.Info("Map snapshot created: %s", snapshotFile)

			// Cleanup old snapshots
			c.cleanupOldSnapshots()
		}
	}
}

// cleanupOldSnapshots removes old snapshots beyond retention limit
func (c *mapCore) cleanupOldSnapshots() {
	if c.config.SnapshotRetention <= 0 || c.config.SnapshotDirectory == "" {
		return
	}

	// List snapshot files
	files, err := filepath.Glob(filepath.Join(c.config.SnapshotDirectory, "qmap_snapshot_*.gob"))
	if err != nil {
		qlog.Error("Failed to list snapshot files: %v", err)
		return
	}

	// Sort by name (which includes timestamp)
	sort.Strings(files)

	// If we have more snapshots than retention limit, delete oldest
	if len(files) > c.config.SnapshotRetention {
		for i := 0; i < len(files)-c.config.SnapshotRetention; i++ {
			if err := os.Remove(files[i]); err != nil {
				qlog.Error("Failed to remove old snapshot %s: %v", files[i], err)
			} else {
				qlog.Info("Removed old snapshot: %s", files[i])
			}
		}
	}
}

// findLatestSnapshot returns the path to the latest snapshot file
func (c *mapCore) findLatestSnapshot() string {
	if c.config.SnapshotDirectory == "" {
		return ""
	}

	files, err := filepath.Glob(filepath.Join(c.config.SnapshotDirectory, "qmap_snapshot_*.gob"))
	if err != nil || len(files) == 0 {
		return ""
	}

	// Sort files by name (which includes timestamp)
	sort.Strings(files)

	// Return the latest snapshot (last in sorted list)
	return files[len(files)-1]
}

// SaveSnapshotToFile saves a snapshot to a file using gob encoding
func (c *mapCore) SaveSnapshotToFile(snapshot *MapSnapshot, path string) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()

	encoder := gob.NewEncoder(file)
	return encoder.Encode(snapshot)
}

// LoadSnapshotFromFile loads a snapshot from a file using gob decoding
func (c *mapCore) LoadSnapshotFromFile(path string) (*MapSnapshot, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var snapshot MapSnapshot
	decoder := gob.NewDecoder(file)
	err = decoder.Decode(&snapshot)
	if err != nil {
		return nil, err
	}

	return &snapshot, nil
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

// lockKey is a key for storing lock information in the context
type lockKey struct{}

// lockInfo contains information about an active lock
type lockInfo struct {
	hasReadLock  bool
	hasWriteLock bool
}

// WithReadLock executes a function with a read lock
func (c *mapCore) WithReadLock(ctx context.Context, fn func(context.Context) error) error {
	// Check if we already have a lock from the context
	info, ok := ctx.Value(lockKey{}).(lockInfo)

	// If we already have a read or write lock, just execute the function
	if ok && (info.hasReadLock || info.hasWriteLock) {
		return fn(ctx)
	}

	// Otherwise, acquire a read lock
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	// Create a new context with lock info
	newInfo := lockInfo{hasReadLock: true, hasWriteLock: info.hasWriteLock}
	newCtx := context.WithValue(ctx, lockKey{}, newInfo)

	// Execute the function with the new context
	return fn(newCtx)
}

// WithWriteLock executes a function with a write lock
func (c *mapCore) WithWriteLock(ctx context.Context, fn func(context.Context) error) error {
	// Check if we already have a lock from the context
	info, ok := ctx.Value(lockKey{}).(lockInfo)

	// If we already have a write lock, just execute the function
	if ok && info.hasWriteLock {
		return fn(ctx)
	}

	// Otherwise, acquire a write lock
	c.mutex.Lock()
	defer c.mutex.Unlock()

	// Create a new context with lock info
	newInfo := lockInfo{hasReadLock: true, hasWriteLock: true} // Write lock implies read lock
	newCtx := context.WithValue(ctx, lockKey{}, newInfo)

	// Execute the function with the new context
	return fn(newCtx)
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
	entityIdStr := entityId.AsString()
	// Simply check if the entity has any fields map entry
	_, exists := c.fields[entityIdStr]
	return exists
}

// CreateEntity creates an entity
func (c *mapCore) CreateEntity(entityId qdata.EntityId) error {
	entityType := entityId.GetEntityType()
	entityTypeStr := entityType.AsString()
	entityIdStr := entityId.AsString()

	// Initialize entity type collections if they don't exist
	if _, exists := c.entities[entityTypeStr]; !exists {
		c.entities[entityTypeStr] = make([]qdata.EntityId, 0)
		c.entityTypes = append(c.entityTypes, entityType)
	}

	// Check if entity already exists to avoid duplicates
	if _, exists := c.fields[entityIdStr]; !exists {
		// Add the entity to the slice
		c.entities[entityTypeStr] = append(c.entities[entityTypeStr], entityId)

		// Initialize the entity's fields map
		c.fields[entityIdStr] = make(map[string]*qdata.Field)
	}

	return nil
}

// DeleteEntity removes an entity and all its fields
func (c *mapCore) DeleteEntity(entityId qdata.EntityId) error {
	entityType := entityId.GetEntityType()
	entityTypeStr := entityType.AsString()
	entityIdStr := entityId.AsString()

	// Remove from entities slice
	if entitySlice, exists := c.entities[entityTypeStr]; exists {
		// Remove from slice
		newEntitySlice := make([]qdata.EntityId, 0, len(entitySlice)-1)
		for _, id := range entitySlice {
			if id.AsString() != entityIdStr {
				newEntitySlice = append(newEntitySlice, id)
			}
		}
		c.entities[entityTypeStr] = newEntitySlice
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
	entityTypeStr := entityType.AsString()

	// Check if this is a new entity type
	if _, exists := c.schemas[entityTypeStr]; !exists {
		c.entityTypes = append(c.entityTypes, entityType)
	}

	c.schemas[entityTypeStr] = schema.Clone() // Store a clone to prevent external modification
	return nil
}

// DeleteSchema removes a schema
func (c *mapCore) DeleteSchema(entityType qdata.EntityType) error {
	entityTypeStr := entityType.AsString()
	delete(c.schemas, entityTypeStr)

	// Also remove from the entity types list
	for i, et := range c.entityTypes {
		if et.AsString() == entityTypeStr {
			c.entityTypes = append(c.entityTypes[:i], c.entityTypes[i+1:]...)
			break
		}
	}

	return nil
}

// ListEntities lists all entities of a specific type
func (c *mapCore) ListEntities(entityType qdata.EntityType) ([]qdata.EntityId, error) {
	entityTypeStr := entityType.AsString()

	// If we have entities of this type, return a copy of the slice
	if entitySlice, exists := c.entities[entityTypeStr]; exists {
		result := make([]qdata.EntityId, len(entitySlice))
		copy(result, entitySlice)
		return result, nil
	}

	// Return empty slice if no entities of this type
	return make([]qdata.EntityId, 0), nil
}

// ListEntityTypes lists all entity types
func (c *mapCore) ListEntityTypes() ([]qdata.EntityType, error) {
	// Return a copy of the entity types slice
	result := make([]qdata.EntityType, len(c.entityTypes))
	copy(result, c.entityTypes)
	return result, nil
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
	startTime := time.Now()
	defer func() {
		qlog.Trace("MapCore: CreateMapSnapshot took %v", time.Since(startTime))
	}()

	// Create deep copies of all maps and slices
	schemasCopy := make(map[string]*qdata.EntitySchema, len(c.schemas))
	for k, v := range c.schemas {
		schemasCopy[k] = v.Clone()
	}

	entityTypesCopy := make([]qdata.EntityType, len(c.entityTypes))
	copy(entityTypesCopy, c.entityTypes)

	entitiesCopy := make(map[string][]qdata.EntityId, len(c.entities))
	for entityTypeStr, entitySlice := range c.entities {
		sliceCopy := make([]qdata.EntityId, len(entitySlice))
		copy(sliceCopy, entitySlice)
		entitiesCopy[entityTypeStr] = sliceCopy
	}

	fieldsCopy := make(map[string]map[string]*qdata.Field, len(c.fields))
	for entityId, entityFields := range c.fields {
		fieldsCopy[entityId] = make(map[string]*qdata.Field, len(entityFields))
		for fieldType, field := range entityFields {
			fieldsCopy[entityId][fieldType] = field.Clone()
		}
	}

	return &MapSnapshot{
		Schemas:     schemasCopy,
		Entities:    entitiesCopy,
		EntityTypes: entityTypesCopy,
		Fields:      fieldsCopy,
	}, nil
}

// RestoreMapSnapshot restores data from a snapshot
func (c *mapCore) RestoreMapSnapshot(snapshot *MapSnapshot) error {
	if snapshot == nil {
		return fmt.Errorf("cannot restore from nil snapshot")
	}

	// Replace all maps and slices with the snapshot data (deep copies)
	c.schemas = make(map[string]*qdata.EntitySchema, len(snapshot.Schemas))
	for k, v := range snapshot.Schemas {
		c.schemas[k] = v.Clone()
	}

	c.entityTypes = make([]qdata.EntityType, len(snapshot.EntityTypes))
	copy(c.entityTypes, snapshot.EntityTypes)

	c.entities = make(map[string][]qdata.EntityId, len(snapshot.Entities))
	for entityTypeStr, entitySlice := range snapshot.Entities {
		sliceCopy := make([]qdata.EntityId, len(entitySlice))
		copy(sliceCopy, entitySlice)
		c.entities[entityTypeStr] = sliceCopy
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
