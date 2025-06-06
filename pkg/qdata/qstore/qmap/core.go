package qmap

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"sync"
	"time"

	"github.com/rqure/qlib/pkg/qdata"
	"github.com/rqure/qlib/pkg/qlog"
	"github.com/rqure/qlib/pkg/qss"
)

type SnapshotManager interface {
	CreateSnapshot(ctx context.Context) (*qdata.Snapshot, error)
	RestoreSnapshot(ctx context.Context, snapshot *qdata.Snapshot) error
}

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

	SetSnapshot(snapshot *qdata.Snapshot) error
	SaveSnapshotToFile(snapshot *qdata.Snapshot, path string) error
	LoadSnapshotFromFile(path string) (*qdata.Snapshot, error)

	StartBackgroundTasks(ctx context.Context)
	StopBackgroundTasks()

	SetSnapshotManager(manager SnapshotManager)

	// Configuration
	GetConfig() MapConfig
}

// mapCore implements the MapCore interface
type mapCore struct {
	schemas         map[qdata.EntityType]*qdata.EntitySchema            // Maps entity type string to schema
	entities        map[qdata.EntityType][]qdata.EntityId               // Maps entity type string to slice of entity IDs
	entityTypes     []qdata.EntityType                                  // Ordered list of entity types for pagination
	fields          map[qdata.EntityId]map[qdata.FieldType]*qdata.Field // Maps entity ID to map of field type to field
	mutex           sync.RWMutex                                        // Global lock for all maps
	config          MapConfig
	isConnected     bool
	connected       qss.Signal[qdata.ConnectedArgs]
	disconnected    qss.Signal[qdata.DisconnectedArgs]
	cancelFunctions []context.CancelFunc

	snapshotManager SnapshotManager
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
		schemas:         make(map[qdata.EntityType]*qdata.EntitySchema),
		entities:        make(map[qdata.EntityType][]qdata.EntityId),
		entityTypes:     make([]qdata.EntityType, 0),
		fields:          make(map[qdata.EntityId]map[qdata.FieldType]*qdata.Field),
		config:          config,
		connected:       qss.New[qdata.ConnectedArgs](),
		disconnected:    qss.New[qdata.DisconnectedArgs](),
		cancelFunctions: make([]context.CancelFunc, 0),
	}
}

func (c *mapCore) SetSnapshotManager(manager SnapshotManager) {
	c.snapshotManager = manager
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
				err = c.snapshotManager.RestoreSnapshot(ctx, snapshot)
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
			timestamp := time.Now().Format("20060102_150405")
			snapshotFile := filepath.Join(c.config.SnapshotDirectory, fmt.Sprintf("qmap_snapshot_%s.gob", timestamp))
			snapshot, err := c.snapshotManager.CreateSnapshot(ctx)

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
			timestamp := time.Now().Format("20060102_150405")
			snapshotFile := filepath.Join(c.config.SnapshotDirectory, fmt.Sprintf("qmap_snapshot_%s.gob", timestamp))

			snapshot, err := c.snapshotManager.CreateSnapshot(ctx)

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
func (c *mapCore) SaveSnapshotToFile(snapshot *qdata.Snapshot, path string) error {
	b, err := snapshot.AsBytes()
	if err != nil {
		return err
	}

	return os.WriteFile(path, b, 0644) // Create or truncate the file
}

// LoadSnapshotFromFile loads a snapshot from a file using gob decoding
func (c *mapCore) LoadSnapshotFromFile(path string) (*qdata.Snapshot, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	snapshot, err := new(qdata.Snapshot).FromBytes(b)
	if err != nil {
		return nil, err
	}

	return snapshot, nil
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
	entityFields, exists := c.fields[entityId]
	if !exists {
		return nil, false
	}

	field, exists := entityFields[fieldType]
	return field, exists
}

// SetField sets a field value
func (c *mapCore) SetField(entityId qdata.EntityId, fieldType qdata.FieldType, field *qdata.Field) error {
	// Ensure the entity exists in the fields map
	if _, exists := c.fields[entityId]; !exists {
		c.fields[entityId] = make(map[qdata.FieldType]*qdata.Field)
	}

	// Store the field
	c.fields[entityId][fieldType] = field.Clone() // Store a clone to prevent external modification

	return nil
}

// DeleteField removes a field
func (c *mapCore) DeleteField(entityId qdata.EntityId, fieldType qdata.FieldType) error {
	if entityFields, exists := c.fields[entityId]; exists {
		delete(entityFields, fieldType)
	}

	return nil
}

// EntityExists checks if an entity exists
func (c *mapCore) EntityExists(entityId qdata.EntityId) bool {
	_, exists := c.fields[entityId]
	return exists
}

// CreateEntity creates an entity
func (c *mapCore) CreateEntity(entityId qdata.EntityId) error {
	entityType := entityId.GetEntityType()
	if _, exists := c.fields[entityId]; exists {
		return fmt.Errorf("entity %s already exists", entityId)
	}

	// Add the entity to the slice
	c.entities[entityType] = append(c.entities[entityType], entityId)

	// Initialize the entity's fields map
	c.fields[entityId] = make(map[qdata.FieldType]*qdata.Field)

	return nil
}

// DeleteEntity removes an entity and all its fields
func (c *mapCore) DeleteEntity(entityId qdata.EntityId) error {
	entityType := entityId.GetEntityType()

	// Remove from entities slice
	if _, exists := c.entities[entityType]; exists {
		c.entities[entityType] = slices.DeleteFunc(c.entities[entityType], func(id qdata.EntityId) bool {
			return id == entityId
		})
	}

	// Delete all fields for this entity
	delete(c.fields, entityId)

	return nil
}

// GetSchema retrieves a schema
func (c *mapCore) GetSchema(entityType qdata.EntityType) (*qdata.EntitySchema, bool) {
	schema, exists := c.schemas[entityType]
	if exists {
		return schema.Clone(), exists // Return a clone to prevent external modification
	}
	return nil, false
}

// SetSchema sets a schema
func (c *mapCore) SetSchema(entityType qdata.EntityType, schema *qdata.EntitySchema) error {
	// Check if this is a new entity type
	if _, exists := c.schemas[entityType]; !exists {
		c.entityTypes = append(c.entityTypes, entityType)
		c.entities[entityType] = make([]qdata.EntityId, 0)
	}

	c.schemas[entityType] = schema.Clone() // Store a clone to prevent external modification
	return nil
}

// DeleteSchema removes a schema
func (c *mapCore) DeleteSchema(entityType qdata.EntityType) error {
	delete(c.schemas, entityType)

	c.entityTypes = slices.DeleteFunc(c.entityTypes, func(t qdata.EntityType) bool {
		return t == entityType
	})

	for _, entityId := range c.entities[entityType] {
		delete(c.fields, entityId)
	}

	delete(c.entities, entityType)

	return nil
}

// ListEntities lists all entities of a specific type
func (c *mapCore) ListEntities(entityType qdata.EntityType) ([]qdata.EntityId, error) {
	// If we have entities of this type, return a copy of the slice
	if entitySlice, exists := c.entities[entityType]; exists {
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
	entityFields, exists := c.fields[entityId]

	if !exists {
		return make([]qdata.FieldType, 0), nil
	}

	fields := make([]qdata.FieldType, 0, len(entityFields))
	for fieldKey := range entityFields {
		fields = append(fields, qdata.FieldType(fieldKey))
	}

	return fields, nil
}

// GetConfig returns the core configuration
func (c *mapCore) GetConfig() MapConfig {
	return c.config
}

func (c *mapCore) SetSnapshot(snapshot *qdata.Snapshot) error {
	qlog.Trace("Setting snapshot with %d entities and %d schemas", len(snapshot.Entities), len(snapshot.Schemas))

	startTime := time.Now()
	defer func() {
		qlog.Trace("SetSnapshot took %s", time.Since(startTime))
	}()

	c.schemas = make(map[qdata.EntityType]*qdata.EntitySchema)
	c.entities = make(map[qdata.EntityType][]qdata.EntityId)
	c.entityTypes = make([]qdata.EntityType, 0)
	c.fields = make(map[qdata.EntityId]map[qdata.FieldType]*qdata.Field)

	for _, schema := range snapshot.Schemas {
		qlog.Trace("Setting schema for entity type %s", schema.EntityType)
		if err := c.SetSchema(schema.EntityType, schema); err != nil {
			return err
		}
	}

	for _, entity := range snapshot.Entities {
		qlog.Trace("Setting entity %s", entity.EntityId)
		if err := c.CreateEntity(entity.EntityId); err != nil {
			return err
		}

		for _, field := range entity.Fields {
			qlog.Trace("Setting field %s for entity %s", field.FieldType, entity.EntityId)
			if err := c.SetField(entity.EntityId, field.FieldType, field); err != nil {
				return err
			}
		}
	}

	return nil
}
