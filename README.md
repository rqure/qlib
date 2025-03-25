# qlib

A Go library for building event-driven, worker-based applications with a hierarchical object database.

## Core Packages

### qapp
Base application framework providing:
- Worker lifecycle management
- Signal handling
- Environment configuration
- Application instance identification

### qauth
Authentication and authorization:
- User management
- Session handling
- Permission control
- Identity provider integration
- OAuth/OIDC support

### qdata
Data management and persistence:
- Entity management
- Schema validation
- Field operations
- Real-time notifications
- Snapshot management

### qss
Signal/slot implementation:
- Type-safe callbacks
- Disconnectable signals
- Thread-safe emission
- Zero allocation design

## Overview

qlib provides a framework for building applications with the following key features:

- Single-threaded application model with worker-based architecture
- Hierarchical object database with real-time notifications
- Event-driven communication between workers

## Logging System

qlib uses a structured logging system with separate application and library log levels.

### Log Levels

```go
const (
    UNSPECIFIED Level = iota
    TRACE
    DEBUG
    INFO
    WARN
    ERROR
    PANIC
)
```

### Configuring Log Levels

```go
// Set application log level
qlog.SetLevel(qlog.INFO)

// Set library log level (for qlib internal logs)
qlog.SetLibLevel(qlog.WARN)

// Get current levels
currentLevel := qlog.GetLevel()
currentLibLevel := qlog.GetLibLevel()
```

### Using the Logger

```go
// Basic logging
qlog.Info("Server started on port %d", port)
qlog.Debug("Processing request: %v", request)
qlog.Error("Failed to connect: %v", err)

// With stack traces
// ERROR and PANIC levels automatically include stack traces
qlog.Panic("Critical system failure: %v", err)
```

Log output format:
```
2024-01-20T15:04:05.123Z | INFO | myapp/server.go:42 | MyWorker.Init | Server started on port 8080
```

## Core Concepts

### Applications

Applications in qlib are built around a main thread and a set of workers. Create a new application:

```go
app := qapp.NewApplication("myapp")
```

### Workers

Workers are logical units that handle specific functionality. Each worker:
- Must implement the `Worker` interface
- Is initialized with the application context
- Runs in the main thread via `DoWork()`
- Can communicate with other workers through the database or signals

Example worker:

```go
type MyWorker struct {
    store qdata.Store
}

func (w *MyWorker) Init(ctx context.Context, h qapp.Handle) {
    // Initialize worker
}

func (w *MyWorker) DoWork(ctx context.Context) {
    // Handle work in main thread
}

func (w *MyWorker) Deinit(ctx context.Context) {
    // Cleanup
}
```

Add workers to your application:

```go
app.AddWorker(myWorker)
```

### Database Structure

The database follows a tree-like structure where:
- Each node is an "Entity" with a type and unique ID
- Entities can have fields (properties)
- Entities can have parent-child relationships
- Changes to entity fields trigger notifications

Example structure:
```
Root
├── Service
│   ├── Name: "myapp"
│   ├── Status: "running"
│   └── Config
│       ├── Setting1: "value1"
│       └── Setting2: "value2"
└── AnotherEntity
    └── Field1: "data"
```

### Field Notifications

Workers can subscribe to field changes:

```go
store.Notify(
    ctx,
    qdata.NewConfig().
        SetEntityType("Service").
        SetFieldName("Status"),
    qdata.NewCallback(func(ctx context.Context, n qdata.Notification) {
        // Handle field change
    }),
)
```

## Database (Store) Interface

The Store interface is the core component for data persistence and real-time updates. It provides a rich API for managing entities, fields, and notifications.

### Store Configuration

The store supports multiple backend configurations:

```go
// Create store with Postgres backend
store := qdata.New(
    qdata.CommunicateOverPostgres("postgres://user:pass@localhost:5432/db"),
)

// Create store with NATS backend
store := qdata.New(
    qdata.CommunicateOverNats("nats://localhost:4222"),
)

// Mix multiple backends
store := qdata.New(
    qdata.PersistOverPostgres("postgres://user:pass@localhost:5432/db"),
    qdata.NotifyOverNats("nats://localhost:4222"),
)
```

Each backend configuration function sets up the appropriate components for:
- Entity management
- Field operations
- Schema management
- Notifications
- Snapshots

### Basic Store Operations

```go
type Store interface {
    // Connection management
    Connect(context.Context)
    Disconnect(context.Context)
    IsConnected(context.Context) bool

    // Entity operations
    CreateEntity(ctx context.Context, entityType qdata.EntityType, parentId qdata.EntityId, name string)
    GetEntity(ctx context.Context, entityId qdata.EntityId) Entity
    DeleteEntity(ctx context.Context, entityId qdata.EntityId)
    FindEntities(ctx context.Context, entityType qdata.EntityType) []qdata.EntityId

    // Field operations
    Read(context.Context, ...Request)
    Write(context.Context, ...Request)

    // Schema operations
    GetEntitySchema(ctx context.Context, entityType string) EntitySchema
    SetEntitySchema(context.Context, EntitySchema)

    // Notification system
    Notify(ctx context.Context, config NotificationConfig, callback NotificationCallback) NotificationToken
    Unnotify(ctx context.Context, subscriptionId string)
}
```

### Entity Management

Entities are the primary data containers. Each entity:
- Has a unique ID
- Has a type (defines its schema)
- Can have a parent entity
- Can have child entities
- Contains fields (properties)

```go
// Creating an entity
store.CreateEntity(ctx, "Device", parentId, "LivingRoomLight")

// Reading an entity
entity := store.GetEntity(ctx, entityId)
fmt.Printf("Name: %s, Type: %s\n", entity.GetName(), entity.GetType())

// Finding entities by type
deviceIds := store.FindEntities(ctx, "Device")
```

### Field Operations

Fields store the actual data values. The store supports various field types:
- Int
- Float
- String
- Bool
- BinaryFile
- EntityReference (links to other entities)
- Timestamp
- Choice (selection from predefined options)
- EntityList (collection of entity references)

Reading and writing fields:

```go
// Writing fields
store.Write(ctx, 
    qdata.NewRequest().
        SetEntityId(deviceId).
        SetFieldName("Status").
        SetValue(qdata.NewString("ON")),
)

// Reading fields
store.Read(ctx,
    qdata.NewRequest().
        SetEntityId(deviceId).
        SetFieldName("Status"),
)
```

### Using Bindings

Bindings provide a more object-oriented way to interact with entities and fields:

```go
// Get a multi-binding for batch operations
multi := qdata.NewMulti(store)

// Get an entity binding
device := multi.GetEntityById(ctx, deviceId)

// Read field values
status := device.GetField("Status").ReadString(ctx)

// Write field values
device.GetField("Status").WriteString(ctx, "OFF")

// Commit changes
multi.Commit(ctx)
```

### Database Queries

The Query interface provides SQL-like operations for finding entities:

```go
// Find all active devices
devices := qdata.NewQuery(store).
    Select("Status", "Name").
    From("Device").
    Where("Status").Equals("ON").
    Execute(ctx)

// Process results
for _, device := range devices {
    name := device.GetField("Name").GetString()
    status := device.GetField("Status").GetString()
    fmt.Printf("Device %s is %s\n", name, status)
}
```

### Real-time Notifications

The notification system allows workers to react to field changes:

```go
// Subscribe to field changes
token := store.Notify(
    ctx,
    qdata.NewConfig().
        SetEntityType("Device").
        SetFieldName("Status").
        SetContextFields("Name", "Location"),
    qdata.NewCallback(func(ctx context.Context, n qdata.Notification) {
        // Access changed value
        newValue := n.GetCurrent().GetValue().GetString()
        oldValue := n.GetPrevious().GetValue().GetString()
        
        // Access context fields
        name := n.GetContext(0).GetValue().GetString()
        location := n.GetContext(1).GetValue().GetString()
        
        fmt.Printf("Device %s in %s changed from %s to %s\n",
            name, location, oldValue, newValue)
    }),
)

// Later, unsubscribe
token.Unbind(ctx)
```

### Database Schema

Schemas define the structure of entities and their fields:

```go
// Define a schema
schema := qdata.NewSchema("Device").
    AddField("Status", "string").
    AddField("Name", "string").
    AddField("Location", "string").
    AddField("LastSeen", "timestamp")

// Set the schema
store.SetEntitySchema(ctx, schema)

// Retrieve schema
deviceSchema := store.GetEntitySchema(ctx, "Device")
for _, field := range deviceSchema.GetFields() {
    fmt.Printf("Field: %s, Type: %s\n", 
        field.GetFieldName(), 
        field.GetFieldType())
}
```

### Transaction Management

While the store operations are atomic by default, you can use MultiBinding for transaction-like operations:

```go
// Start a multi-binding session
multi := qdata.NewMulti(store)

// Perform multiple operations
deviceEntity := multi.GetEntityById(ctx, deviceId)
deviceEntity.GetField("Status").WriteString(ctx, "OFF")
deviceEntity.GetField("LastSeen").WriteTimestamp(ctx, time.Now())

configEntity := multi.GetEntityById(ctx, configId)
configEntity.GetField("LastUpdate").WriteTimestamp(ctx, time.Now())

// Commit all changes atomically
multi.Commit(ctx)
```

### Database Snapshots

The store supports creating and restoring snapshots of the entire database state:

```go
// Create a snapshot
snapshot := store.CreateSnapshot(ctx)

// Restore from snapshot
store.RestoreSnapshot(ctx, snapshot)

// Access snapshot data
for _, entity := range snapshot.GetEntities() {
    fmt.Printf("Entity: %s (%s)\n", entity.GetName(), entity.GetType())
}

for _, field := range snapshot.GetFields() {
    fmt.Printf("Field: %s = %v\n", field.GetFieldName(), field.GetValue())
}

for _, schema := range snapshot.GetSchemas() {
    fmt.Printf("Schema: %s\n", schema.GetType())
}
```

### Field Validation

The EntityFieldValidator ensures data integrity by validating required fields:

```go
// Create a validator
validator := qdata.NewEntityFieldValidator(store)

// Register required fields for entity types
validator.RegisterEntityFields("Service",
    "ApplicationName",
    "LogLevel",
)

// Validate fields exist in schema
if err := validator.ValidateFields(ctx); err != nil {
    log.Fatal("Schema validation failed:", err)
}
```

## Best Practices

### Entity Organization
- Design your entity hierarchy to reflect your domain model
- Use meaningful entity types and field names
- Keep entity relationships logical and maintainable

### Performance Optimization
- Use batch operations with MultiBinding when possible
- Subscribe to specific fields rather than entire entities
- Include relevant context fields in notifications to avoid additional queries

### Error Handling
- Always check connection status before operations
- Implement retry logic for transient failures
- Use proper error handling in notification callbacks

### Schema Management
- Define schemas early in application initialization
- Validate required fields during startup
- Consider schema versioning for application updates

### Memory Management
- Unbind notification tokens when no longer needed
- Clean up entity references in MultiBinding sessions
- Use snapshots judiciously as they hold entire database state

## Naming Conventions

Entity and field names in qlib follow strict naming conventions:

### Pascal Case
- Entity types must use PascalCase (e.g., `DeviceController`, `LightSwitch`)
- Field names must use PascalCase (e.g., `CurrentState`, `LastUpdateTime`)
- This applies to both schema definitions and runtime operations

```go
// Correct naming
schema := qdata.NewSchema("LightController").
    AddField("PowerState", "string").
    AddField("BrightnessLevel", "int").
    AddField("LastStateChange", "timestamp")

// Incorrect naming - will cause validation errors
schema := qdata.NewSchema("light_controller").
    AddField("power_state", "string").
    AddField("brightnessLevel", "int")
```

### Field Indirection

qlib supports referencing fields relative to an entity using the `->` delimiter. This powerful feature allows you to:
- Reference fields in parent or child entities
- Navigate the entity tree
- Create dynamic field references

```go
// Basic field indirection syntax:
// EntityReference->FieldName

// Examples:
store.Write(ctx,
    qdata.NewRequest().
        SetEntityId(deviceId).
        SetFieldName("Controller->Status").  // References Status field in Controller entity
        SetValue(qdata.NewString("ON")),
)

// Multiple levels of indirection
store.Notify(
    ctx,
    qdata.NewConfig().
        SetEntityType("Device").
        SetFieldName("Zone->Building->Status").  // References Building's Status via Zone
        SetContextFields(
            "Name",
            "Parent->Name",           // Get parent entity's Name
            "Zone->BuildingName",     // Get BuildingName from Zone entity
        ),
    qdata.NewCallback(func(ctx context.Context, n qdata.Notification) {
        deviceName := n.GetContext(0).GetValue().GetString()
        parentName := n.GetContext(1).GetValue().GetString()
        buildingName := n.GetContext(2).GetValue().GetString()
    }),
)

// Using EntityReference fields
schema := qdata.NewSchema("Device").
    AddField("Controller", "entityref").     // References another entity
    AddField("Zone", "entityref")

// Query using indirection
devices := qdata.NewQuery(store).
    Select(
        "Name",
        "Controller->Status",         // Status from referenced Controller
        "Zone->Building->Address",    // Address from Building referenced by Zone
    ).
    From("Device").
    Where("Controller->Type").Equals("MainController").
    Execute(ctx)
```

#### Common Use Cases for Indirection

1. **Hierarchical Data Access**
   ```go
   // Access parent data
   "Parent->Name"
   "Parent->Parent->Type"
   
   // Access child data
   "MainController->Status"
   "SubDevices->Count"
   ```

2. **Cross-Entity Relationships**
   ```go
   // Reference through entity reference fields
   "Zone->Controller->Status"
   "Building->MainPower->State"
   ```

3. **Dynamic Configuration**
   ```go
   // Use indirection in configuration entities
   "ConfigSource->Settings->UpdateInterval"
   "Template->DefaultValues->State"
   ```

4. **Notification Context**
   ```go
   qdata.NewConfig().
       SetEntityType("Device").
       SetFieldName("Status").
       SetContextFields(
           "Name",
           "Parent->Location",
           "Zone->Building->Name",
           "Controller->Type",
       )
   ```

#### Best Practices for Indirection

1. **Depth Management**
   - Keep indirection chains reasonably short
   - Consider creating direct references for frequently used deep paths
   - Document complex indirection paths

2. **Performance**
   - Cache frequently accessed indirect values when possible
   - Use MultiBinding for batch operations with indirection
   - Include necessary context fields in notifications to avoid additional lookups

3. **Error Handling**
   - Validate entity references exist before using indirection
   - Handle missing intermediate entities gracefully
   - Log invalid indirection paths for debugging

## Field Value Validation

qlib enforces strict type validation for field values. Each field type has specific validation rules:

### Type-Specific Validation

1. **Int**
   ```go
   // Valid int assignments
   field.WriteInt(ctx, 42)
   field.WriteInt(ctx, int64(1000))
   
   // Invalid - will cause validation error
   field.WriteInt(ctx, "42")    // string not allowed
   field.WriteInt(ctx, 3.14)    // float not allowed
   ```

2. **Float**
   ```go
   // Valid float assignments
   field.WriteFloat(ctx, 3.14)
   field.WriteFloat(ctx, float64(1.0))
   
   // Integer values are automatically converted
   field.WriteFloat(ctx, 42)    // becomes 42.0
   ```

3. **String**
   ```go
   // Valid string assignments
   field.WriteString(ctx, "hello")
   
   // Non-string values must be explicitly converted
   field.WriteString(ctx, fmt.Sprintf("%d", 42))
   ```

4. **Bool**
   ```go
   // Valid boolean assignments
   field.WriteBool(ctx, true)
   field.WriteBool(ctx, false)
   
   // Invalid - will cause validation error
   field.WriteBool(ctx, "true")   // string not allowed
   field.WriteBool(ctx, 1)        // int not allowed
   ```

5. **Timestamp**
   ```go
   // Valid timestamp assignments
   field.WriteTimestamp(ctx, time.Now())
   field.WriteTimestamp(ctx, time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC))
   ```

6. **EntityReference**
   ```go
   // Valid entity reference assignments
   field.WriteEntityReference(ctx, "DeviceId123")
   
   // Best practice: validate entity exists
   if store.EntityExists(ctx, "DeviceId123") {
       field.WriteEntityReference(ctx, "DeviceId123")
   }
   ```

7. **BinaryFile**
   ```go
   // Valid binary file assignments
   content := []byte{...}
   encoded := qdata.FileEncode(content)
   field.WriteBinaryFile(ctx, encoded)
   
   // Reading binary files
   encoded := field.ReadBinaryFile(ctx)
   content := qdata.FileDecode(encoded)
   ```

### Working with Choice Fields

Choice fields represent a selected option from a predefined list of choices:

```go
// Define schema with choice field
schema := qdata.NewSchema("Device").
    AddField("Status", "string").
    AddField("OperatingMode", "choice")

// Set the available options in the schema
schema.SetChoiceOptions(ctx, store, "Device", "OperatingMode", 
    []string{"Normal", "Eco", "Boost", "Away"})

// Write a choice value - just the selected index
device.GetField("OperatingMode").WriteChoice(ctx, 0)  // Select "Normal" option

// Read choice value with rich interface
choice := device.GetField("OperatingMode").ReadChoice(ctx)
selectedIndex := choice.GetSelectedIndex()
selectedMode := choice.GetSelectedValue()

// Get options from schema through the binding
allModes := device.GetField("OperatingMode").GetChoiceOptions(ctx)

// Update selection
device.GetField("OperatingMode").SelectChoiceByValue(ctx, "Eco")
```

### Working with EntityList Fields

EntityList fields store collections of entity references:

```go
// Define schema with entitylist field
schema := qdata.NewSchema("User").
    AddField("Name", "string").
    AddField("Devices", "entitylist")

// Write an entity list
user.GetField("Devices").WriteEntityList(ctx, 
    []string{"device-1", "device-2", "device-3"}
)

// Read entity list with rich interface
deviceList := user.GetField("Devices").ReadEntityList(ctx)
allDevices := deviceList.GetEntities()
deviceCount := deviceList.Count()

// Use convenience methods to modify list
user.GetField("Devices").AddEntityToList(ctx, "device-4")
user.GetField("Devices").RemoveEntityFromList(ctx, "device-2")
hasDevice := user.GetField("Devices").EntityListContains(ctx, "device-1")
```

### Best Practices for Field Values

1. **Type Safety**
   ```go
   // Use type-specific methods
   if field.IsInt() {
       value := field.ReadInt(ctx)
   } else if field.IsString() {
       value := field.ReadString(ctx)
   }
   ```

2. **Null Values**
   ```go
   // Check for nil values
   if !field.GetValue().IsNil() {
       value := field.ReadString(ctx)
   }
   ```

3. **Value Conversion**
   ```go
   // Convert values explicitly when needed
   intValue := int64(field.ReadFloat(ctx))
   strValue := fmt.Sprintf("%d", field.ReadInt(ctx))
   ```

4. **Batch Updates**
   ```go
   // Use MultiBinding for multiple value updates
   multi := qdata.NewMulti(store)
   entity := multi.GetEntityById(ctx, entityId)
   
   entity.GetField("IntValue").WriteInt(ctx, 42)
   entity.GetField("FloatValue").WriteFloat(ctx, 3.14)
   entity.GetField("StringValue").WriteString(ctx, "hello")
   
   multi.Commit(ctx)
   ```

5. **Error Handling**
   ```go
   // Handle type conversion errors
   field := entity.GetField("Value")
   if !field.IsFloat() {
       log.Error("Expected float field")
       return
   }
   value := field.ReadFloat(ctx)
   ```

6. **Value Validation in Schema**
   ```go
   // Define field types in schema
   schema := qdata.NewSchema("Device").
       AddField("Status", "string").
       AddField("Temperature", "float").
       AddField("IsActive", "bool").
       AddField("LastUpdate", "timestamp").
       AddField("Controller", "entityref")
   ```

## Application Readiness

qlib provides built-in support for application readiness checks through the Readiness worker. This ensures your application only becomes active when all required dependencies and conditions are met.

### Readiness Worker

The Readiness worker manages the application's ready state:

```go
// Create readiness worker
readinessWorker := qapp.NewReadiness()

// Add readiness criteria
readinessWorker.AddCriteria(qapp.NewStoreConnectedCriteria(store))
readinessWorker.AddCriteria(qapp.NewSchemaValidityCriteria(store))

// Add custom criteria
readinessWorker.AddCriteria(qapp.ReadinessCriteriaFunc(func() bool {
    return myCondition // e.g., database is connected
}))

// Handle readiness changes
readinessWorker.BecameReady().Connect(func() {
    // Application is now ready
})

readinessWorker.BecameUnready().Connect(func() {
    // Application is no longer ready
})

// Add to application
app.AddWorker(readinessWorker)
```

### Best Practices for Readiness

1. **Critical Dependencies**
   - Add criteria for all critical services
   - Monitor connection states
   - Validate required configurations

2. **Schema Validation**
   - Use SchemaValidityCriteria to ensure schema integrity
   - Validate required fields exist
   - Check field types match expectations

3. **Custom Conditions**
   - Implement custom ReadinessCriteria for specific needs
   - Keep criteria checks lightweight
   - Avoid blocking operations

4. **Monitoring**
   - Track readiness state changes
   - Log criteria failures
   - Implement health checks based on readiness

Example readiness-aware worker:

```go
type MyWorker struct {
    isReady bool
}

func (w *MyWorker) Init(ctx context.Context, h qapp.Handle) {
    readiness.BecameReady().Connect(func() {
        w.isReady = true
        // Start operations
    })
    
    readiness.BecameUnready().Connect(func() {
        w.isReady = false
        // Stop operations
    })
}

func (w *MyWorker) DoWork(ctx context.Context) {
    if (!w.isReady) {
        return // Only work when ready
    }
    // Perform operations
}
```

## Getting Started

1. Create a new application
2. Implement workers for your business logic
3. Set up database schema and entity structure
4. Add notification handlers
5. Run the application

Example:

```go
func main() {
    app := qapp.NewApplication("myapp")
    
    // Add workers
    app.AddWorker(NewMyWorker())
    
    // Run the application
    app.Execute()
}
```

## Application Lifecycle Management

Applications in qlib follow a strict initialization and shutdown sequence to ensure proper resource management and graceful termination.

### Initialization Sequence

```go
func main() {
    // Create application with unique name
    app := qapp.NewApplication("myapp")

    // Configure workers before Execute()
    storeWorker := qapp.NewStore(myStore)
    
    // Add workers
    app.AddWorker(storeWorker)
    app.AddWorker(myBusinessWorker)

    // Execute handles init/deinit
    app.Execute()
}
```

### Worker Initialization

Workers should properly initialize their resources:

```go
type MyWorker struct {
    store qdata.Store
    subscriptions []qdata.NotificationToken
}

func (w *MyWorker) Init(ctx context.Context, h qapp.Handle) {
    // Set up notification subscriptions
    w.subscriptions = append(w.subscriptions,
        w.store.Notify(ctx,
            qdata.NewConfig().
                SetEntityType("Device").
                SetFieldName("Status"),
            qdata.NewCallback(w.onStatusChange),
        ),
    )

    // Initialize resources with timeout
    timeoutCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
    defer cancel()
    
    if err := w.initializeResource(timeoutCtx); err != nil {
        log.Panic("Failed to initialize: %v", err)
    }
}
```

### Graceful Shutdown

Workers must clean up resources on shutdown:

```go
func (w *MyWorker) Deinit(ctx context.Context) {
    // Unsubscribe from notifications
    for _, sub := range w.subscriptions {
        sub.Unbind(ctx)
    }

    // Close connections
    if w.conn != nil {
        w.conn.Close()
    }

    // Wait for pending operations
    w.wg.Wait()
}
```

### Error Handling

Best practices for handling errors in workers:

```go
func (w *MyWorker) DoWork(ctx context.Context) {
    // Check context cancellation
    if ctx.Err() != nil {
        return
    }

    // Handle recoverable errors
    if err := w.doSomething(ctx); err != nil {
        qlog.Error("Operation failed: %v", err)
        // Update status to reflect error
        w.store.Write(ctx,
            qdata.NewRequest().
                SetEntityId(w.entityId).
                SetFieldName("Error").
                SetValue(qdata.NewString(err.Error())),
        )
        return
    }

    // Handle fatal errors
    if err := w.criticalOperation(ctx); err != nil {
        qlog.Panic("Critical error: %v", err)
        // Application will shut down
    }
}
```

### Startup Dependencies

Managing worker dependencies:

```go
type MyWorker struct {
    store qdata.Store
    isStoreConnected bool
}

func (w *MyWorker) Init(ctx context.Context, h qapp.Handle) {
    // Subscribe to store connection status
    w.store.Connected().Connect(func(ctx context.Context) {
        w.isStoreConnected = true
    })
    
    w.store.Disconnected().Connect(func(ctx context.Context) {
        w.isStoreConnected = false
    })
}

func (w *MyWorker) DoWork(ctx context.Context) {
    // Wait for dependencies
    if !w.isStoreConnected {
        return
    }
    
    // Proceed with work
}
```

### Signal Handling

The application handles system signals:

```go
func main() {
    app := qapp.NewApplication("myapp")
    
    // App.Execute() handles:
    // - SIGINT (Ctrl+C)
    // - SIGTERM
    // 
    // Workers get ctx.Done() on signal
    app.Execute()
}

// In your worker
func (w *MyWorker) DoWork(ctx context.Context) {
    select {
    case <-ctx.Done():
        // Clean up and return
        return
    default:
        // Do work
    }
}
```

### Environment Configuration

Environment variables that affect application behavior:

```bash
# Application identification
APP_NAME=myapp                  # Override application name
Q_IN_DOCKER=true               # Running in container
```

## Best Practices Summary

1. **Worker State**
   - Initialize all state in Init()
   - Clean up all resources in Deinit()
   - Use context cancellation for termination

2. **Resource Management**
   - Track and clean up subscriptions
   - Close connections and channels
   - Wait for goroutines to finish

3. **Error Handling**
   - Use qlog.Error for recoverable errors
   - Use qlog.Panic for fatal errors
   - Update entity status on errors

4. **Dependencies**
   - Check dependency status before operations
   - Handle dependency failures gracefully
   - Implement proper retry logic