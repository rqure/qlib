# qlib

A Go library for building event-driven, worker-based applications with a hierarchical object database.

## Overview

qlib provides a framework for building applications with the following key features:

- Single-threaded application model with worker-based architecture
- Hierarchical object database with real-time notifications
- Built-in leadership election and high availability support
- Event-driven communication between workers
- WebSocket-based communication using Protocol Buffers

## Core Concepts

### Applications

Applications in qlib are built around a main thread and a set of workers. Create a new application:

```go
app := app.NewApplication("myapp")
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
    store data.Store
}

func (w *MyWorker) Init(ctx context.Context, h app.Handle) {
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
    notification.NewConfig().
        SetEntityType("Service").
        SetFieldName("Status"),
    notification.NewCallback(func(ctx context.Context, n data.Notification) {
        // Handle field change
    }),
)
```

## Database (Store) Interface

The Store interface is the core component for data persistence and real-time updates. It provides a rich API for managing entities, fields, and notifications.

### Basic Store Operations

```go
type Store interface {
    // Connection management
    Connect(context.Context)
    Disconnect(context.Context)
    IsConnected(context.Context) bool

    // Entity operations
    CreateEntity(ctx context.Context, entityType, parentId, name string)
    GetEntity(ctx context.Context, entityId string) Entity
    DeleteEntity(ctx context.Context, entityId string)
    FindEntities(ctx context.Context, entityType string) []string

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
- Transformation

Reading and writing fields:

```go
// Writing fields
store.Write(ctx, 
    request.New().
        SetEntityId(deviceId).
        SetFieldName("Status").
        SetValue(value.NewString("ON")),
)

// Reading fields
store.Read(ctx,
    request.New().
        SetEntityId(deviceId).
        SetFieldName("Status"),
)
```

### Using Bindings

Bindings provide a more object-oriented way to interact with entities and fields:

```go
// Get a multi-binding for batch operations
multi := binding.NewMulti(store)

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
devices := query.New(store).
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
    notification.NewConfig().
        SetEntityType("Device").
        SetFieldName("Status").
        SetContextFields("Name", "Location"),
    notification.NewCallback(func(ctx context.Context, n data.Notification) {
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
schema := schema.New("Device").
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
multi := binding.NewMulti(store)

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
validator := data.NewEntityFieldValidator(store)

// Register required fields for entity types
validator.RegisterEntityFields("Service",
    "Leader",
    "Candidates",
    "HeartbeatTrigger",
    "ApplicationName",
    "LogLevel",
)

// Validate fields exist in schema
if err := validator.ValidateFields(ctx); err != nil {
    log.Fatal("Schema validation failed:", err)
}
```

### Best Practices for Store Usage

1. **Entity Organization**
   - Design your entity hierarchy to reflect your domain model
   - Use meaningful entity types and field names
   - Keep entity relationships logical and maintight

2. **Performance Optimization**
   - Use batch operations with MultiBinding when possible
   - Subscribe to specific fields rather than entire entities
   - Include relevant context fields in notifications to avoid additional queries

3. **Error Handling**
   - Always check connection status before operations
   - Implement retry logic for transient failures
   - Use proper error handling in notification callbacks

4. **Schema Management**
   - Define schemas early in application initialization
   - Validate required fields during startup
   - Consider schema versioning for application updates

5. **Memory Management**
   - Unbind notification tokens when no longer needed
   - Clean up entity references in MultiBinding sessions
   - Use snapshots judiciously as they hold entire database state

### Naming Conventions

Entity and field names in qlib follow strict naming conventions:

1. **Pascal Case**
   - Entity types must use PascalCase (e.g., `DeviceController`, `LightSwitch`)
   - Field names must use PascalCase (e.g., `CurrentState`, `LastUpdateTime`)
   - This applies to both schema definitions and runtime operations

```go
// Correct naming
schema := schema.New("LightController").
    AddField("PowerState", "string").
    AddField("BrightnessLevel", "int").
    AddField("LastStateChange", "timestamp")

// Incorrect naming - will cause validation errors
schema := schema.New("light_controller").
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
    request.New().
        SetEntityId(deviceId).
        SetFieldName("Controller->Status").  // References Status field in Controller entity
        SetValue(value.NewString("ON")),
)

// Multiple levels of indirection
store.Notify(
    ctx,
    notification.NewConfig().
        SetEntityType("Device").
        SetFieldName("Zone->Building->Status").  // References Building's Status via Zone
        SetContextFields(
            "Name",
            "Parent->Name",           // Get parent entity's Name
            "Zone->BuildingName",     // Get BuildingName from Zone entity
        ),
    notification.NewCallback(func(ctx context.Context, n data.Notification) {
        deviceName := n.GetContext(0).GetValue().GetString()
        parentName := n.GetContext(1).GetValue().GetString()
        buildingName := n.GetContext(2).GetValue().GetString()
    }),
)

// Using EntityReference fields
schema := schema.New("Device").
    AddField("Controller", "entityref").     // References another entity
    AddField("Zone", "entityref")

// Query using indirection
devices := query.New(store).
    Select(
        "Name",
        "Controller->Status",         // Status from referenced Controller
        "Zone->Building->Address",    // Address from Building referenced by Zone
    ).
    From("Device").
    Where("Controller->Type").Equals("MainController").
    Execute(ctx)
```

Common Use Cases for Indirection:
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
   notification.NewConfig().
       SetEntityType("Device").
       SetFieldName("Status").
       SetContextFields(
           "Name",
           "Parent->Location",
           "Zone->Building->Name",
           "Controller->Type",
       )
   ```

Best Practices for Indirection:
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

### Field Value Validation

qlib enforces strict type validation for field values. Each field type has specific validation rules:

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
   encoded := data.FileEncode(content)
   field.WriteBinaryFile(ctx, encoded)
   
   // Reading binary files
   encoded := field.ReadBinaryFile(ctx)
   content := data.FileDecode(encoded)
   ```

8. **Transformation**
   ```go
   // Transformation fields contain scripts/expressions
   field.WriteTransformation(ctx, "value * 2")
   field.WriteTransformation(ctx, "Parent->Value + 10")
   ```

Best Practices for Field Values:

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
   multi := binding.NewMulti(store)
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
   schema := schema.New("Device").
       AddField("Status", "string").
       AddField("Temperature", "float").
       AddField("IsActive", "bool").
       AddField("LastUpdate", "timestamp").
       AddField("Controller", "entityref")
   ```

## Leadership Election

qlib provides built-in support for high availability through leadership election. This ensures that in a distributed setup, only one instance of your application is the active leader.

### Leadership Worker

The Leadership worker manages the election process:

```go
// Create leadership worker
leaderWorker := workers.NewLeadership(store)

// Add availability criteria
leaderWorker.AddAvailabilityCriteria(func() bool {
    return myCondition // e.g., database is connected
})

// Handle leadership changes
leaderWorker.BecameLeader().Connect(func(ctx context.Context) {
    // This instance is now the leader
})

leaderWorker.BecameFollower().Connect(func(ctx context.Context) {
    // This instance is now a follower
})

leaderWorker.LosingLeadership().Connect(func(ctx context.Context) {
    // Cleanup before losing leadership
})

// Add to application
app.AddWorker(leaderWorker)
```

### Leadership Store

The leadership election uses Redis for coordination:

```go
type LeadershipStore interface {
    Connect(context.Context)
    Disconnect(context.Context)
    IsConnected(context.Context) bool
    
    // Leadership management
    AcquireLock(ctx context.Context, key string) bool
    ReleaseLock(ctx context.Context, key string)
    RefreshLock(ctx context.Context, key string) bool
}
```

### Configuration

Leadership election can be configured through environment variables:

```bash
# Redis connection for leadership coordination
Q_LEADER_STORE_ADDR=redis:6379
Q_LEADER_STORE_PASSWORD=mypassword

# Instance identification
Q_IN_DOCKER=true  # Uses container hostname as instance ID
```

### Best Practices for High Availability

1. **Multiple Instances**
   - Run multiple instances of your application
   - Configure each instance with the same Redis endpoint
   - Use different instance IDs for each instance

2. **Graceful Leadership Transitions**
   - Handle BecameLeader/BecameFollower signals
   - Clean up resources on LosingLeadership
   - Implement proper shutdown procedures

3. **Monitoring**
   - Track leadership status changes
   - Monitor Redis connection health
   - Log leadership transitions

4. **Leader-Only Operations**
   - Restrict certain operations to leader instance
   - Handle failover scenarios gracefully
   - Use leader status for write operations

Example leadership-aware worker:

```go
type MyWorker struct {
    isLeader bool
}

func (w *MyWorker) Init(ctx context.Context, h app.Handle) {
    leadership.BecameLeader().Connect(func(ctx context.Context) {
        w.isLeader = true
        // Start leader-only operations
    })
    
    leadership.BecameFollower().Connect(func(ctx context.Context) {
        w.isLeader = false
        // Stop leader-only operations
    })
}

func (w *MyWorker) DoWork(ctx context.Context) {
    if (!w.isLeader) {
        return // Only leader performs this work
    }
    // Perform leader-only operations
}
```

## Web Communication

The library uses Protocol Buffers for structured web communication over WebSocket connections. Messages follow a standard format:

```protobuf
message WebMessage {
    WebHeader header = 1;
    google.protobuf.Any payload = 2;
}

message WebHeader {
    string id = 1;
    google.protobuf.Timestamp timestamp = 2;
    AuthenticationStatusEnum authenticationStatus = 3;
}
```

### Working with WebMessages

1. **Sending Messages**

```go
// Create and send a message to a specific client
client.Write(&protobufs.WebMessage{
    Header: &protobufs.WebHeader{
        Id: uuid.New().String(),
        Timestamp: timestamppb.Now(),
    },
    Payload: myPayload, // any proto message
})
```

2. **Handling Messages**

```go
// Set up a message handler
client.SetMessageHandler(func(c web.Client, m web.Message) {
    switch payload := m.GetPayload().(type) {
    case *protobufs.WebConfigCreateEntityRequest:
        // Handle entity creation request
    case *protobufs.WebRuntimeDatabaseRequest:
        // Handle database operation request
    }
})
```

### Common Message Types

- Database Operations:
  - `WebRuntimeDatabaseRequest`: Read/write field values
  - `WebConfigCreateEntityRequest`: Create new entities
  - `WebConfigDeleteEntityRequest`: Delete entities
  - `WebConfigGetEntityRequest`: Retrieve entity details

- Schema Operations:
  - `WebConfigGetEntitySchemaRequest`: Get entity type schema
  - `WebConfigSetEntitySchemaRequest`: Update entity schema
  - `WebConfigGetEntityTypesRequest`: List available entity types

- Notifications:
  - `WebRuntimeRegisterNotificationRequest`: Subscribe to changes
  - `WebRuntimeGetNotificationsRequest`: Get pending notifications
  - `WebRuntimeUnregisterNotificationRequest`: Remove subscriptions

Example database operation:

```go
// Send a database read request
client.Write(&protobufs.WebMessage{
    Header: &protobufs.WebHeader{
        Id: uuid.New().String(),
        Timestamp: timestamppb.Now(),
    },
    Payload: &anypb.Any{
        TypeUrl: "WebRuntimeDatabaseRequest",
        Value: &protobufs.WebRuntimeDatabaseRequest{
            RequestType: protobufs.WebRuntimeDatabaseRequest_READ,
            Requests: []*protobufs.DatabaseRequest{
                {
                    Id: entityId,
                    Field: "Status",
                },
            },
        },
    },
})
```

## Worker Communication Patterns

Workers in qlib can communicate with each other through the Store using several patterns:

### Direct Field Communication

Workers can communicate by writing to and monitoring specific fields:

```go
// Worker A: Sender
type SenderWorker struct {
    store data.Store
}

func (w *SenderWorker) DoWork(ctx context.Context) {
    w.store.Write(ctx,
        request.New().
            SetEntityId("command-entity").
            SetFieldName("Command").
            SetValue(value.NewString("start_operation")),
    )
}

// Worker B: Receiver
type ReceiverWorker struct {
    store data.Store
}

func (w *ReceiverWorker) Init(ctx context.Context, h app.Handle) {
    w.store.Notify(
        ctx,
        notification.NewConfig().
            SetEntityType("command-entity").
            SetFieldName("Command"),
        notification.NewCallback(w.onCommand),
    )
}

func (w *ReceiverWorker) onCommand(ctx context.Context, n data.Notification) {
    command := n.GetCurrent().GetValue().GetString()
    if command == "start_operation" {
        // Handle command
    }
}
```

### Request-Response Pattern

For request-response style communication:

```go
// Request entity schema
schema := schema.New("Request").
    AddField("Status", "string").     // "pending", "processing", "completed"
    AddField("Data", "string").       // Request data
    AddField("Response", "string").   // Response data
    AddField("Error", "string")       // Error message if any

// Worker A: Client
func (w *ClientWorker) makeRequest(ctx context.Context, data string) {
    // Create request entity
    w.store.CreateEntity(ctx, "Request", parentId, "request-1")
    
    // Write request data
    w.store.Write(ctx,
        request.New().
            SetEntityId("request-1").
            SetFieldName("Data").
            SetValue(value.NewString(data)),
        request.New().
            SetEntityId("request-1").
            SetFieldName("Status").
            SetValue(value.NewString("pending")),
    )
    
    // Monitor for response
    w.store.Notify(
        ctx,
        notification.NewConfig().
            SetEntityId("request-1").
            SetFieldName("Status").
            SetContextFields("Response", "Error"),
        notification.NewCallback(w.onResponse),
    )
}

// Worker B: Server
func (w *ServerWorker) Init(ctx context.Context, h app.Handle) {
    w.store.Notify(
        ctx,
        notification.NewConfig().
            SetEntityType("Request").
            SetFieldName("Status"),
        notification.NewCallback(w.onRequest),
    )
}

func (w *ServerWorker) onRequest(ctx context.Context, n data.Notification) {
    status := n.GetCurrent().GetValue().GetString()
    if status != "pending" {
        return
    }
    
    requestId := n.GetCurrent().GetEntityId()
    requestEntity := w.store.GetEntity(ctx, requestId)
    
    // Process request and write response
    w.store.Write(ctx,
        request.New().
            SetEntityId(requestId).
            SetFieldName("Response").
            SetValue(value.NewString("result")),
        request.New().
            SetEntityId(requestId).
            SetFieldName("Status").
            SetValue(value.NewString("completed")),
    )
}
```

### Pub/Sub Pattern

For broadcast-style communication:

```go
// Publisher worker publishes events
func (w *PublisherWorker) publishEvent(ctx context.Context, eventType, data string) {
    w.store.Write(ctx,
        request.New().
            SetEntityId("events").
            SetFieldName(eventType).
            SetValue(value.NewString(data)),
    )
}

// Subscriber worker listens for events
func (w *SubscriberWorker) Init(ctx context.Context, h app.Handle) {
    w.store.Notify(
        ctx,
        notification.NewConfig().
            SetEntityId("events").
            SetFieldName("user_login"),  // Subscribe to specific event type
        notification.NewCallback(w.onUserLogin),
    )
}

func (w *SubscriberWorker) onUserLogin(ctx context.Context, n data.Notification) {
    userData := n.GetCurrent().GetValue().GetString()
    // Handle user login event
}
```

### Best Practices for Worker Communication

1. **Entity Design**
   - Create dedicated entities for inter-worker communication
   - Use clear field names for commands and events
   - Include timestamps for tracking message age

2. **Error Handling**
   - Include error fields in request-response patterns
   - Implement timeouts for pending requests
   - Handle missing or invalid data gracefully

3. **Performance**
   - Use appropriate notification patterns (change-only vs. all updates)
   - Clean up completed request entities
   - Batch related field updates using MultiBinding

4. **Testing**
   - Test communication patterns in isolation
   - Verify proper cleanup of temporary entities
   - Simulate network and timing issues

## Getting Started

1. Create a new application
2. Implement workers for your business logic
3. Set up database schema and entity structure
4. Add notification handlers
5. Run the application

Example:

```go
func main() {
    app := app.NewApplication("myapp")
    
    // Add workers
    app.AddWorker(NewMyWorker())
    
    // Run the application
    app.Execute()
}
```

## Best Practices

1. Keep workers focused on single responsibilities
2. Use the main thread for coordination
3. Leverage notifications for event-driven architecture
4. Structure your database hierarchy logically
5. Use leadership election for high availability

## Environment Variables

- `Q_LEADER_STORE_ADDR`: Redis address for leadership election (default: "redis:6379")
- `Q_LEADER_STORE_PASSWORD`: Redis password
- `Q_IN_DOCKER`: Set when running in Docker container

## Application Lifecycle Management

Applications in qlib follow a strict initialization and shutdown sequence to ensure proper resource management and graceful termination.

### Initialization Sequence

```go
func main() {
    // Create application with unique name
    app := app.NewApplication("myapp")

    // Configure workers before Execute()
    storeWorker := workers.NewStore(myStore)
    webWorker := workers.NewWeb(":8080")
    
    // Order matters - add core workers first
    app.AddWorker(storeWorker)
    app.AddWorker(leadershipWorker)
    app.AddWorker(webWorker)
    app.AddWorker(myBusinessWorker)

    // Execute handles init/deinit
    app.Execute()
}
```

### Worker Initialization

Workers should properly initialize their resources:

```go
type MyWorker struct {
    store data.Store
    subscriptions []data.NotificationToken
}

func (w *MyWorker) Init(ctx context.Context, h app.Handle) {
    // Set up notification subscriptions
    w.subscriptions = append(w.subscriptions,
        w.store.Notify(ctx,
            notification.NewConfig().
                SetEntityType("Device").
                SetFieldName("Status"),
            notification.NewCallback(w.onStatusChange),
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
        log.Error("Operation failed: %v", err)
        // Update status to reflect error
        w.store.Write(ctx,
            request.New().
                SetEntityId(w.entityId).
                SetFieldName("Error").
                SetValue(value.NewString(err.Error())),
        )
        return
    }

    // Handle fatal errors
    if err := w.criticalOperation(ctx); err != nil {
        log.Panic("Critical error: %v", err)
        // Application will shut down
    }
}
```

### Startup Dependencies

Managing worker dependencies:

```go
type MyWorker struct {
    store data.Store
    isStoreConnected bool
}

func (w *MyWorker) Init(ctx context.Context, h app.Handle) {
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
    app := app.NewApplication("myapp")
    
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

# Store configuration
Q_LEADER_STORE_ADDR=redis:6379 # Leadership store address
Q_LEADER_STORE_PASSWORD=secret # Leadership store password

# Runtime behavior
Q_LOG_LEVEL=INFO              # Application log level
Q_LIB_LOG_LEVEL=WARN         # Library log level
```

Best Practices:
1. **Worker State**
   - Initialize all state in Init()
   - Clean up all resources in Deinit()
   - Use context cancellation for termination

2. **Resource Management**
   - Track and clean up subscriptions
   - Close connections and channels
   - Wait for goroutines to finish

3. **Error Handling**
   - Use log.Error for recoverable errors
   - Use log.Panic for fatal errors
   - Update entity status on errors

4. **Dependencies**
   - Check dependency status before operations
   - Handle dependency failures gracefully
   - Implement proper retry logic