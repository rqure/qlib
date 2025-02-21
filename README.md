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