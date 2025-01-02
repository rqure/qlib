package store

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"encoding/base64"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rqure/qlib/pkg/app"
	"github.com/rqure/qlib/pkg/data"
	"github.com/rqure/qlib/pkg/data/entity"
	"github.com/rqure/qlib/pkg/data/field"
	"github.com/rqure/qlib/pkg/data/notification"
	"github.com/rqure/qlib/pkg/data/request"
	"github.com/rqure/qlib/pkg/data/snapshot"
	"github.com/rqure/qlib/pkg/data/transformer"
	"github.com/rqure/qlib/pkg/log"
	"github.com/rqure/qlib/pkg/protobufs"
	"google.golang.org/protobuf/proto"
)

const createTablesSQL = `
CREATE TABLE IF NOT EXISTS Entities (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    parent_id TEXT,
    type TEXT NOT NULL,
    children TEXT[] NOT NULL
);

CREATE TABLE IF NOT EXISTS EntitySchema (
    entity_type TEXT NOT NULL,
    field_name TEXT NOT NULL,
    field_type TEXT NOT NULL,
    rank INTEGER NOT NULL,
    PRIMARY KEY (entity_type, field_name)
);

CREATE TABLE IF NOT EXISTS Strings (
    entity_id TEXT NOT NULL,
    field_name TEXT NOT NULL,
    field_value TEXT,
    write_time TIMESTAMP NOT NULL,
    writer TEXT NOT NULL
);

-- Similar tables for other types...

CREATE TABLE IF NOT EXISTS NotificationConfigEntityId (
    id SERIAL PRIMARY KEY,
    entity_id TEXT NOT NULL,
    field_name TEXT NOT NULL,
    context_fields TEXT[] NOT NULL,
    notify_on_change BOOLEAN NOT NULL,
    service_id TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS NotificationConfigEntityType (
    id SERIAL PRIMARY KEY,
    entity_type TEXT NOT NULL,
    field_name TEXT NOT NULL,
    context_fields TEXT[] NOT NULL,
    notify_on_change BOOLEAN NOT NULL,
    service_id TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS Notifications (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMP NOT NULL,
    service_id TEXT NOT NULL,
    acknowledged BOOLEAN NOT NULL DEFAULT false,
    notification BYTEA NOT NULL
);
`

type PostgresConfig struct {
	ConnectionString string
}

type Postgres struct {
	pool        *pgxpool.Pool
	config      PostgresConfig
	callbacks   map[string][]data.NotificationCallback
	transformer data.Transformer
}

func NewPostgres(config PostgresConfig) data.Store {
	s := &Postgres{
		config:    config,
		callbacks: map[string][]data.NotificationCallback{},
	}
	s.transformer = transformer.NewTransformer(s)
	return s
}

func (s *Postgres) Connect(ctx context.Context) {
	s.Disconnect(ctx)

	config, err := pgxpool.ParseConfig(s.config.ConnectionString)
	if err != nil {
		log.Error("Failed to parse connection string: %v", err)
		return
	}

	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		log.Error("Failed to create connection pool: %v", err)
		return
	}

	s.pool = pool

	if err := s.initializeDatabase(ctx); err != nil {
		log.Error("Failed to initialize database: %v", err)
		s.Disconnect(ctx)
		return
	}

	// Listen for notifications
	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		log.Error("Failed to acquire connection: %v", err)
		return
	}
	defer conn.Release()

	_, err = conn.Exec(ctx, fmt.Sprintf("LISTEN %s", s.getServiceId()))
	if err != nil {
		log.Error("Failed to listen for notifications: %v", err)
	}
}

func (s *Postgres) Disconnect(ctx context.Context) {
	if s.pool != nil {
		s.pool.Close()
		s.pool = nil
	}
}

func (s *Postgres) IsConnected(ctx context.Context) bool {
	if s.pool == nil {
		return false
	}
	return s.pool.Ping(ctx) == nil
}

func (s *Postgres) GetEntity(ctx context.Context, entityId string) data.Entity {
	row := s.pool.QueryRow(ctx, `
		SELECT id, name, parent_id, type, children
		FROM Entities
		WHERE id = $1
	`, entityId)

	var e protobufs.DatabaseEntity
	var children []string
	err := row.Scan(&e.Id, &e.Name, &e.Parent, &e.Type, &children)
	if err != nil {
		if !errors.Is(err, pgx.ErrNoRows) {
			log.Error("Failed to get entity: %v", err)
		}
		return nil
	}

	e.Children = make([]*protobufs.EntityReference, len(children))
	for i, child := range children {
		e.Children[i] = &protobufs.EntityReference{Raw: child}
	}

	return entity.FromEntityPb(&e)
}

func (s *Postgres) SetEntity(ctx context.Context, e data.Entity) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		log.Error("Failed to begin transaction: %v", err)
		return
	}
	defer tx.Rollback(ctx)

	// Update entity or create if it doesn't exist
	_, err = tx.Exec(ctx, `
		INSERT INTO Entities (id, name, parent_id, type, children)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (id) DO UPDATE
		SET name = $2, parent_id = $3, type = $4, children = $5
	`, e.GetId(), e.GetName(), e.GetParentId(), e.GetType(), e.GetChildrenIds())
	if err != nil {
		log.Error("Failed to update entity: %v", err)
		return
	}

	err = tx.Commit(ctx)
	if err != nil {
		log.Error("Failed to commit transaction: %v", err)
	}
}

func (s *Postgres) CreateEntity(ctx context.Context, entityType, parentId, name string) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		log.Error("Failed to begin transaction: %v", err)
		return
	}
	defer tx.Rollback(ctx)

	entityId := uuid.New().String()
	_, err = tx.Exec(ctx, `
		INSERT INTO Entities (id, name, parent_id, type, children)
		VALUES ($1, $2, $3, $4, $5)
	`, entityId, name, parentId, entityType, []string{})

	if err != nil {
		log.Error("Failed to create entity: %v", err)
		return
	}

	if parentId != "" {
		_, err = tx.Exec(ctx, `
			UPDATE Entities 
			SET children = array_append(children, $1)
			WHERE id = $2
		`, entityId, parentId)

		if err != nil {
			log.Error("Failed to update parent entity: %v", err)
			return
		}
	}

	// Initialize fields with default values
	schema := s.GetEntitySchema(ctx, entityType)
	if schema != nil {
		for _, f := range schema.GetFields() {
			req := request.New().SetEntityId(entityId).SetFieldName(f.GetFieldName())
			s.writeWithTx(ctx, tx, req)
		}
	}

	err = tx.Commit(ctx)
	if err != nil {
		log.Error("Failed to commit transaction: %v", err)
	}
}

func (s *Postgres) Read(ctx context.Context, requests ...data.Request) {
	for _, r := range requests {
		indirectField, indirectEntity := s.resolveIndirection(ctx, r.GetFieldName(), r.GetEntityId())
		if indirectField == "" || indirectEntity == "" {
			continue
		}

		entity := s.GetEntity(ctx, indirectEntity)
		if entity == nil {
			continue
		}

		schema := s.GetFieldSchema(ctx, entity.GetType(), indirectField)
		if schema == nil {
			continue
		}

		tableName := s.getTableForType(schema.GetFieldType())
		if tableName == "" {
			continue
		}

		row := s.pool.QueryRow(ctx, fmt.Sprintf(`
			SELECT field_value, write_time, writer
			FROM %s
			WHERE entity_id = $1 AND field_name = $2
		`, tableName), indirectEntity, indirectField)

		var fieldValue interface{}
		var writeTime time.Time
		var writer string

		err := row.Scan(&fieldValue, &writeTime, &writer)
		if err != nil {
			if !errors.Is(err, pgx.ErrNoRows) {
				log.Error("Failed to read field: %v", err)
			}
			continue
		}

		value := s.convertToValue(schema.GetFieldType(), fieldValue)
		if value == nil {
			continue
		}

		r.SetValue(value)
		r.SetWriteTime(&writeTime)
		r.SetWriter(&writer)
		r.SetSuccessful(true)
	}
}

func (s *Postgres) Write(ctx context.Context, requests ...data.Request) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		log.Error("Failed to begin transaction: %v", err)
		return
	}
	defer tx.Rollback(ctx)

	for _, r := range requests {
		s.writeWithTx(ctx, tx, r)
	}

	err = tx.Commit(ctx)
	if err != nil {
		log.Error("Failed to commit transaction: %v", err)
	}
}

func (s *Postgres) writeWithTx(ctx context.Context, tx pgx.Tx, r data.Request) {
	indirectField, indirectEntity := s.resolveIndirection(ctx, r.GetFieldName(), r.GetEntityId())
	if indirectField == "" || indirectEntity == "" {
		log.Error("Failed to resolve indirection")
		return
	}

	entity := s.GetEntity(ctx, indirectEntity)
	if entity == nil {
		log.Error("Failed to get entity")
		return
	}

	schema := s.GetFieldSchema(ctx, entity.GetType(), indirectField)
	if schema == nil {
		log.Error("Failed to get field schema")
		return
	}

	tableName := s.getTableForType(schema.GetFieldType())
	if tableName == "" {
		log.Error("Invalid field type")
		return
	}

	// Read existing value for notification
	oldReq := request.New().SetEntityId(r.GetEntityId()).SetFieldName(r.GetFieldName())
	s.Read(ctx, oldReq)

	writeTime := time.Now()
	if r.GetWriteTime() != nil {
		writeTime = *r.GetWriteTime()
	}

	writer := ""
	if r.GetWriter() != nil {
		writer = *r.GetWriter()
	}

	fieldValue := s.convertFromValue(r.GetValue())
	if fieldValue == nil {
		log.Error("Failed to convert value")
		return
	}

	// Upsert the field value
	_, err := tx.Exec(ctx, fmt.Sprintf(`
        INSERT INTO %s (entity_id, field_name, field_value, write_time, writer)
        VALUES ($1, $2, $3, $4, $5)
        ON CONFLICT (entity_id, field_name) 
        DO UPDATE SET field_value = $3, write_time = $4, writer = $5
    `, tableName), indirectEntity, indirectField, fieldValue, writeTime, writer)

	if err != nil {
		log.Error("Failed to write field: %v", err)
		return
	}

	// Handle notifications
	s.triggerNotificationsWithTx(ctx, tx, r, oldReq)
	r.SetSuccessful(true)
}

func (s *Postgres) triggerNotificationsWithTx(ctx context.Context, tx pgx.Tx, r data.Request, o data.Request) {
	notifications := []*protobufs.DatabaseNotification{}

	// Get entity-specific notifications
	rows, err := tx.Query(ctx, `
        SELECT id, context_fields, notify_on_change, service_id
        FROM NotificationConfigEntityId
        WHERE entity_id = $1 AND field_name = $2
    `, r.GetEntityId(), r.GetFieldName())
	if err != nil {
		log.Error("Failed to get entity notifications: %v", err)
		return
	}
	defer rows.Close()

	s.processNotificationRows(ctx, rows, r, o, notifications)

	// Get type-specific notifications
	entity := s.GetEntity(ctx, r.GetEntityId())
	if entity != nil {
		rows, err = tx.Query(ctx, `
            SELECT id, context_fields, notify_on_change, service_id
            FROM NotificationConfigEntityType
            WHERE entity_type = $1 AND field_name = $2
        `, entity.GetType(), r.GetFieldName())
		if err != nil {
			log.Error("Failed to get type notifications: %v", err)
			return
		}
		defer rows.Close()

		s.processNotificationRows(ctx, rows, r, o, notifications)
	}

	// Insert notifications
	for _, n := range notifications {
		b, err := proto.Marshal(n)
		if err != nil {
			log.Error("Failed to marshal notification: %v", err)
			continue
		}

		_, err = tx.Exec(ctx, `
            INSERT INTO Notifications (timestamp, service_id, acknowledged, notification)
            VALUES ($1, $2, false, $3)
        `, time.Now(), n.ServiceId, b)
		if err != nil {
			log.Error("Failed to insert notification: %v", err)
		}
	}
}

func (s *Postgres) processNotificationRows(ctx context.Context, rows pgx.Rows, r data.Request, o data.Request, notifications []*protobufs.DatabaseNotification) {
	for rows.Next() {
		var id int
		var contextFields []string
		var notifyOnChange bool
		var serviceId string

		err := rows.Scan(&id, &contextFields, &notifyOnChange, &serviceId)
		if err != nil {
			log.Error("Failed to scan notification config: %v", err)
			continue
		}

		// Create new notification config
		nc := &protobufs.DatabaseNotificationConfig{
			ServiceId:      serviceId,
			ContextFields:  contextFields,
			NotifyOnChange: notifyOnChange,
		}

		b, err := proto.Marshal(nc)
		if err != nil {
			log.Error("Failed to marshal notification config: %v", err)
			continue
		}
		token := base64.StdEncoding.EncodeToString(b)

		// Create context fields
		context := []*protobufs.DatabaseField{}
		for _, cf := range contextFields {
			cr := request.New().SetEntityId(r.GetEntityId()).SetFieldName(cf)
			s.Read(ctx, cr)
			if cr.IsSuccessful() {
				context = append(context, field.ToFieldPb(field.FromRequest(cr)))
			}
		}

		notifications = append(notifications, &protobufs.DatabaseNotification{
			Token:     token,
			ServiceId: serviceId,
			Current:   field.ToFieldPb(field.FromRequest(r)),
			Previous:  field.ToFieldPb(field.FromRequest(o)),
			Context:   context,
		})
	}
}

// Fix notification processing to avoid lock copying
func (s *Postgres) ProcessNotifications(ctx context.Context) {
	s.transformer.ProcessPending()

	rows, err := s.pool.Query(ctx, `
        UPDATE Notifications 
        SET acknowledged = true 
        WHERE acknowledged = false AND service_id = $1
        RETURNING notification
    `, s.getServiceId())
	if err != nil {
		if !errors.Is(err, pgx.ErrNoRows) {
			log.Error("Failed to process notifications: %v", err)
		}
		return
	}
	defer rows.Close()

	for rows.Next() {
		var notificationBytes []byte
		err := rows.Scan(&notificationBytes)
		if err != nil {
			log.Error("Failed to scan notification: %v", err)
			continue
		}

		// Unmarshal into new notification instance to avoid lock copying
		n := new(protobufs.DatabaseNotification)
		if err := proto.Unmarshal(notificationBytes, n); err != nil {
			log.Error("Failed to unmarshal notification: %v", err)
			continue
		}

		if callbacks, ok := s.callbacks[n.Token]; ok {
			notif := notification.FromPb(n)
			for _, callback := range callbacks {
				callback.Fn(ctx, notif)
			}
		}
	}
}

func (s *Postgres) Notify(ctx context.Context, nc data.NotificationConfig, cb data.NotificationCallback) data.NotificationToken {
	if nc.GetServiceId() == "" {
		nc.SetServiceId(s.getServiceId())
	}

	b, err := proto.Marshal(notification.ToConfigPb(nc))
	if err != nil {
		log.Error("Failed to marshal notification config: %v", err)
		return notification.NewToken("", s, nil)
	}

	token := base64.StdEncoding.EncodeToString(b)

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		log.Error("Failed to begin transaction: %v", err)
		return notification.NewToken("", s, nil)
	}
	defer tx.Rollback(ctx)

	var id int
	if nc.GetEntityId() != "" {
		err = tx.QueryRow(ctx, `
            INSERT INTO NotificationConfigEntityId (entity_id, field_name, context_fields, notify_on_change, service_id)
            VALUES ($1, $2, $3, $4, $5)
            RETURNING id
        `, nc.GetEntityId(), nc.GetFieldName(), nc.GetContextFields(), nc.GetNotifyOnChange(), nc.GetServiceId()).Scan(&id)
	} else {
		err = tx.QueryRow(ctx, `
            INSERT INTO NotificationConfigEntityType (entity_type, field_name, context_fields, notify_on_change, service_id)
            VALUES ($1, $2, $3, $4, $5)
            RETURNING id
        `, nc.GetEntityType(), nc.GetFieldName(), nc.GetContextFields(), nc.GetNotifyOnChange(), nc.GetServiceId()).Scan(&id)
	}

	if err != nil {
		log.Error("Failed to create notification config: %v", err)
		return notification.NewToken("", s, nil)
	}

	s.callbacks[token] = append(s.callbacks[token], cb)

	err = tx.Commit(ctx)
	if err != nil {
		log.Error("Failed to commit notification config: %v", err)
		return notification.NewToken("", s, nil)
	}

	return notification.NewToken(token, s, cb)
}

// Fix DeleteEntity to clean up notifications
func (s *Postgres) DeleteEntity(ctx context.Context, entityId string) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		log.Error("Failed to begin transaction: %v", err)
		return
	}
	defer tx.Rollback(ctx)

	// Get entity first to handle parent/child relationships
	entity := s.GetEntity(ctx, entityId)
	if entity == nil {
		return
	}

	// Remove this entity from parent's children list
	if entity.GetParentId() != "" {
		_, err = tx.Exec(ctx, `
            UPDATE Entities 
            SET children = array_remove(children, $1)
            WHERE id = $2
        `, entityId, entity.GetParentId())
		if err != nil {
			log.Error("Failed to update parent entity: %v", err)
			return
		}
	}

	// Recursively delete children
	for _, childId := range entity.GetChildrenIds() {
		s.DeleteEntity(ctx, childId)
	}

	// Delete all field values
	for _, table := range []string{"Strings", "BinaryFiles", "Ints", "Floats", "Bools",
		"EntityReferences", "Timestamps", "Transformations"} {
		_, err = tx.Exec(ctx, fmt.Sprintf(`
            DELETE FROM %s WHERE entity_id = $1
        `, table), entityId)
		if err != nil {
			log.Error("Failed to delete fields from %s: %v", table, err)
			return
		}
	}

	// Delete notification configs
	_, err = tx.Exec(ctx, `
        DELETE FROM NotificationConfigEntityId WHERE entity_id = $1;
        DELETE FROM Notifications WHERE token IN (
            SELECT base64(notification_pb) FROM NotificationConfigEntityId 
            WHERE entity_id = $1
        );
    `, entityId)
	if err != nil {
		log.Error("Failed to delete notification configs: %v", err)
		return
	}

	// Finally delete the entity itself
	_, err = tx.Exec(ctx, `
        DELETE FROM Entities WHERE id = $1
    `, entityId)
	if err != nil {
		log.Error("Failed to delete entity: %v", err)
		return
	}

	err = tx.Commit(ctx)
	if err != nil {
		log.Error("Failed to commit entity deletion: %v", err)
	}
}

func (s *Postgres) convertFromValue(v data.Value) interface{} {
	if v == nil || v.IsNil() {
		return nil
	}

	switch {
	case v.IsInt():
		return v.GetInt()
	case v.IsFloat():
		return v.GetFloat()
	case v.IsString():
		return v.GetString()
	case v.IsBool():
		return v.GetBool()
	case v.IsBinaryFile():
		return v.GetBinaryFile()
	case v.IsEntityReference():
		return v.GetEntityReference()
	case v.IsTimestamp():
		return v.GetTimestamp().Format(time.RFC3339Nano)
	case v.IsTransformation():
		return v.GetTransformation()
	default:
		return nil
	}
}

func (s *Postgres) convertToValue(fieldType string, value interface{}) data.Value {
	if value == nil {
		return nil
	}

	v := field.NewValue()
	switch fieldType {
	case "protobufs.Int":
		v.SetInt(value)
	case "protobufs.Float":
		v.SetFloat(value)
	case "protobufs.String":
		v.SetString(value)
	case "protobufs.Bool":
		v.SetBool(value)
	case "protobufs.BinaryFile":
		v.SetBinaryFile(value)
	case "protobufs.EntityReference":
		v.SetEntityReference(value)
	case "protobufs.Timestamp":
		if ts, err := time.Parse(time.RFC3339Nano, value.(string)); err == nil {
			v.SetTimestamp(ts)
		}
	case "protobufs.Transformation":
		v.SetTransformation(value)
	default:
		return nil
	}
	return v
}

// Helper methods

func (s *Postgres) getTableForType(fieldType string) string {
	switch fieldType {
	case "protobufs.Int":
		return "Ints"
	case "protobufs.Float":
		return "Floats"
	case "protobufs.String":
		return "Strings"
	case "protobufs.Bool":
		return "Bools"
	case "protobufs.BinaryFile":
		return "BinaryFiles"
	case "protobufs.EntityReference":
		return "EntityReferences"
	case "protobufs.Timestamp":
		return "Timestamps"
	case "protobufs.Transformation":
		return "Transformations"
	default:
		return ""
	}
}

func (s *Postgres) getServiceId() string {
	return app.GetName()
}

func (s *Postgres) resolveIndirection(ctx context.Context, indirectField, entityId string) (string, string) {
	fields := strings.Split(indirectField, "->")
	if len(fields) == 1 {
		return indirectField, entityId
	}

	currentEntityId := entityId
	for _, f := range fields[:len(fields)-1] {
		// Try field reference first
		req := request.New().SetEntityId(currentEntityId).SetFieldName(f)
		s.Read(ctx, req)
		if req.IsSuccessful() {
			v := req.GetValue()
			if v.IsEntityReference() {
				currentEntityId = v.GetEntityReference()
				if currentEntityId == "" {
					return "", ""
				}
				continue
			}
		}

		// Try parent reference
		entity := s.GetEntity(ctx, currentEntityId)
		if entity == nil {
			return "", ""
		}

		parentId := entity.GetParentId()
		if parentId != "" {
			parentEntity := s.GetEntity(ctx, parentId)
			if parentEntity != nil && parentEntity.GetName() == f {
				currentEntityId = parentId
				continue
			}
		}

		// Try child reference
		found := false
		for _, childId := range entity.GetChildrenIds() {
			child := s.GetEntity(ctx, childId)
			if child != nil && child.GetName() == f {
				currentEntityId = childId
				found = true
				break
			}
		}

		if !found {
			return "", ""
		}
	}

	return fields[len(fields)-1], currentEntityId
}

// ... Implement remaining Store interface methods ...

func (s *Postgres) CreateSnapshot(ctx context.Context) data.Snapshot {
	ss := snapshot.New()

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		log.Error("Failed to begin transaction: %v", err)
		return ss
	}
	defer tx.Rollback(ctx)

	// Get all entity types and their schemas
	rows, err := tx.Query(ctx, `
        SELECT DISTINCT entity_type 
        FROM EntitySchema
    `)
	if err != nil {
		log.Error("Failed to get entity types: %v", err)
		return ss
	}
	defer rows.Close()

	for rows.Next() {
		var entityType string
		if err := rows.Scan(&entityType); err != nil {
			log.Error("Failed to scan entity type: %v", err)
			continue
		}

		// Add schema
		schema := s.GetEntitySchema(ctx, entityType)
		if schema != nil {
			ss.AppendSchema(schema)

			// Add entities of this type and their fields
			entities := s.FindEntities(ctx, entityType)
			for _, entityId := range entities {
				entity := s.GetEntity(ctx, entityId)
				if entity != nil {
					ss.AppendEntity(entity)

					// Add fields for this entity
					for _, fieldName := range schema.GetFieldNames() {
						req := request.New().SetEntityId(entityId).SetFieldName(fieldName)
						s.Read(ctx, req)
						if req.IsSuccessful() {
							ss.AppendField(field.FromRequest(req))
						}
					}
				}
			}
		}
	}

	return ss
}

func (s *Postgres) RestoreSnapshot(ctx context.Context, ss data.Snapshot) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		log.Error("Failed to begin transaction: %v", err)
		return
	}
	defer tx.Rollback(ctx)

	// Clear existing data
	_, err = tx.Exec(ctx, `
        TRUNCATE TABLE Entities CASCADE;
        TRUNCATE TABLE EntitySchema CASCADE;
        TRUNCATE TABLE Strings CASCADE;
        TRUNCATE TABLE BinaryFiles CASCADE;
        TRUNCATE TABLE Ints CASCADE;
        TRUNCATE TABLE Floats CASCADE;
        TRUNCATE TABLE Bools CASCADE;
        TRUNCATE TABLE EntityReferences CASCADE;
        TRUNCATE TABLE Timestamps CASCADE;
        TRUNCATE TABLE Transformations CASCADE;
        TRUNCATE TABLE Notifications CASCADE;
        TRUNCATE TABLE NotificationConfigEntityId CASCADE;
        TRUNCATE TABLE NotificationConfigEntityType CASCADE;
    `)
	if err != nil {
		log.Error("Failed to clear existing data: %v", err)
		return
	}

	// Restore schemas
	for _, schema := range ss.GetSchemas() {
		for i, field := range schema.GetFields() {
			_, err := tx.Exec(ctx, `
                INSERT INTO EntitySchema (entity_type, field_name, field_type, rank)
                VALUES ($1, $2, $3, $4)
            `, schema.GetType(), field.GetFieldName(), field.GetFieldType(), i)
			if err != nil {
				log.Error("Failed to restore schema: %v", err)
				continue
			}
		}
	}

	// Restore entities
	for _, e := range ss.GetEntities() {
		_, err := tx.Exec(ctx, `
            INSERT INTO Entities (id, name, parent_id, type, children)
            VALUES ($1, $2, $3, $4, $5)
        `, e.GetId(), e.GetName(), e.GetParentId(), e.GetType(), e.GetChildrenIds())
		if err != nil {
			log.Error("Failed to restore entity: %v", err)
			continue
		}
	}

	// Restore fields
	for _, f := range ss.GetFields() {
		req := request.FromField(f)
		s.writeWithTx(ctx, tx, req)
	}

	err = tx.Commit(ctx)
	if err != nil {
		log.Error("Failed to commit snapshot restoration: %v", err)
		return
	}
}

func (s *Postgres) FindEntities(ctx context.Context, entityType string) []string {
	rows, err := s.pool.Query(ctx, `
        SELECT id FROM Entities WHERE type = $1
    `, entityType)
	if err != nil {
		log.Error("Failed to find entities: %v", err)
		return nil
	}
	defer rows.Close()

	var entities []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			log.Error("Failed to scan entity ID: %v", err)
			continue
		}
		entities = append(entities, id)
	}
	return entities
}

func (s *Postgres) GetEntityTypes(ctx context.Context) []string {
	rows, err := s.pool.Query(ctx, `
        SELECT DISTINCT entity_type FROM EntitySchema
    `)
	if err != nil {
		log.Error("Failed to get entity types: %v", err)
		return nil
	}
	defer rows.Close()

	var types []string
	for rows.Next() {
		var entityType string
		if err := rows.Scan(&entityType); err != nil {
			log.Error("Failed to scan entity type: %v", err)
			continue
		}
		types = append(types, entityType)
	}
	return types
}

func (s *Postgres) GetEntitySchema(ctx context.Context, entityType string) data.EntitySchema {
	rows, err := s.pool.Query(ctx, `
        SELECT field_name, field_type, rank
        FROM EntitySchema
        WHERE entity_type = $1
        ORDER BY rank
    `, entityType)
	if err != nil {
		log.Error("Failed to get entity schema: %v", err)
		return nil
	}
	defer rows.Close()

	schema := entity.FromSchemaPb(&protobufs.DatabaseEntitySchema{})
	schema.SetType(entityType)
	var fields []data.FieldSchema

	for rows.Next() {
		var fieldName, fieldType string
		var rank int
		if err := rows.Scan(&fieldName, &fieldType, &rank); err != nil {
			log.Error("Failed to scan field schema: %v", err)
			continue
		}
		fields = append(fields, field.FromSchemaPb(&protobufs.DatabaseFieldSchema{
			Name: fieldName,
			Type: fieldType,
		}))
	}

	schema.SetFields(fields)
	return schema
}

func (s *Postgres) SetEntitySchema(ctx context.Context, schema data.EntitySchema) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		log.Error("Failed to begin transaction: %v", err)
		return
	}
	defer tx.Rollback(ctx)

	// Get existing schema for comparison
	oldSchema := s.GetEntitySchema(ctx, schema.GetType())

	// Delete existing schema
	_, err = tx.Exec(ctx, `
        DELETE FROM EntitySchema WHERE entity_type = $1
    `, schema.GetType())
	if err != nil {
		log.Error("Failed to delete existing schema: %v", err)
		return
	}

	// Insert new schema
	for i, field := range schema.GetFields() {
		_, err = tx.Exec(ctx, `
            INSERT INTO EntitySchema (entity_type, field_name, field_type, rank)
            VALUES ($1, $2, $3, $4)
        `, schema.GetType(), field.GetFieldName(), field.GetFieldType(), i)
		if err != nil {
			log.Error("Failed to insert field schema: %v", err)
			return
		}
	}

	// Handle field changes for existing entities
	if oldSchema != nil {
		removedFields := []string{}
		newFields := []string{}

		// Find removed fields
		for _, oldField := range oldSchema.GetFields() {
			found := false
			for _, newField := range schema.GetFields() {
				if oldField.GetFieldName() == newField.GetFieldName() {
					found = true
					break
				}
			}
			if !found {
				removedFields = append(removedFields, oldField.GetFieldName())
			}
		}

		// Find new fields
		for _, newField := range schema.GetFields() {
			found := false
			for _, oldField := range oldSchema.GetFields() {
				if newField.GetFieldName() == oldField.GetFieldName() {
					found = true
					break
				}
			}
			if !found {
				newFields = append(newFields, newField.GetFieldName())
			}
		}

		// Update existing entities
		entities := s.FindEntities(ctx, schema.GetType())
		for _, entityId := range entities {
			// Remove deleted fields
			for _, fieldName := range removedFields {
				tableName := s.getTableForType(oldSchema.GetField(fieldName).GetFieldType())
				if tableName == "" {
					continue
				}
				_, err = tx.Exec(ctx, fmt.Sprintf(`
                    DELETE FROM %s 
                    WHERE entity_id = $1 AND field_name = $2
                `, tableName), entityId, fieldName)
				if err != nil {
					log.Error("Failed to delete field: %v", err)
					continue
				}
			}

			// Initialize new fields
			for _, fieldName := range newFields {
				req := request.New().SetEntityId(entityId).SetFieldName(fieldName)
				s.writeWithTx(ctx, tx, req)
			}
		}
	}

	err = tx.Commit(ctx)
	if err != nil {
		log.Error("Failed to commit schema changes: %v", err)
	}
}

func (s *Postgres) EntityExists(ctx context.Context, entityId string) bool {
	var exists bool
	err := s.pool.QueryRow(ctx, `
        SELECT EXISTS(SELECT 1 FROM Entities WHERE id = $1)
    `, entityId).Scan(&exists)
	if err != nil {
		log.Error("Failed to check entity existence: %v", err)
		return false
	}
	return exists
}

func (s *Postgres) FieldExists(ctx context.Context, fieldName, entityType string) bool {
	var exists bool
	err := s.pool.QueryRow(ctx, `
        SELECT EXISTS(
            SELECT 1 FROM EntitySchema 
            WHERE entity_type = $1 AND field_name = $2
        )
    `, entityType, fieldName).Scan(&exists)
	if err != nil {
		log.Error("Failed to check field existence: %v", err)
		return false
	}
	return exists
}

func (s *Postgres) Unnotify(ctx context.Context, token string) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		log.Error("Failed to begin transaction: %v", err)
		return
	}
	defer tx.Rollback(ctx)

	// Delete from both config tables since we don't know which one contains the token
	_, err = tx.Exec(ctx, `
        DELETE FROM NotificationConfigEntityId WHERE id = $1;
        DELETE FROM NotificationConfigEntityType WHERE id = $1;
    `, token)
	if err != nil {
		log.Error("Failed to delete notification config: %v", err)
		return
	}

	delete(s.callbacks, token)

	err = tx.Commit(ctx)
	if err != nil {
		log.Error("Failed to commit notification removal: %v", err)
	}
}

func (s *Postgres) UnnotifyCallback(ctx context.Context, token string, callback data.NotificationCallback) {
	if s.callbacks[token] == nil {
		return
	}

	callbacks := []data.NotificationCallback{}
	for _, cb := range s.callbacks[token] {
		if cb.Id() != callback.Id() {
			callbacks = append(callbacks, cb)
		}
	}

	if len(callbacks) == 0 {
		s.Unnotify(ctx, token)
	} else {
		s.callbacks[token] = callbacks
	}
}

// DB Initialization Queries - these should be run when setting up the database

const createIndexesSQL = `
-- Add indexes for improved query performance
CREATE INDEX IF NOT EXISTS idx_entities_type ON Entities(type);
CREATE INDEX IF NOT EXISTS idx_entities_parent_id ON Entities(parent_id);

-- Add composite primary keys for field tables to ensure uniqueness
ALTER TABLE Strings ADD PRIMARY KEY (entity_id, field_name);
ALTER TABLE BinaryFiles ADD PRIMARY KEY (entity_id, field_name);
ALTER TABLE Ints ADD PRIMARY KEY (entity_id, field_name);
ALTER TABLE Floats ADD PRIMARY KEY (entity_id, field_name);
ALTER TABLE Bools ADD PRIMARY KEY (entity_id, field_name);
ALTER TABLE EntityReferences ADD PRIMARY KEY (entity_id, field_name);
ALTER TABLE Timestamps ADD PRIMARY KEY (entity_id, field_name);
ALTER TABLE Transformations ADD PRIMARY KEY (entity_id, field_name);

-- Add indexes for notification queries
CREATE INDEX IF NOT EXISTS idx_notif_config_entity_field ON NotificationConfigEntityId(entity_id, field_name);
CREATE INDEX IF NOT EXISTS idx_notif_config_type_field ON NotificationConfigEntityType(entity_type, field_name);

-- Add index for unacknowledged notifications
CREATE INDEX IF NOT EXISTS idx_notifications_unack ON Notifications(acknowledged) WHERE NOT acknowledged;
`

func (s *Postgres) initializeDatabase(ctx context.Context) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %v", err)
	}
	defer tx.Rollback(ctx)

	_, err = tx.Exec(ctx, createTablesSQL)
	if err != nil {
		return fmt.Errorf("failed to create tables: %v", err)
	}

	_, err = tx.Exec(ctx, createIndexesSQL)
	if err != nil {
		return fmt.Errorf("failed to create indexes: %v", err)
	}

	return tx.Commit(ctx)
}

func (s *Postgres) GetFieldSchema(ctx context.Context, entityType, fieldName string) data.FieldSchema {
	row := s.pool.QueryRow(ctx, `
        SELECT field_name, field_type
        FROM EntitySchema
        WHERE entity_type = $1 AND field_name = $2
    `, entityType, fieldName)

	var schema struct {
		fieldName string
		fieldType string
	}

	err := row.Scan(&schema.fieldName, &schema.fieldType)
	if err != nil {
		if !errors.Is(err, pgx.ErrNoRows) {
			log.Error("Failed to get field schema: %v", err)
		}
		return nil
	}

	return field.FromSchemaPb(&protobufs.DatabaseFieldSchema{
		Name: schema.fieldName,
		Type: schema.fieldType,
	})
}

func (s *Postgres) SetFieldSchema(ctx context.Context, entityType, fieldName string, schema data.FieldSchema) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		log.Error("Failed to begin transaction: %v", err)
		return
	}
	defer tx.Rollback(ctx)

	// Get current schema to handle field type changes
	oldSchema := s.GetFieldSchema(ctx, entityType, fieldName)

	// Update or insert the field schema
	_, err = tx.Exec(ctx, `
        INSERT INTO EntitySchema (entity_type, field_name, field_type, rank)
        VALUES ($1, $2, $3, 0)
        ON CONFLICT (entity_type, field_name) 
        DO UPDATE SET field_type = $3
    `, entityType, fieldName, schema.GetFieldType())

	if err != nil {
		log.Error("Failed to set field schema: %v", err)
		return
	}

	// If field type changed, migrate existing data
	if oldSchema != nil && oldSchema.GetFieldType() != schema.GetFieldType() {
		oldTable := s.getTableForType(oldSchema.GetFieldType())
		newTable := s.getTableForType(schema.GetFieldType())

		if oldTable != "" && newTable != "" {
			// Delete old field values
			_, err = tx.Exec(ctx, fmt.Sprintf(`
                DELETE FROM %s 
                WHERE entity_id IN (
                    SELECT id FROM Entities WHERE type = $1
                ) AND field_name = $2
            `, oldTable), entityType, fieldName)

			if err != nil {
				log.Error("Failed to delete old field values: %v", err)
				return
			}
		}

		// Initialize new field values for all entities of this type
		entities := s.FindEntities(ctx, entityType)
		for _, entityId := range entities {
			req := request.New().SetEntityId(entityId).SetFieldName(fieldName)
			s.writeWithTx(ctx, tx, req)
		}
	}

	err = tx.Commit(ctx)
	if err != nil {
		log.Error("Failed to commit field schema changes: %v", err)
	}
}
