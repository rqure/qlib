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
    type TEXT NOT NULL
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
    writer TEXT NOT NULL,
    PRIMARY KEY (entity_id, field_name)
);

CREATE TABLE IF NOT EXISTS BinaryFiles (
    entity_id TEXT NOT NULL,
    field_name TEXT NOT NULL,
    field_value TEXT,
    write_time TIMESTAMP NOT NULL,
    writer TEXT NOT NULL,
    PRIMARY KEY (entity_id, field_name)
);

CREATE TABLE IF NOT EXISTS Ints (
    entity_id TEXT NOT NULL,
    field_name TEXT NOT NULL,
    field_value BIGINT,
    write_time TIMESTAMP NOT NULL,
    writer TEXT NOT NULL,
    PRIMARY KEY (entity_id, field_name)
);

CREATE TABLE IF NOT EXISTS Floats (
    entity_id TEXT NOT NULL,
    field_name TEXT NOT NULL,
    field_value DOUBLE PRECISION,
    write_time TIMESTAMP NOT NULL,
    writer TEXT NOT NULL,
    PRIMARY KEY (entity_id, field_name)
);

CREATE TABLE IF NOT EXISTS Bools (
    entity_id TEXT NOT NULL,
    field_name TEXT NOT NULL,
    field_value BOOLEAN,
    write_time TIMESTAMP NOT NULL,
    writer TEXT NOT NULL,
    PRIMARY KEY (entity_id, field_name)
);

CREATE TABLE IF NOT EXISTS EntityReferences (
    entity_id TEXT NOT NULL,
    field_name TEXT NOT NULL,
    field_value TEXT,
    write_time TIMESTAMP NOT NULL,
    writer TEXT NOT NULL,
    PRIMARY KEY (entity_id, field_name)
);

CREATE TABLE IF NOT EXISTS Timestamps (
    entity_id TEXT NOT NULL,
    field_name TEXT NOT NULL,
    field_value TIMESTAMP,
    write_time TIMESTAMP NOT NULL,
    writer TEXT NOT NULL,
    PRIMARY KEY (entity_id, field_name)
);

CREATE TABLE IF NOT EXISTS Transformations (
    entity_id TEXT NOT NULL,
    field_name TEXT NOT NULL,
    field_value TEXT,
    write_time TIMESTAMP NOT NULL,
    writer TEXT NOT NULL,
    PRIMARY KEY (entity_id, field_name)
);

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
    notification BYTEA NOT NULL
);
`

const (
	NotificationExpiryDuration = time.Minute
)

type PostgresConfig struct {
	ConnectionString string
}

type Postgres struct {
	pool        *pgxpool.Pool
	tx          pgx.Tx
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

func (s *Postgres) withTx(ctx context.Context, fn func(context.Context, pgx.Tx)) {
	if s.tx == nil {
		tx, err := s.pool.Begin(ctx)
		if err != nil {
			log.Error("Failed to begin transaction: %v", err)
			return
		}
		defer tx.Rollback(ctx)

		s.tx = tx
		fn(ctx, tx)
		s.tx = nil

		err = tx.Commit(ctx)
		if err != nil {
			log.Error("Failed to commit transaction: %v", err)
		}
	} else {
		fn(ctx, s.tx)
	}
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
	var e data.Entity

	s.withTx(ctx, func(ctx context.Context, tx pgx.Tx) {
		// First get the entity's basic info
		row := tx.QueryRow(ctx, `
		SELECT id, name, parent_id, type
		FROM Entities
		WHERE id = $1
		`, entityId)

		var name, parentId, entityType string
		err := row.Scan(&entityId, &name, &parentId, &entityType)
		if err != nil {
			if !errors.Is(err, pgx.ErrNoRows) {
				log.Error("Failed to get entity: %v", err)
			}
			return
		}

		// Then get children using parent_id relationship
		rows, err := tx.Query(ctx, `
		SELECT id
		FROM Entities
		WHERE parent_id = $1
		`, entityId)
		if err != nil {
			log.Error("Failed to get children: %v", err)
			return
		}
		defer rows.Close()

		var children []string
		for rows.Next() {
			var childId string
			if err := rows.Scan(&childId); err != nil {
				log.Error("Failed to scan child id: %v", err)
				continue
			}
			children = append(children, childId)
		}

		de := &protobufs.DatabaseEntity{
			Id:   entityId,
			Name: name,
			Parent: &protobufs.EntityReference{
				Raw: parentId,
			},
			Type: entityType,
		}

		de.Children = make([]*protobufs.EntityReference, len(children))
		for i, child := range children {
			de.Children[i] = &protobufs.EntityReference{Raw: child}
		}

		e = entity.FromEntityPb(de)
	})

	return e
}

func (s *Postgres) SetEntity(ctx context.Context, e data.Entity) {
	s.withTx(ctx, func(ctx context.Context, tx pgx.Tx) {
		// Update entity or create if it doesn't exist
		_, err := tx.Exec(ctx, `
			INSERT INTO Entities (id, name, parent_id, type)
			VALUES ($1, $2, $3, $4)
			ON CONFLICT (id) DO UPDATE
			SET name = $2, parent_id = $3, type = $4
		`, e.GetId(), e.GetName(), e.GetParentId(), e.GetType())

		if err != nil {
			log.Error("Failed to update entity: %v", err)
			return
		}
	})
}

func (s *Postgres) CreateEntity(ctx context.Context, entityType, parentId, name string) {
	s.withTx(ctx, func(ctx context.Context, tx pgx.Tx) {
		entityId := uuid.New().String()
		_, err := tx.Exec(ctx, `
			INSERT INTO Entities (id, name, parent_id, type)
			VALUES ($1, $2, $3, $4)
		`, entityId, name, parentId, entityType)

		if err != nil {
			log.Error("Failed to create entity: %v", err)
			return
		}

		// Initialize fields with default values
		schema := s.GetEntitySchema(ctx, entityType)
		if schema != nil {
			for _, f := range schema.GetFields() {
				req := request.New().SetEntityId(entityId).SetFieldName(f.GetFieldName())
				s.Write(ctx, req)
			}
		}
	})
}

func (s *Postgres) Read(ctx context.Context, requests ...data.Request) {
	s.withTx(ctx, func(ctx context.Context, tx pgx.Tx) {
		for _, r := range requests {
			r.SetSuccessful(false)

			indirectField, indirectEntity := s.resolveIndirection(ctx, r.GetFieldName(), r.GetEntityId())
			if indirectField == "" || indirectEntity == "" {
				return
			}

			entity := s.GetEntity(ctx, indirectEntity)
			if entity == nil {
				return
			}

			schema := s.GetFieldSchema(ctx, entity.GetType(), indirectField)
			if schema == nil {
				return
			}

			tableName := s.getTableForType(schema.GetFieldType())
			if tableName == "" {
				return
			}

			row := tx.QueryRow(ctx, fmt.Sprintf(`
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
				return
			}

			value := s.convertToValue(schema.GetFieldType(), fieldValue)
			if value == nil {
				return
			}

			r.SetValue(value)
			r.SetWriteTime(&writeTime)
			r.SetWriter(&writer)
			r.SetSuccessful(true)
		}
	})
}

func (s *Postgres) Write(ctx context.Context, requests ...data.Request) {
	s.withTx(ctx, func(ctx context.Context, tx pgx.Tx) {
		for _, r := range requests {
			indirectField, indirectEntity := s.resolveIndirection(ctx, r.GetFieldName(), r.GetEntityId())
			if indirectField == "" || indirectEntity == "" {
				log.Error("Failed to resolve indirection")
				return
			}

			// Get entity and schema info in a single query
			var entityType string
			schema := &protobufs.DatabaseFieldSchema{}
			err := tx.QueryRow(ctx, `
				WITH entity_type AS (
					SELECT type FROM Entities WHERE id = $1
				)
				SELECT 
					entity_type.type,
					EntitySchema.field_name,
					EntitySchema.field_type
				FROM entity_type
				LEFT JOIN EntitySchema ON 
					EntitySchema.entity_type = entity_type.type
					AND EntitySchema.field_name = $2
			`, indirectEntity, indirectField).Scan(&entityType, &schema.Name, &schema.Type)

			if err != nil {
				log.Error("Failed to get entity and schema info: %v", err)
				return
			}

			tableName := s.getTableForType(schema.Type)
			if tableName == "" {
				log.Error("Invalid field type")
				return
			}

			// Read existing value for notification in same transaction
			var oldValue interface{}
			var oldWriteTime time.Time
			var oldWriter string
			_ = tx.QueryRow(ctx, fmt.Sprintf(`
				SELECT field_value, write_time, writer
				FROM %s
				WHERE entity_id = $1 AND field_name = $2
			`, tableName), indirectEntity, indirectField).Scan(&oldValue, &oldWriteTime, &oldWriter)

			oldReq := request.New().
				SetEntityId(r.GetEntityId()).
				SetFieldName(r.GetFieldName()).
				SetValue(s.convertToValue(schema.Type, oldValue)).
				SetWriteTime(&oldWriteTime).
				SetWriter(&oldWriter)

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
			_, err = tx.Exec(ctx, fmt.Sprintf(`
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
			s.triggerNotificationsWithTx(ctx, r, oldReq)
			r.SetSuccessful(true)
		}
	})
}

func (s *Postgres) triggerNotificationsWithTx(ctx context.Context, r data.Request, o data.Request) {
	notifications := []*protobufs.DatabaseNotification{}

	s.withTx(ctx, func(ctx context.Context, tx pgx.Tx) {
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

		notifications = append(notifications, s.processNotificationRows(ctx, rows, r, o)...)
	})

	s.withTx(ctx, func(ctx context.Context, tx pgx.Tx) {
		entity := s.GetEntity(ctx, r.GetEntityId())
		if entity == nil {
			log.Error("Failed to get entity")
			return
		}

		rows, err := tx.Query(ctx, `
			SELECT id, context_fields, notify_on_change, service_id
			FROM NotificationConfigEntityType
			WHERE entity_type = $1 AND field_name = $2
		`, entity.GetType(), r.GetFieldName())
		if err != nil {
			log.Error("Failed to get type notifications: %v", err)
			return
		}
		defer rows.Close()

		notifications = append(notifications, s.processNotificationRows(ctx, rows, r, o)...)
	})

	s.withTx(ctx, func(ctx context.Context, tx pgx.Tx) {
		for _, n := range notifications {
			notifBytes, err := proto.Marshal(n)
			if err != nil {
				log.Error("Failed to marshal notification: %v", err)
				continue
			}

			_, err = tx.Exec(ctx, `
				INSERT INTO Notifications (timestamp, service_id, notification)
				VALUES ($1, $2, $3)
			`, time.Now(), n.ServiceId, notifBytes)
			if err != nil {
				log.Error("Failed to insert notification: %v", err)
			}
		}
	})
}

func (s *Postgres) processNotificationRows(ctx context.Context, rows pgx.Rows, r data.Request, o data.Request) []*protobufs.DatabaseNotification {
	notifications := []*protobufs.DatabaseNotification{}

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

	return notifications
}

// Fix notification processing to avoid lock copying
func (s *Postgres) ProcessNotifications(ctx context.Context) {
	s.transformer.ProcessPending()

	s.withTx(ctx, func(ctx context.Context, tx pgx.Tx) {
		expireTime := time.Now().Add(-NotificationExpiryDuration)
		_, err := tx.Exec(ctx, `
			DELETE FROM Notifications
			WHERE timestamp < $1
		`, expireTime)
		if err != nil {
			log.Error("Failed to delete expired notifications: %v", err)
			return
		}

		// Select and delete notifications in one transaction to prevent duplicates
		rows, err := tx.Query(ctx, `
			DELETE FROM Notifications 
			WHERE service_id = $1 
			AND timestamp > $2
			RETURNING notification
		`, s.getServiceId(), expireTime)

		if err != nil {
			if !errors.Is(err, pgx.ErrNoRows) {
				log.Error("Failed to process notifications: %v", err)
			}
			return
		}
		defer rows.Close()

		for rows.Next() {
			var notifBytes []byte
			err := rows.Scan(&notifBytes)
			if err != nil {
				log.Error("Failed to scan notification: %v", err)
				continue
			}

			n := &protobufs.DatabaseNotification{}
			if err := proto.Unmarshal(notifBytes, n); err != nil {
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
	})
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

	var n data.NotificationToken
	s.withTx(ctx, func(ctx context.Context, tx pgx.Tx) {
		if nc.GetEntityId() != "" {
			_, err := tx.Exec(ctx, `
				INSERT INTO NotificationConfigEntityId (entity_id, field_name, context_fields, notify_on_change, service_id)
				VALUES ($1, $2, $3, $4, $5)
			`, nc.GetEntityId(), nc.GetFieldName(), nc.GetContextFields(), nc.GetNotifyOnChange(), nc.GetServiceId())

			if err != nil {
				log.Error("Failed to create notification config: %v", err)
				return
			}
		} else {
			_, err := tx.Exec(ctx, `
				INSERT INTO NotificationConfigEntityType (entity_type, field_name, context_fields, notify_on_change, service_id)
				VALUES ($1, $2, $3, $4, $5)
			`, nc.GetEntityType(), nc.GetFieldName(), nc.GetContextFields(), nc.GetNotifyOnChange(), nc.GetServiceId())

			if err != nil {
				log.Error("Failed to create notification config: %v", err)
				return
			}
		}

		s.callbacks[token] = append(s.callbacks[token], cb)
		n = notification.NewToken(token, s, cb)
	})

	if n == nil {
		n = notification.NewToken("", s, nil)
	}

	return n
}

// Fix DeleteEntity to clean up notifications
func (s *Postgres) DeleteEntity(ctx context.Context, entityId string) {
	s.withTx(ctx, func(ctx context.Context, tx pgx.Tx) {
		// Get children using parent_id relationship
		rows, err := tx.Query(ctx, `
			SELECT id FROM Entities WHERE parent_id = $1
		`, entityId)
		if err != nil {
			log.Error("Failed to get children: %v", err)
			return
		}
		defer rows.Close()

		// Recursively delete children
		for rows.Next() {
			var childId string
			if err := rows.Scan(&childId); err != nil {
				log.Error("Failed to scan child id: %v", err)
				continue
			}
			s.DeleteEntity(ctx, childId)
		}

		// Delete all field values
		for _, table := range []string{"Strings", "BinaryFiles", "Ints", "Floats", "Bools",
			"EntityReferences", "Timestamps", "Transformations"} {
			_, err := tx.Exec(ctx, fmt.Sprintf(`
				DELETE FROM %s WHERE entity_id = $1
			`, table), entityId)
			if err != nil {
				log.Error("Failed to delete fields from %s: %v", table, err)
				return
			}
		}

		// Delete notification configs and their notifications
		_, err = tx.Exec(ctx, `
			WITH deleted_configs AS (
				DELETE FROM NotificationConfigEntityId 
				WHERE entity_id = $1
				RETURNING service_id, 
						encode(
							notification::bytea, 
							'base64'
						) as token
			)
			DELETE FROM Notifications 
			WHERE service_id IN (SELECT service_id FROM deleted_configs)
			AND notification::text = ANY(SELECT token FROM deleted_configs);
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
	})
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
		switch t := value.(type) {
		case time.Time:
			v.SetTimestamp(t)
		default:
			log.Error("Invalid timestamp type: %T", value)
			return nil
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
		// Get field reference and entity info in a single query
		var fieldValue, entityName, parentId string
		var childIds []string

		var err error
		s.withTx(ctx, func(ctx context.Context, tx pgx.Tx) {
			err = tx.QueryRow(ctx, `
				WITH field_ref AS (
					SELECT field_value 
					FROM EntityReferences 
					WHERE entity_id = $1 AND field_name = $2
				),
				entity_info AS (
					SELECT name, parent_id, children
					FROM Entities
					WHERE id = $1
				)
				SELECT field_ref.field_value, entity_info.name, 
					entity_info.parent_id, entity_info.children
				FROM field_ref
				FULL OUTER JOIN entity_info ON TRUE
			`, currentEntityId, f).Scan(&fieldValue, &entityName, &parentId, &childIds)
		})

		if err != nil && !errors.Is(err, pgx.ErrNoRows) {
			log.Error("Failed to resolve indirection: %v", err)
			return "", ""
		}

		// Try field reference first
		if fieldValue != "" {
			currentEntityId = fieldValue
			continue
		}

		// Try parent reference
		if parentId != "" {
			var parentName string

			s.withTx(ctx, func(ctx context.Context, tx pgx.Tx) {
				err = tx.QueryRow(ctx, `
					SELECT name FROM Entities WHERE id = $1
				`, parentId).Scan(&parentName)
			})

			if err == nil && parentName == f {
				currentEntityId = parentId
				continue
			}
		}

		// Try child reference
		found := false
		for _, childId := range childIds {
			var childName string

			s.withTx(ctx, func(ctx context.Context, tx pgx.Tx) {
				err = tx.QueryRow(ctx, `
					SELECT name FROM Entities WHERE id = $1
				`, childId).Scan(&childName)
			})

			if err == nil && childName == f {
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

func (s *Postgres) CreateSnapshot(ctx context.Context) data.Snapshot {
	ss := snapshot.New()

	s.withTx(ctx, func(ctx context.Context, tx pgx.Tx) {
		// Get all entity types and their schemas
		rows, err := tx.Query(ctx, `
			SELECT DISTINCT entity_type 
			FROM EntitySchema
		`)
		if err != nil {
			log.Error("Failed to get entity types: %v", err)
			return
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

	})

	return ss
}

func (s *Postgres) RestoreSnapshot(ctx context.Context, ss data.Snapshot) {
	s.withTx(ctx, func(ctx context.Context, tx pgx.Tx) {
		// Remove existing tables
		_, err := tx.Exec(ctx, `
			DROP TABLE IF EXISTS
				Entities, EntitySchema, Strings,
				BinaryFiles, Ints, Floats, Bools, EntityReferences,
				Timestamps, Transformations, NotificationConfigEntityId,
				NotificationConfigEntityType, Notifications
			CASCADE
		`)

		if err != nil {
			log.Error("Failed to clear existing data: %v", err)
			return
		}

		// Recreate tables and indexes
		if err := s.initializeDatabase(ctx); err != nil {
			log.Error("Failed to initialize database: %v", err)
			return
		}

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
			INSERT INTO Entities (id, name, parent_id, type)
			VALUES ($1, $2, $3, $4)
		`, e.GetId(), e.GetName(), e.GetParentId(), e.GetType())
			if err != nil {
				log.Error("Failed to restore entity: %v", err)
				continue
			}
		}

		// Restore fields
		for _, f := range ss.GetFields() {
			req := request.FromField(f)
			s.Write(ctx, req)
		}
	})
}

func (s *Postgres) FindEntities(ctx context.Context, entityType string) []string {
	processRows := func(rows pgx.Rows) []string {
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

	entities := []string{}
	s.withTx(ctx, func(ctx context.Context, tx pgx.Tx) {
		rows, err := tx.Query(ctx, `
		SELECT id FROM Entities WHERE type = $1
	`, entityType)
		if err != nil {
			log.Error("Failed to find entities: %v", err)
			return
		}
		defer rows.Close()

		entities = processRows(rows)
	})

	return entities
}

func (s *Postgres) GetEntityTypes(ctx context.Context) []string {
	processRows := func(rows pgx.Rows) []string {
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

	types := []string{}
	s.withTx(ctx, func(ctx context.Context, tx pgx.Tx) {
		rows, err := tx.Query(ctx, `
		SELECT DISTINCT entity_type FROM EntitySchema
	`)
		if err != nil {
			log.Error("Failed to get entity types: %v", err)
			return
		}
		defer rows.Close()
		types = processRows(rows)
	})

	return types
}

func (s *Postgres) GetEntitySchema(ctx context.Context, entityType string) data.EntitySchema {
	processRows := func(rows pgx.Rows) data.EntitySchema {
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

	var schema data.EntitySchema
	s.withTx(ctx, func(ctx context.Context, tx pgx.Tx) {
		rows, err := tx.Query(ctx, `
			SELECT field_name, field_type, rank
			FROM EntitySchema
			WHERE entity_type = $1
			ORDER BY rank
		`, entityType)
		if err != nil {
			log.Error("Failed to get entity schema: %v", err)
			return
		}
		defer rows.Close()
		schema = processRows(rows)
	})

	return schema
}

func (s *Postgres) SetEntitySchema(ctx context.Context, schema data.EntitySchema) {
	s.withTx(ctx, func(ctx context.Context, tx pgx.Tx) {
		// Get existing schema for comparison
		oldSchema := s.GetEntitySchema(ctx, schema.GetType())

		// Delete existing schema
		_, err := tx.Exec(ctx, `
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
					s.Write(ctx, req)
				}
			}
		}
	})
}

func (s *Postgres) EntityExists(ctx context.Context, entityId string) bool {
	exists := false

	s.withTx(ctx, func(ctx context.Context, tx pgx.Tx) {
		err := tx.QueryRow(ctx, `
			SELECT EXISTS(SELECT 1 FROM Entities WHERE id = $1)
		`, entityId).Scan(&exists)

		if err != nil {
			log.Error("Failed to check entity existence: %v", err)
		}
	})

	return exists
}

func (s *Postgres) FieldExists(ctx context.Context, fieldName, entityType string) bool {
	exists := false

	s.withTx(ctx, func(ctx context.Context, tx pgx.Tx) {
		err := tx.QueryRow(ctx, `
			SELECT EXISTS(
				SELECT 1 FROM EntitySchema 
				WHERE entity_type = $1 AND field_name = $2
			)
		`, entityType, fieldName).Scan(&exists)
		if err != nil {
			log.Error("Failed to check field existence: %v", err)
		}
	})

	return exists
}

func (s *Postgres) Unnotify(ctx context.Context, token string) {
	// Decode the token to get the notification config
	b, err := base64.StdEncoding.DecodeString(token)
	if err != nil {
		log.Error("Failed to decode token: %v", err)
		return
	}

	nc := &protobufs.DatabaseNotificationConfig{}
	if err := proto.Unmarshal(b, nc); err != nil {
		log.Error("Failed to unmarshal notification config: %v", err)
		return
	}

	s.withTx(ctx, func(ctx context.Context, tx pgx.Tx) {
		// Delete based on service_id and other matching fields
		if nc.Id != "" {
			_, err = tx.Exec(ctx, `
			DELETE FROM NotificationConfigEntityId 
			WHERE service_id = $1 
			AND entity_id = $2 
			AND field_name = $3
			`, nc.ServiceId, nc.Id, nc.Field)
		} else {
			_, err = tx.Exec(ctx, `
			DELETE FROM NotificationConfigEntityType 
			WHERE service_id = $1 
			AND entity_type = $2 
			AND field_name = $3
			`, nc.ServiceId, nc.Type, nc.Field)
		}

		if err != nil {
			log.Error("Failed to delete notification config: %v", err)
			return
		}

		delete(s.callbacks, token)
	})
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

-- Add indexes for notification queries
CREATE INDEX IF NOT EXISTS idx_notif_config_entity_field ON NotificationConfigEntityId(entity_id, field_name);
CREATE INDEX IF NOT EXISTS idx_notif_config_type_field ON NotificationConfigEntityType(entity_type, field_name);

-- Remove old index for unacknowledged notifications and add simpler index
CREATE INDEX IF NOT EXISTS idx_notifications_service ON Notifications(service_id);

-- Add index for notification timestamps
CREATE INDEX IF NOT EXISTS idx_notifications_timestamp ON Notifications(timestamp);
`

func (s *Postgres) initializeDatabase(ctx context.Context) error {
	var err error
	s.withTx(ctx, func(ctx context.Context, tx pgx.Tx) {
		_, err = tx.Exec(ctx, createTablesSQL)
		if err != nil {
			err = fmt.Errorf("failed to create tables: %v", err)
			return
		}

		_, err = tx.Exec(ctx, createIndexesSQL)
		if err != nil {
			err = fmt.Errorf("failed to create indexes: %v", err)
			return
		}
	})
	return err
}

func (s *Postgres) GetFieldSchema(ctx context.Context, entityType, fieldName string) data.FieldSchema {
	schema := &protobufs.DatabaseFieldSchema{}
	s.withTx(ctx, func(ctx context.Context, tx pgx.Tx) {
		err := tx.QueryRow(ctx, `
			SELECT field_name, field_type
			FROM EntitySchema
			WHERE entity_type = $1 AND field_name = $2
		`, entityType, fieldName).Scan(&schema.Name, &schema.Type)
		if err != nil {
			if !errors.Is(err, pgx.ErrNoRows) {
				log.Error("Failed to get field schema: %v", err)
			}
			schema = nil
		}
	})

	if schema == nil {
		return nil
	}

	return field.FromSchemaPb(schema)
}

func (s *Postgres) SetFieldSchema(ctx context.Context, entityType, fieldName string, schema data.FieldSchema) {
	s.withTx(ctx, func(ctx context.Context, tx pgx.Tx) {
		// Get current schema to handle field type changes
		oldSchema := s.GetFieldSchema(ctx, entityType, fieldName)

		// Update or insert the field schema
		_, err := tx.Exec(ctx, `
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
				s.Write(ctx, req)
			}
		}
	})
}
