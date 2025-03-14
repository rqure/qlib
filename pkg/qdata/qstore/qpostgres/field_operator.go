package qpostgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/rqure/qlib/pkg/qapp"
	"github.com/rqure/qlib/pkg/qdata"
	"github.com/rqure/qlib/pkg/qdata/qfield"
	"github.com/rqure/qlib/pkg/qdata/qquery"
	"github.com/rqure/qlib/pkg/qdata/qrequest"
	"github.com/rqure/qlib/pkg/qlog"
	"github.com/rqure/qlib/pkg/qprotobufs"
	"google.golang.org/protobuf/proto"
)

type FieldOperator struct {
	core                  Core
	schemaManager         qdata.SchemaManager
	entityManager         qdata.EntityManager
	notificationPublisher qdata.NotificationPublisher
	clientId              *string
}

func NewFieldOperator(core Core) qdata.ModifiableFieldOperator {
	return &FieldOperator{
		core: core,
	}
}

func (me *FieldOperator) SetSchemaManager(schemaManager qdata.SchemaManager) {
	me.schemaManager = schemaManager
}

func (me *FieldOperator) SetEntityManager(entityManager qdata.EntityManager) {
	me.entityManager = entityManager
}

func (me *FieldOperator) SetNotificationPublisher(publisher qdata.NotificationPublisher) {
	me.notificationPublisher = publisher
}

func (me *FieldOperator) Read(ctx context.Context, requests ...qdata.Request) {
	ir := qquery.NewIndirectionResolver(me.entityManager, me)

	me.core.WithTx(ctx, func(ctx context.Context, tx pgx.Tx) {
		for _, req := range requests {
			req.SetSuccessful(false)

			indirectEntity, indirectField := ir.Resolve(ctx, req.GetEntityId(), req.GetFieldName())
			if indirectField == "" || indirectEntity == "" {
				qlog.Error("Failed to resolve indirection for: %s->%s", req.GetEntityId(), req.GetFieldName())
				continue
			}

			entity := me.entityManager.GetEntity(ctx, indirectEntity)
			if entity == nil {
				qlog.Error("Failed to get entity: %s", indirectEntity)
				continue
			}

			schema := me.schemaManager.GetFieldSchema(ctx, entity.GetType(), indirectField)
			if schema == nil {
				qlog.Error("Failed to get field schema: %s->%s", entity.GetType(), indirectField)
				continue
			}

			tableName := getTableForType(schema.GetFieldType())
			if tableName == "" {
				qlog.Error("Invalid field type %s for field %s->%s", schema.GetFieldType(), entity.GetType(), indirectField)
				continue
			}

			if authorizer, ok := ctx.Value(qdata.FieldAuthorizerKey).(qdata.FieldAuthorizer); ok {
				if authorizer != nil && !authorizer.IsAuthorized(ctx, indirectEntity, indirectField, false) {
					qlog.Error("%s is not authorized to read from field: %s->%s", authorizer.AccessorId(), req.GetEntityId(), req.GetFieldName())
					continue
				}
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
					qlog.Error("Failed to read field: %v", err)
				}

				continue
			}

			value := convertToValue(schema.GetFieldType(), fieldValue)
			if value == nil {
				qlog.Error("Failed to convert value for field %s->%s", entity.GetType(), indirectField)
				continue
			}

			req.SetValue(value)
			req.SetWriteTime(&writeTime)
			req.SetWriter(&writer)
			req.SetSuccessful(true)
		}
	})
}

func (me *FieldOperator) Write(ctx context.Context, requests ...qdata.Request) {
	ir := qquery.NewIndirectionResolver(me.entityManager, me)

	me.core.WithTx(ctx, func(ctx context.Context, tx pgx.Tx) {
		for _, req := range requests {
			indirectEntity, indirectField := ir.Resolve(ctx, req.GetEntityId(), req.GetFieldName())
			if indirectField == "" || indirectEntity == "" {
				qlog.Error("Failed to resolve indirection for: %s->%s", req.GetEntityId(), req.GetFieldName())
				continue
			}

			// Get entity and schema info in a single query
			var entityType string
			schema := &qprotobufs.DatabaseFieldSchema{}
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
				qlog.Error("Failed to get entity and schema info: %v", err)
				continue
			}

			tableName := getTableForType(schema.Type)
			if tableName == "" {
				qlog.Error("Invalid field type %s for field %s->%s", schema.Type, entityType, indirectField)
				continue
			}

			if req.GetValue().IsNil() {
				anyPbField := fieldTypeToProtoType(schema.Type)
				req.SetValue(qfield.FromAnyPb(&anyPbField))
			}

			oldReq := qrequest.New().SetEntityId(req.GetEntityId()).SetFieldName(req.GetFieldName())
			me.Read(ctx, oldReq)

			if oldReq.IsSuccessful() && req.GetWriteOpt() == qdata.WriteChanges {
				if proto.Equal(qfield.ToAnyPb(oldReq.GetValue()), qfield.ToAnyPb(req.GetValue())) {
					req.SetSuccessful(true)
					continue
				}
			}

			fieldValue := fieldValueToInterface(req.GetValue())
			if fieldValue == nil {
				qlog.Error("Failed to convert value for field %s->%s", entityType, indirectField)
				continue
			}

			if req.GetWriteTime() == nil {
				wt := time.Now()
				req.SetWriteTime(&wt)
			}

			if req.GetWriter() == nil {
				wr := ""

				if me.clientId == nil && qapp.GetName() != "" {
					clients := qquery.New(&qdata.LimitedStore{
						FieldOperator:         me,
						EntityManager:         me.entityManager,
						NotificationPublisher: me.notificationPublisher,
						SchemaManager:         me.schemaManager,
					}).Select().
						From("Client").
						Where("Name").Equals(qapp.GetName()).
						Execute(ctx)

					if len(clients) == 0 {
						qlog.Error("Failed to get client id")
					} else {
						if len(clients) > 1 {
							qlog.Warn("Multiple clients found: %v", clients)
						}

						clientId := clients[0].GetId()
						me.clientId = &clientId
					}
				}

				if me.clientId != nil {
					wr = *me.clientId
				}

				req.SetWriter(&wr)
			}

			if authorizer, ok := ctx.Value(qdata.FieldAuthorizerKey).(qdata.FieldAuthorizer); ok {
				if authorizer != nil && !authorizer.IsAuthorized(ctx, indirectEntity, indirectField, true) {
					qlog.Error("%s is not authorized to write to field: %s->%s", authorizer.AccessorId(), req.GetEntityId(), req.GetFieldName())
					continue
				}
			}

			if oldReq.IsSuccessful() && (oldReq.GetValue().IsEntityReference() || oldReq.GetValue().IsEntityList()) {
				var oldReferences []string
				if oldReq.GetValue().IsEntityReference() {
					oldRef := oldReq.GetValue().GetEntityReference()
					if oldRef != "" {
						oldReferences = []string{oldRef}
					}
				} else if oldReq.GetValue().IsEntityList() {
					oldReferences = oldReq.GetValue().GetEntityList().GetEntities()
				}

				// Delete old references
				for _, oldRef := range oldReferences {
					_, err = tx.Exec(ctx, `
                            DELETE FROM ReverseEntityReferences 
                            WHERE referenced_entity_id = $1 
                            AND referenced_by_entity_id = $2
                            AND referenced_by_field_name = $3
                        `, oldRef, indirectEntity, indirectField)

					if err != nil {
						qlog.Error("Failed to delete old reverse entity reference: %v", err)
					}
				}
			}

			if req.GetValue().IsEntityReference() || req.GetValue().IsEntityList() {
				var newReferences []string
				if req.GetValue().IsEntityReference() {
					newRef := req.GetValue().GetEntityReference()
					if newRef != "" && me.entityManager.EntityExists(ctx, newRef) {
						newReferences = []string{newRef}
						req.GetValue().SetEntityReference(newRef)
					} else {
						req.GetValue().SetEntityReference("")
					}
				} else if req.GetValue().IsEntityList() {
					for _, newRef := range req.GetValue().GetEntityList().GetEntities() {
						if newRef != "" && me.entityManager.EntityExists(ctx, newRef) {
							newReferences = append(newReferences, newRef)
						}
					}
					req.GetValue().SetEntityList(newReferences)
				}

				// Insert new references
				for _, newRef := range newReferences {
					_, err = tx.Exec(ctx, `
                        INSERT INTO ReverseEntityReferences 
                        (referenced_entity_id, referenced_by_entity_id, referenced_by_field_name)
                        VALUES ($1, $2, $3)
                        ON CONFLICT (referenced_entity_id, referenced_by_entity_id, referenced_by_field_name) 
                        DO NOTHING
                    `, newRef, indirectEntity, indirectField)

					if err != nil {
						qlog.Error("Failed to insert reverse entity reference: %v", err)
					}
				}
			}

			// Upsert the field value
			_, err = tx.Exec(ctx, fmt.Sprintf(`
				INSERT INTO %s (entity_id, field_name, field_value, write_time, writer)
				VALUES ($1, $2, $3, $4, $5)
				ON CONFLICT (entity_id, field_name) 
				DO UPDATE SET field_value = $3, write_time = $4, writer = $5
			`, tableName), indirectEntity, indirectField, fieldValue, *req.GetWriteTime(), *req.GetWriter())

			if err != nil {
				qlog.Error("Failed to write field: %v", err)
				continue
			}

			// Handle notifications
			if me.notificationPublisher != nil {
				me.notificationPublisher.PublishNotifications(ctx, req, oldReq)
			}
			req.SetSuccessful(true)
		}
	})
}
