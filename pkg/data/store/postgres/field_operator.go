package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/rqure/qlib/pkg/data"
	"github.com/rqure/qlib/pkg/data/field"
	"github.com/rqure/qlib/pkg/data/query"
	"github.com/rqure/qlib/pkg/data/request"
	"github.com/rqure/qlib/pkg/log"
	"github.com/rqure/qlib/pkg/protobufs"
	"google.golang.org/protobuf/proto"
)

type FieldOperator struct {
	core                  Core
	schemaManager         data.SchemaManager
	entityManager         data.EntityManager
	notificationPublisher data.NotificationPublisher
	transformer           data.Transformer
}

func NewFieldOperator(core Core) data.ModifiableFieldOperator {
	return &FieldOperator{
		core: core,
	}
}

func (me *FieldOperator) SetSchemaManager(schemaManager data.SchemaManager) {
	me.schemaManager = schemaManager
}

func (me *FieldOperator) SetEntityManager(entityManager data.EntityManager) {
	me.entityManager = entityManager
}

func (me *FieldOperator) SetNotificationPublisher(publisher data.NotificationPublisher) {
	me.notificationPublisher = publisher
}

func (me *FieldOperator) SetTransformer(transformer data.Transformer) {
	me.transformer = transformer
}

func (me *FieldOperator) AuthorizedRead(ctx context.Context, authorizer data.FieldAuthorizer, requests ...data.Request) {
	ir := query.NewIndirectionResolver(me.entityManager, me)

	me.core.WithTx(ctx, func(ctx context.Context, tx pgx.Tx) {
		for _, req := range requests {
			req.SetSuccessful(false)

			indirectField, indirectEntity := ir.Resolve(ctx, req.GetEntityId(), req.GetFieldName())
			if indirectField == "" || indirectEntity == "" {
				log.Error("Failed to resolve indirection for: %s->%s", req.GetEntityId(), req.GetFieldName())
				continue
			}

			entity := me.entityManager.GetEntity(ctx, indirectEntity)
			if entity == nil {
				log.Error("Failed to get entity: %s", indirectEntity)
				continue
			}

			schema := me.schemaManager.GetFieldSchema(ctx, entity.GetType(), indirectField)
			if schema == nil {
				log.Error("Failed to get field schema: %s->%s", entity.GetType(), indirectField)
				continue
			}

			tableName := getTableForType(schema.GetFieldType())
			if tableName == "" {
				log.Error("Invalid field type %s for field %s->%s", schema.GetFieldType(), entity.GetType(), indirectField)
				continue
			}

			if authorizer != nil && !authorizer.IsAuthorized(ctx, indirectEntity, indirectField) {
				log.Error("%s is not authorized to read from field: %s->%s", authorizer.AccessorId(), req.GetEntityId(), req.GetFieldName())
				continue
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

				continue
			}

			value := convertToValue(schema.GetFieldType(), fieldValue)
			if value == nil {
				log.Error("Failed to convert value for field %s->%s", entity.GetType(), indirectField)
				continue
			}

			req.SetValue(value)
			req.SetWriteTime(&writeTime)
			req.SetWriter(&writer)
			req.SetSuccessful(true)
		}
	})
}

func (me *FieldOperator) AuthorizedWrite(ctx context.Context, authorizer data.FieldAuthorizer, requests ...data.Request) {
	ir := query.NewIndirectionResolver(me.entityManager, me)

	me.core.WithTx(ctx, func(ctx context.Context, tx pgx.Tx) {
		for _, req := range requests {
			indirectField, indirectEntity := ir.Resolve(ctx, req.GetEntityId(), req.GetFieldName())
			if indirectField == "" || indirectEntity == "" {
				log.Error("Failed to resolve indirection for: %s->%s", req.GetEntityId(), req.GetFieldName())
				continue
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
				continue
			}

			tableName := getTableForType(schema.Type)
			if tableName == "" {
				log.Error("Invalid field type %s for field %s->%s", schema.Type, entityType, indirectField)
				continue
			}

			if req.GetValue().IsNil() {
				req.SetValue(field.FromAnyPb(fieldTypeToProtoType(schema.Type)))
			} else {
				v := field.FromAnyPb(fieldTypeToProtoType(schema.Type))
				if req.GetValue().GetType() != v.GetType() && !v.IsTransformation() {
					log.Warn("Field type mismatch for %s.%s. Got: %v, Expected: %v. Writing default value instead.", req.GetEntityId(), req.GetFieldName(), req.GetValue().GetType(), v.GetType())
					req.SetValue(v)
				}
			}

			oldReq := request.New().SetEntityId(req.GetEntityId()).SetFieldName(req.GetFieldName())
			me.Read(ctx, oldReq)

			// Set the value in the database
			// Note that for a transformation, we don't actually write the value to the database
			// unless the new value is a transformation. This is because the transformation is
			// executed by the transformer, which will write the result to the database.
			if oldReq.IsSuccessful() && oldReq.GetValue().IsTransformation() && !req.GetValue().IsTransformation() {
				src := oldReq.GetValue().GetTransformation()
				me.transformer.Transform(ctx, src, req)
				req.SetValue(oldReq.GetValue())
			} else if oldReq.IsSuccessful() && req.GetWriteOpt() == data.WriteChanges {
				if proto.Equal(field.ToAnyPb(oldReq.GetValue()), field.ToAnyPb(req.GetValue())) {
					req.SetSuccessful(true)
					continue
				}
			}

			fieldValue := fieldValueToInterface(req.GetValue())
			if fieldValue == nil {
				log.Error("Failed to convert value for field %s->%s", entityType, indirectField)
				continue
			}

			if req.GetWriteTime() == nil {
				wt := time.Now()
				req.SetWriteTime(&wt)
			}

			if req.GetWriter() == nil {
				wr := ""
				req.SetWriter(&wr)
			}

			if authorizer != nil && !authorizer.IsAuthorized(ctx, indirectEntity, indirectField) {
				log.Error("%s is not authorized to write to field: %s->%s", authorizer.AccessorId(), req.GetEntityId(), req.GetFieldName())
				continue
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
						log.Error("Failed to delete old reverse entity reference: %v", err)
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
					req.GetValue().GetEntityList().SetEntities(newReferences)
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
						log.Error("Failed to insert reverse entity reference: %v", err)
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
				log.Error("Failed to write field: %v", err)
				continue
			}

			// Handle notifications
			me.notificationPublisher.PublishNotifications(ctx, req, oldReq)
			req.SetSuccessful(true)
		}
	})
}

func (me *FieldOperator) Read(ctx context.Context, requests ...data.Request) {
	me.AuthorizedRead(ctx, nil, requests...)
}

func (me *FieldOperator) Write(ctx context.Context, requests ...data.Request) {
	me.AuthorizedWrite(ctx, nil, requests...)
}
