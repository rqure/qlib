package postgres

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/rqure/qlib/pkg/data"
	"github.com/rqure/qlib/pkg/data/field"
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

func (me *FieldOperator) Read(ctx context.Context, requests ...data.Request) {
	me.core.WithTx(ctx, func(ctx context.Context, tx pgx.Tx) {
		for _, r := range requests {
			r.SetSuccessful(false)

			indirectField, indirectEntity := me.resolveIndirection(ctx, r.GetFieldName(), r.GetEntityId())
			if indirectField == "" || indirectEntity == "" {
				log.Error("Failed to resolve indirection for: %s->%s", r.GetEntityId(), r.GetFieldName())
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

			r.SetValue(value)
			r.SetWriteTime(&writeTime)
			r.SetWriter(&writer)
			r.SetSuccessful(true)
		}
	})
}

func (me *FieldOperator) Write(ctx context.Context, requests ...data.Request) {
	me.core.WithTx(ctx, func(ctx context.Context, tx pgx.Tx) {
		for _, r := range requests {
			indirectField, indirectEntity := me.resolveIndirection(ctx, r.GetFieldName(), r.GetEntityId())
			if indirectField == "" || indirectEntity == "" {
				log.Error("Failed to resolve indirection for: %s->%s", r.GetEntityId(), r.GetFieldName())
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

			if r.GetValue().IsNil() {
				r.SetValue(field.FromAnyPb(fieldTypeToProtoType(schema.Type)))
			} else {
				v := field.FromAnyPb(fieldTypeToProtoType(schema.Type))
				if r.GetValue().GetType() != v.GetType() && !v.IsTransformation() {
					log.Warn("Field type mismatch for %s.%s. Got: %v, Expected: %v. Writing default value instead.", r.GetEntityId(), r.GetFieldName(), r.GetValue().GetType(), v.GetType())
					r.SetValue(v)
				}
			}

			oldReq := request.New().SetEntityId(r.GetEntityId()).SetFieldName(r.GetFieldName())
			me.Read(ctx, oldReq)

			// Set the value in the database
			// Note that for a transformation, we don't actually write the value to the database
			// unless the new value is a transformation. This is because the transformation is
			// executed by the transformer, which will write the result to the database.
			if oldReq.IsSuccessful() && oldReq.GetValue().IsTransformation() && !r.GetValue().IsTransformation() {
				src := oldReq.GetValue().GetTransformation()
				me.transformer.Transform(ctx, src, r)
				r.SetValue(oldReq.GetValue())
			} else if oldReq.IsSuccessful() && r.GetWriteOpt() == data.WriteChanges {
				if proto.Equal(field.ToAnyPb(oldReq.GetValue()), field.ToAnyPb(r.GetValue())) {
					r.SetSuccessful(true)
					continue
				}
			}

			fieldValue := fieldValueToInterface(r.GetValue())
			if fieldValue == nil {
				log.Error("Failed to convert value for field %s->%s", entityType, indirectField)
				continue
			}

			if r.GetWriteTime() == nil {
				wt := time.Now()
				r.SetWriteTime(&wt)
			}

			if r.GetWriter() == nil {
				wr := ""
				r.SetWriter(&wr)
			}

			// Upsert the field value
			_, err = tx.Exec(ctx, fmt.Sprintf(`
				INSERT INTO %s (entity_id, field_name, field_value, write_time, writer)
				VALUES ($1, $2, $3, $4, $5)
				ON CONFLICT (entity_id, field_name) 
				DO UPDATE SET field_value = $3, write_time = $4, writer = $5
			`, tableName), indirectEntity, indirectField, fieldValue, *r.GetWriteTime(), *r.GetWriter())

			if err != nil {
				log.Error("Failed to write field: %v", err)
				continue
			}

			// Handle notifications
			me.notificationPublisher.PublishNotifications(ctx, r, oldReq)
			r.SetSuccessful(true)
		}
	})
}

func (me *FieldOperator) resolveIndirection(ctx context.Context, indirectField, entityId string) (string, string) {
	fields := strings.Split(indirectField, "->")

	if len(fields) == 1 {
		return indirectField, entityId
	}

	for _, f := range fields[:len(fields)-1] {
		r := request.New().SetEntityId(entityId).SetFieldName(f)

		me.Read(ctx, r)

		if r.IsSuccessful() {
			v := r.GetValue()
			if v.IsEntityReference() {
				entityId = v.GetEntityReference()

				if entityId == "" {
					log.Error("Failed to resolve entity reference: %v", r)
					return "", ""
				}

				continue
			}

			log.Error("Field is not an entity reference: %v", r)
			return "", ""
		}

		// Fallback to parent entity reference by name
		entity := me.entityManager.GetEntity(ctx, entityId)
		if entity == nil {
			log.Error("Failed to get entity: %v", entityId)
			return "", ""
		}

		parentId := entity.GetParentId()
		if parentId != "" {
			parentEntity := me.entityManager.GetEntity(ctx, parentId)

			if parentEntity != nil && parentEntity.GetName() == f {
				entityId = parentId
				continue
			}
		}

		// Fallback to child entity reference by name
		foundChild := false
		for _, childId := range entity.GetChildrenIds() {
			childEntity := me.entityManager.GetEntity(ctx, childId)
			if childEntity == nil {
				log.Error("Failed to get child entity: %v", childId)
				continue
			}

			if childEntity.GetName() == f {
				entityId = childId
				foundChild = true
				break
			}
		}

		if !foundChild {
			log.Error("Failed to find child entity: %v", f)
			return "", ""
		}
	}

	return fields[len(fields)-1], entityId
}
