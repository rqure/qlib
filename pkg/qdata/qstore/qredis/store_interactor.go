package qredis

import (
	"context"
	"fmt"
	"strings"

	"github.com/redis/go-redis/v9"
	"github.com/rqure/qlib/pkg/qdata"
	"github.com/rqure/qlib/pkg/qss"
)

type refInfo struct {
	entityId   qdata.EntityId
	entityType qdata.EntityType
	fieldType  qdata.FieldType
}

func (me *refInfo) AsString() string {
	return fmt.Sprintf("%s:%s:%s", me.entityId.AsString(), me.entityType.AsString(), me.fieldType.AsString())
}

func (me *refInfo) FromString(s string) error {
	parts := strings.Split(s, ":")
	if len(parts) == 3 {
		me.entityId = qdata.EntityId(parts[0])
		me.entityType = qdata.EntityType(parts[1])
		me.fieldType = qdata.FieldType(parts[2])
		return nil
	}
	return fmt.Errorf("invalid refInfo format")
}

// RedisStoreInteractor implements Redis-specific caching mechanisms
type RedisStoreInteractor struct {
	core         RedisCore
	keyBuilder   *KeyBuilder
	publisherSig qss.Signal[qdata.PublishNotificationArgs]
}

// NewStoreInteractor creates a new Redis store interactor
func NewStoreInteractor(core RedisCore) qdata.StoreInteractor {
	return &RedisStoreInteractor{
		core:         core,
		keyBuilder:   NewKeyBuilder("qos"),
		publisherSig: qss.New[qdata.PublishNotificationArgs](),
	}
}

func (me *RedisStoreInteractor) CreateEntity(ctx context.Context, entityType qdata.EntityType, parentId qdata.EntityId, name string) (*qdata.Entity, error) {
	entityId := qdata.GenerateEntityId(entityType)

	for {
		exists, err := me.EntityExists(ctx, entityId)
		if err != nil {
			return nil, err
		}

		if !exists {
			break
		}

		entityId = qdata.GenerateEntityId(entityType)
	}

	entity := new(qdata.Entity).Init(entityId, qdata.EOEntityType(entityType))
	schema, err := me.GetEntitySchema(ctx, entityType)
	if err != nil {
		return nil, err
	}

	err = me.core.WithClient(ctx, func(ctx context.Context, client *redis.Client) error {
		result := client.ZAdd(
			ctx,
			me.keyBuilder.GetAllEntitiesKey(entityType),
			redis.Z{
				Score:  float64(0),
				Member: entityId.AsString(),
			},
		)

		if result.Err() != nil {
			return result.Err()
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	reqs := make([]*qdata.Request, 0)
	for _, field := range schema.Fields {
		req := new(qdata.Request).Init(entityId, field.FieldType)

		if field.FieldType == qdata.FTName {
			req.Value.FromString(name)
		} else if field.FieldType == qdata.FTParent {
			req.Value.FromEntityReference(parentId)
		}

		reqs = append(reqs, req)
	}

	err = me.Write(ctx, reqs...)
	if err != nil {
		return nil, err
	}

	if parentId != "" {
		req := new(qdata.Request).Init(parentId, qdata.FTChildren)
		err = me.Read(ctx, req)
		if err == nil {
			children := req.Value.GetEntityList()
			children = append(children, entityId)
			req.Value.FromEntityList(children)
			me.Write(ctx, req)
		} else {
			return nil, fmt.Errorf("failed to read parent entity: %v", err)
		}
	}

	return entity, nil
}

func (me *RedisStoreInteractor) DeleteEntity(ctx context.Context, entityId qdata.EntityId) error {
	err := me.core.WithClient(ctx, func(ctx context.Context, client *redis.Client) error {
		result := client.ZRem(
			ctx,
			me.keyBuilder.GetAllEntitiesKey(entityId.GetEntityType()),
			entityId.AsString(),
		)

		if result.Err() != nil {
			return result.Err()
		}

		return nil
	})

	if err != nil {
		return err
	}

	errs := make([]error, 0)

	req := new(qdata.Request).Init(entityId, qdata.FTChildren)
	err = me.Read(ctx, req)
	errs = append(errs, err)

	if err == nil {
		children := req.Value.GetEntityList()
		for _, childId := range children {
			err = me.DeleteEntity(ctx, childId)
			errs = append(errs, err)
		}
	}

	err = me.core.WithClient(ctx, func(ctx context.Context, client *redis.Client) error {
		result := client.Del(ctx, me.keyBuilder.GetEntityKey(entityId))
		if result.Err() != nil {
			return result.Err()
		}
		return nil
	})
	errs = append(errs, err)

	refs, err := me.getAllReferences(ctx, entityId)
	errs = append(errs, err)
	if err != nil {
		for _, ref := range refs {
			err = me.clearReferences(ctx, entityId, ref)
			errs = append(errs, err)
		}
	}

	err = qdata.AccumulateErrors(errs...)
	if err != nil {
		return err
	}

	return nil
}

func (me *RedisStoreInteractor) PrepareQuery(sql string, args ...any) (*qdata.PageResult[qdata.QueryRow], error) {
	// Implement query preparation logic here
	return nil, nil
}

func (me *RedisStoreInteractor) FindEntities(entityType qdata.EntityType, pageOpts ...qdata.PageOpts) (*qdata.PageResult[qdata.EntityId], error) {
	pageConfig := qdata.DefaultPageConfig().ApplyOpts(pageOpts...)

	if pageConfig.PageSize <= 0 {
		pageConfig.PageSize = 100
	}

	return &qdata.PageResult[qdata.EntityId]{
		Items:    make([]qdata.EntityId, 0),
		CursorId: pageConfig.CursorId,
		NextPage: func(ctx context.Context) (*qdata.PageResult[qdata.EntityId], error) {
			if pageConfig.CursorId < 0 {
				return &qdata.PageResult[qdata.EntityId]{
					Items:    make([]qdata.EntityId, 0),
					CursorId: -1,
					NextPage: nil,
				}, nil
			}

			entities := make([]qdata.EntityId, 0)
			var nextCursorId int64 = -1

			err := me.core.WithClient(ctx, func(ctx context.Context, client *redis.Client) error {
				result := client.ZRange(ctx, me.keyBuilder.GetAllEntitiesKey(entityType), pageConfig.CursorId, pageConfig.CursorId+pageConfig.PageSize-1)
				if result.Err() != nil {
					return result.Err()
				}

				if result.Val() != nil {
					for _, entityId := range result.Val() {
						entities = append(entities, qdata.EntityId(entityId))
					}
				}

				if len(result.Val()) < int(pageConfig.PageSize) {
					nextCursorId = -1
				} else {
					nextCursorId = pageConfig.CursorId + pageConfig.PageSize
				}

				return nil
			})

			if err != nil {
				return nil, err
			}

			return &qdata.PageResult[qdata.EntityId]{
				Items:    entities,
				CursorId: nextCursorId,
				NextPage: func(ctx context.Context) (*qdata.PageResult[qdata.EntityId], error) {
					next, err := me.FindEntities(entityType, qdata.POCursorId(nextCursorId), qdata.POPageSize(pageConfig.PageSize))
					if err != nil {
						return nil, err
					}
					return next.NextPage(ctx)
				},
			}, nil
		},
	}, nil
}

func (me *RedisStoreInteractor) GetEntityTypes(pageOpts ...qdata.PageOpts) (*qdata.PageResult[qdata.EntityType], error) {
	pageConfig := qdata.DefaultPageConfig().ApplyOpts(pageOpts...)

	if pageConfig.PageSize <= 0 {
		pageConfig.PageSize = 100
	}

	return &qdata.PageResult[qdata.EntityType]{
		Items:    make([]qdata.EntityType, 0),
		CursorId: pageConfig.CursorId,
		NextPage: func(ctx context.Context) (*qdata.PageResult[qdata.EntityType], error) {
			if pageConfig.CursorId < 0 {
				return &qdata.PageResult[qdata.EntityType]{
					Items:    make([]qdata.EntityType, 0),
					CursorId: -1,
					NextPage: nil,
				}, nil
			}

			entityTypes := make([]qdata.EntityType, 0)
			var nextCursorId int64 = -1

			err := me.core.WithClient(ctx, func(ctx context.Context, client *redis.Client) error {
				result := client.ZRange(ctx, me.keyBuilder.GetAllEntityTypesKey(), pageConfig.CursorId, pageConfig.CursorId+pageConfig.PageSize-1)
				if result.Err() != nil {
					return result.Err()
				}

				if result.Val() != nil {
					for _, entityType := range result.Val() {
						entityTypes = append(entityTypes, qdata.EntityType(entityType))
					}
				}

				if len(result.Val()) < int(pageConfig.PageSize) {
					nextCursorId = -1
				} else {
					nextCursorId = pageConfig.CursorId + pageConfig.PageSize
				}

				return nil
			})

			if err != nil {
				return nil, err
			}

			return &qdata.PageResult[qdata.EntityType]{
				Items:    entityTypes,
				CursorId: nextCursorId,
				NextPage: func(ctx context.Context) (*qdata.PageResult[qdata.EntityType], error) {
					next, err := me.GetEntityTypes(qdata.POCursorId(nextCursorId), qdata.POPageSize(pageConfig.PageSize))
					if err != nil {
						return nil, err
					}
					return next.NextPage(ctx)
				},
			}, nil
		},
	}, nil
}

func (me *RedisStoreInteractor) EntityExists(ctx context.Context, entityId qdata.EntityId) (bool, error) {
	exists := true
	err := me.core.WithClient(ctx, func(ctx context.Context, client *redis.Client) error {
		result := client.ZScore(ctx, me.keyBuilder.GetAllEntitiesKey(entityId.GetEntityType()), entityId.AsString())
		if result.Err() != nil && result.Err() != redis.Nil {
			exists = false
			return result.Err()
		}

		if result.Err() == redis.Nil {
			exists = false
		}

		return nil
	})

	if err != nil {
		return false, err
	}

	return exists, nil
}

func (me *RedisStoreInteractor) FieldExists(ctx context.Context, entityType qdata.EntityType, fieldType qdata.FieldType) (bool, error) {
	schema, err := me.GetEntitySchema(ctx, entityType)
	if err != nil {
		return false, err
	}

	_, ok := schema.Fields[fieldType]
	return ok, nil
}

func (me *RedisStoreInteractor) GetEntitySchema(ctx context.Context, entityType qdata.EntityType) (*qdata.EntitySchema, error) {
	schema := new(qdata.EntitySchema).Init(entityType)

	err := me.core.WithClient(ctx, func(ctx context.Context, client *redis.Client) error {
		result := client.Get(ctx, me.keyBuilder.GetSchemaKey(entityType))
		if result.Err() != nil {
			return result.Err()
		}

		b, err := result.Bytes()
		if err != nil {
			return err
		}

		schema.FromBytes(b)

		return nil
	})

	if err != nil {
		if err == redis.Nil {
			return nil, fmt.Errorf("schema not found for entity type %s", entityType)
		}
		return nil, err
	}

	return schema, nil
}

func (me *RedisStoreInteractor) SetEntitySchema(ctx context.Context, schema *qdata.EntitySchema) error {
	// Ensure the default fields are set
	schema.Field(qdata.FTName, qdata.FSOValueType(qdata.VTString), qdata.FSORank(0))
	schema.Field(qdata.FTDescription, qdata.FSOValueType(qdata.VTString), qdata.FSORank(1))
	schema.Field(qdata.FTParent, qdata.FSOValueType(qdata.VTEntityReference), qdata.FSORank(2))
	schema.Field(qdata.FTChildren, qdata.FSOValueType(qdata.VTEntityList), qdata.FSORank(3))

	oldSchema, oldSchemaErr := me.GetEntitySchema(ctx, schema.EntityType)
	removedFields := make(qdata.FieldTypeSlice, 0)
	newFields := make(qdata.FieldTypeSlice, 0)

	err := me.core.WithClient(ctx, func(ctx context.Context, client *redis.Client) error {
		zAddResult := client.ZAdd(ctx, me.keyBuilder.GetAllEntityTypesKey(), redis.Z{
			Score:  float64(0),
			Member: schema.EntityType.AsString(),
		})

		if zAddResult.Err() != nil {
			return zAddResult.Err()
		}

		setResult := client.Set(ctx, me.keyBuilder.GetSchemaKey(schema.EntityType), schema.AsBytes(), 0)

		if setResult.Err() != nil {
			return setResult.Err()
		}

		return nil
	})

	if err != nil {
		return err
	}

	errs := make([]error, 0)

	if oldSchemaErr == nil {
		for _, oldField := range oldSchema.Fields {
			if _, ok := schema.Fields[oldField.FieldType]; !ok {
				removedFields = append(removedFields, oldField.FieldType)
			}
		}

		for _, newField := range schema.Fields {
			if _, ok := oldSchema.Fields[newField.FieldType]; !ok {
				newFields = append(newFields, newField.FieldType)
			}
		}

		iter, err := me.FindEntities(schema.EntityType)
		if err != nil {
			return err
		}

		iter.ForEach(ctx, func(entityId qdata.EntityId) bool {
			err := me.core.WithClient(ctx, func(ctx context.Context, client *redis.Client) error {
				result := client.HDel(ctx, me.keyBuilder.GetEntityKey(entityId), removedFields.AsStringSlice()...)
				if result.Err() != nil {
					return result.Err()
				}
				return nil
			})

			errs = append(errs, fmt.Errorf("failed to remove fields (%+v) from entity %s: %w", removedFields, entityId.AsString(), err))

			reqs := make([]*qdata.Request, 0)
			for _, newField := range newFields {
				req := new(qdata.Request).Init(entityId, newField)
				vt := schema.Fields[newField].ValueType
				req.Value.FromValue(vt.NewValue())
				reqs = append(reqs, req)
			}
			err = me.Write(ctx, reqs...)

			errs = append(errs, fmt.Errorf("failed to add fields (%+v) to entity %s: %w", newFields, entityId.AsString(), err))

			return true
		})
	}

	err = qdata.AccumulateErrors(errs...)
	if err != nil {
		return err
	}

	return nil
}

func (me *RedisStoreInteractor) GetFieldSchema(ctx context.Context, entityType qdata.EntityType, fieldType qdata.FieldType) (*qdata.FieldSchema, error) {
	schema, err := me.GetEntitySchema(ctx, entityType)
	if err != nil {
		return nil, err
	}

	fieldSchema, ok := schema.Fields[fieldType]
	if !ok {
		return nil, fmt.Errorf("field %s not found in entity type %s", fieldType, entityType)
	}

	return fieldSchema, nil
}

func (me *RedisStoreInteractor) SetFieldSchema(ctx context.Context, entityType qdata.EntityType, fieldType qdata.FieldType, schema *qdata.FieldSchema) error {
	entitySchema, err := me.GetEntitySchema(ctx, entityType)
	if err != nil {
		return err
	}

	entitySchema.Fields[fieldType] = schema

	err = me.SetEntitySchema(ctx, entitySchema)
	if err != nil {
		return err
	}

	return nil
}

func (me *RedisStoreInteractor) PublishNotifications() qss.Signal[qdata.PublishNotificationArgs] {
	return me.publisherSig
}

func (me *RedisStoreInteractor) Read(ctx context.Context, req ...*qdata.Request) error {
	// Implement read logic here
}

func (me *RedisStoreInteractor) Write(ctx context.Context, reqs ...*qdata.Request) error {
	// Implement write logic here
}

func (me *RedisStoreInteractor) InitializeSchema(ctx context.Context) error {
	// Implement schema initialization logic here
	return nil
}

func (me *RedisStoreInteractor) CreateSnapshot(ctx context.Context) (*qdata.Snapshot, error) {
	// Implement snapshot creation logic here
	return nil, nil
}

func (me *RedisStoreInteractor) RestoreSnapshot(ctx context.Context, ss *qdata.Snapshot) error {
	// Implement snapshot restoration logic here
	return nil
}

func (me *RedisStoreInteractor) clearReferences(ctx context.Context, referencedEntityId qdata.EntityId, referencedBy *refInfo) error {
	err := me.core.WithClient(ctx, func(ctx context.Context, client *redis.Client) error {
		if referencedBy.fieldType != "" {
			return fmt.Errorf("field type is not set")
		}

		if referencedBy.entityType != "" && referencedBy.entityId != "" {
			return fmt.Errorf("entity type and entity ID are not set")
		}

		if referencedBy.entityId != "" {
			result := client.HDel(ctx, me.keyBuilder.GetReverseReferenceFieldKey(referencedEntityId), referencedBy.AsString())
			if result.Err() != nil {
				return result.Err()
			}
		}

		if referencedBy.entityType != "" {
			result := client.HDel(ctx, me.keyBuilder.GetReverseReferenceSchemaKey(referencedEntityId.AsString()), referencedBy.AsString())
			if result.Err() != nil {
				return result.Err()
			}
		}

		return nil
	})

	if err != nil {
		return fmt.Errorf("failed to clear references: %w", err)
	}

	return nil
}

func (me *RedisStoreInteractor) setReferences(ctx context.Context, referencedEntityId qdata.EntityId, referencedBy *refInfo) error {
	err := me.core.WithClient(ctx, func(ctx context.Context, client *redis.Client) error {
		if referencedBy.fieldType != "" {
			return fmt.Errorf("field type is not set")
		}

		if referencedBy.entityType != "" && referencedBy.entityId != "" {
			return fmt.Errorf("entity type and entity ID are not set")
		}

		if referencedBy.entityId != "" {
			result := client.HSet(ctx, me.keyBuilder.GetReverseReferenceFieldKey(referencedEntityId), referencedBy.AsString(), "")
			if result.Err() != nil {
				return result.Err()
			}
		}

		if referencedBy.entityType != "" {
			result := client.HSet(ctx, me.keyBuilder.GetReverseReferenceSchemaKey(referencedEntityId.AsString()), referencedBy.AsString(), "")
			if result.Err() != nil {
				return result.Err()
			}
		}

		return nil
	})

	if err != nil {
		return fmt.Errorf("failed to set references: %w", err)
	}

	return nil
}

func (me *RedisStoreInteractor) getAllReferences(ctx context.Context, entityId qdata.EntityId) ([]*refInfo, error) {
	var references []*refInfo

	err := me.core.WithClient(ctx, func(ctx context.Context, client *redis.Client) error {
		errs := make([]error, 0)

		result := client.HGetAll(ctx, me.keyBuilder.GetReverseReferenceFieldKey(entityId))
		if result.Err() != nil {
			return result.Err()
		}

		for ref, _ := range result.Val() {
			refInfo := new(refInfo)
			err := refInfo.FromString(ref)
			if err != nil {
				errs = append(errs, err)
				continue
			}
			references = append(references, refInfo)
		}

		return qdata.AccumulateErrors(errs...)
	})

	if err != nil {
		return nil, fmt.Errorf("failed to get all references: %w", err)
	}

	return references, nil
}
