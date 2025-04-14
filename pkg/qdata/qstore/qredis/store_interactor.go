package qredis

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/rqure/qlib/pkg/qcontext"
	"github.com/rqure/qlib/pkg/qdata"
	"github.com/rqure/qlib/pkg/qss"
)

// RedisStoreInteractor implements Redis-specific caching mechanisms
type RedisStoreInteractor struct {
	core         RedisCore
	keyBuilder   *KeyBuilder
	publisherSig qss.Signal[qdata.PublishNotificationArgs]
	clientId     *qdata.EntityId
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

		b, err := schema.AsBytes()
		if err != nil {
			return err
		}

		setResult := client.Set(ctx, me.keyBuilder.GetSchemaKey(schema.EntityType), b, 0)

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

func (me *RedisStoreInteractor) Read(ctx context.Context, reqs ...*qdata.Request) error {
	ir := qdata.NewIndirectionResolver(me)

	errs := make([]error, 0)

	for _, req := range reqs {
		req.Success = false
		req.Err = ""

		indirectEntity, indirectField, err := ir.Resolve(ctx, req.EntityId, req.FieldType)
		if err != nil {
			req.Err = err.Error()
			errs = append(errs, err)
			continue
		}

		entity := new(qdata.Entity).Init(indirectEntity)
		if err != nil {
			req.Err = fmt.Sprintf("schema not found for field %s in entity type %s: %v", indirectField, entity.EntityType, err)
			errs = append(errs, fmt.Errorf(req.Err))
			continue
		}

		if authorizer, ok := qcontext.GetAuthorizer(ctx); ok {
			if !authorizer.CanRead(ctx, new(qdata.Field).Init(indirectEntity, indirectField)) {
				req.Err = fmt.Sprintf("permission denied for field %s in entity type %s", indirectField, entity.EntityType)
				errs = append(errs, fmt.Errorf(req.Err))
				continue
			}
		}

		// Now do the actual reading
		err = me.core.WithClient(ctx, func(ctx context.Context, client *redis.Client) error {
			result := client.HGet(ctx, me.keyBuilder.GetEntityKey(entity.EntityId), indirectField.AsString())
			if result.Err() != nil {
				return result.Err()
			}

			b, err := result.Bytes()
			if err != nil {
				return err
			}

			field, err := new(qdata.Field).FromBytes(b)
			if err != nil {
				return err
			}

			req.Value.FromValue(field.Value)
			req.WriteTime.FromTime(field.WriteTime.AsTime())
			req.WriterId.FromString(field.WriterId.AsString())
			return nil
		})

		if err != nil {
			req.Err = fmt.Sprintf("failed to read field %s in entity type %s: %v", indirectField, entity.EntityType, err)
			errs = append(errs, fmt.Errorf(req.Err))
			continue
		}

		req.Success = true
	}

	return qdata.AccumulateErrors(errs...)
}

func (me *RedisStoreInteractor) Write(ctx context.Context, reqs ...*qdata.Request) error {
	ir := qdata.NewIndirectionResolver(me)
	errs := make([]error, 0)

	for _, req := range reqs {
		req.Success = false
		req.Err = ""

		indirectEntity, indirectField, err := ir.Resolve(ctx, req.EntityId, req.FieldType)
		if err != nil {
			req.Err = err.Error()
			errs = append(errs, err)
			continue
		}

		// Check if the field is part of the entity type schema
		schema, err := me.GetFieldSchema(ctx, indirectEntity.GetEntityType(), indirectField)
		if err != nil {
			req.Err = fmt.Sprintf("schema not found for field %s in entity type %s: %v", indirectField, indirectEntity.GetEntityType(), err)
			errs = append(errs, fmt.Errorf(req.Err))
			continue
		}

		// Check if the subject is allowed to write to the field
		if authorizer, ok := qcontext.GetAuthorizer(ctx); ok {
			if !authorizer.CanWrite(ctx, new(qdata.Field).Init(req.EntityId, req.FieldType)) {
				req.Err = fmt.Sprintf("permission denied for field %s in entity type %s", req.FieldType, req.EntityId.GetEntityType())
				errs = append(errs, fmt.Errorf(req.Err))
				continue
			} else {
				subjectId := authorizer.SubjectId()
				req.WriterId = &subjectId
			}
		}

		oldReq := new(qdata.Request).Init(indirectEntity, indirectField)
		// It's okay if the field doesn't exist
		me.Read(ctx, oldReq)
		if oldReq.Success && req.WriteOpt == qdata.WriteChanges {
			if req.Value.Equals(oldReq.Value) {
				// No changes, so we can skip the write
				req.Success = true
				continue
			}
		}

		if req.WriteTime == nil {
			wt := time.Now()
			req.WriteTime = new(qdata.WriteTime).FromTime(wt)
		}

		if req.WriterId == nil || req.WriterId.IsEmpty() {
			wr := new(qdata.EntityId).FromString("")

			appName := qcontext.GetAppName(ctx)
			if me.clientId == nil && appName != "" {
				iter, err := me.PrepareQuery(`SELECT "$EntityId" FROM Client WHERE Name = %q`, appName)

				if err == nil {
					iter.ForEach(ctx, func(client qdata.QueryRow) bool {
						entityId := client.AsEntity().EntityId
						me.clientId = &entityId
						return false
					})
				}
			}

			if me.clientId != nil {
				*wr = *me.clientId
			}

			req.WriterId = wr
		}

		err = me.core.WithClient(ctx, func(ctx context.Context, client *redis.Client) error {
			b, err := req.AsField().AsBytes()
			if err != nil {
				return err
			}

			result := client.HSet(ctx, me.keyBuilder.GetEntityKey(req.EntityId), req.FieldType.AsString(), b)
			if result.Err() != nil {
				return result.Err()
			}

			return nil
		})

		if err != nil {
			req.Err = fmt.Sprintf("failed to write field %s in entity type %s: %v", req.FieldType, req.EntityId.GetEntityType(), err)
			errs = append(errs, fmt.Errorf(req.Err))
			continue
		}

		me.publisherSig.Emit(qdata.PublishNotificationArgs{
			Ctx:  ctx,
			Curr: req,
			Prev: oldReq,
		})

		req.Success = true
	}

	return qdata.AccumulateErrors(errs...)
}

func (me *RedisStoreInteractor) InitializeSchema(ctx context.Context) error {
	// Nothing to initialize
	return nil
}

func (me *RedisStoreInteractor) CreateSnapshot(ctx context.Context) (*qdata.Snapshot, error) {
	// Implement snapshot creation logic here
	return nil, nil
}

func (me *RedisStoreInteractor) RestoreSnapshot(ctx context.Context, ss *qdata.Snapshot) error {
	// Clear the database
	return me.core.WithClient(ctx, func(ctx context.Context, client *redis.Client) error {
		result := client.FlushDB(ctx)
		if result.Err() != nil {
			return result.Err()
		}

		return nil
	})
}
