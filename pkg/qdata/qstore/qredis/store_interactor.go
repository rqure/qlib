package qredis

import (
	"context"
	"fmt"
	"time"

	"github.com/patrickmn/go-cache"
	"github.com/redis/go-redis/v9"
	"github.com/rqure/qlib/pkg/qcontext"
	"github.com/rqure/qlib/pkg/qdata"
	"github.com/rqure/qlib/pkg/qlog"
	"github.com/rqure/qlib/pkg/qss"
)

// RedisStoreInteractor implements Redis-specific caching mechanisms
type RedisStoreInteractor struct {
	core          RedisCore
	keyBuilder    *KeyBuilder
	publisherSig  qss.Signal[qdata.PublishNotificationArgs]
	readEventSig  qss.Signal[qdata.ReadEventArgs]
	writeEventSig qss.Signal[qdata.WriteEventArgs]
	clientId      *qdata.EntityId

	// Optional in-memory cache for fields and schemas
	cacheEnabled bool
	fieldCache   *cache.Cache // key: entityId:fieldType, value: *qdata.Field
	schemaCache  *cache.Cache // key: entityType, value: *qdata.EntitySchema
}

// Option to enable go-cache for RedisStoreInteractor
func WithGoCache(defaultExpiration, cleanupInterval time.Duration) func(*RedisStoreInteractor) {
	return func(r *RedisStoreInteractor) {
		r.cacheEnabled = true
		r.fieldCache = cache.New(defaultExpiration, cleanupInterval)
		r.schemaCache = cache.New(defaultExpiration, cleanupInterval)
	}
}

// NewStoreInteractor creates a new Redis store interactor
func NewStoreInteractor(core RedisCore, opts ...func(*RedisStoreInteractor)) qdata.StoreInteractor {
	r := &RedisStoreInteractor{
		core:          core,
		keyBuilder:    NewKeyBuilder("qos"),
		publisherSig:  qss.New[qdata.PublishNotificationArgs](),
		readEventSig:  qss.New[qdata.ReadEventArgs](),
		writeEventSig: qss.New[qdata.WriteEventArgs](),
		cacheEnabled:  false,
	}
	for _, opt := range opts {
		opt(r)
	}
	return r
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
	qlog.Trace("PrepareQuery called with SQL: %s, args: %v", sql, args)
	pageOpts := []qdata.PageOpts{}
	typeHintOpts := []qdata.TypeHintOpts{}
	otherArgs := []any{}

	for _, arg := range args {
		switch arg := arg.(type) {
		case qdata.PageOpts:
			pageOpts = append(pageOpts, arg)
		case qdata.TypeHintOpts:
			typeHintOpts = append(typeHintOpts, arg)
		default:
			otherArgs = append(otherArgs, arg)
		}
	}

	// Apply page options or use defaults
	pageConfig := qdata.DefaultPageConfig().ApplyOpts(pageOpts...)

	// Ensure we have a reasonable page size
	if pageConfig.PageSize <= 0 {
		pageConfig.PageSize = 100
	}

	return &qdata.PageResult[qdata.QueryRow]{
		Items:    []qdata.QueryRow{},
		CursorId: pageConfig.CursorId,
		NextPage: func(ctx context.Context) (*qdata.PageResult[qdata.QueryRow], error) {
			// Format and parse the query
			fmtQuery := fmt.Sprintf(sql, otherArgs...)
			qlog.Trace("Formatted query: %s", fmtQuery)
			parsedQuery, err := qdata.ParseQuery(ctx, fmtQuery, me)
			if err != nil {
				qlog.Error("Failed to parse query: %v", err)
				return &qdata.PageResult[qdata.QueryRow]{
					Items:    []qdata.QueryRow{},
					CursorId: -1,
					NextPage: nil,
				}, err
			}

			// Try with ExprEvaluator first
			evaluator := qdata.NewExprEvaluator(me, parsedQuery)
			if evaluator.CanEvaluate() {
				qlog.Trace("Using ExprEvaluator for query")
				return evaluator.ExecuteWithPagination(ctx, pageConfig.PageSize, pageConfig.CursorId, typeHintOpts...)
			}

			// Fall back to SQLiteBuilder for complex queries
			qlog.Trace("Query requires SQLiteBuilder approach")
			builder, err := qdata.NewSQLiteBuilder(me)
			if err != nil {
				qlog.Error("Failed to create SQLite builder: %v", err)
				return &qdata.PageResult[qdata.QueryRow]{
					Items:    []qdata.QueryRow{},
					CursorId: -1,
					NextPage: nil,
				}, err
			}

			result, err := builder.QueryWithPagination(ctx, parsedQuery, pageConfig.PageSize, pageConfig.CursorId, typeHintOpts...)
			if err != nil {
				// Clean up if there was an error
				builder.Close()
				return nil, err
			}

			result.Cleanup = builder.Close

			return result, nil
		},
	}, nil
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

func (me *RedisStoreInteractor) cacheFieldKey(entityId qdata.EntityId, fieldType qdata.FieldType) string {
	return entityId.AsString() + ":" + fieldType.AsString()
}

func (me *RedisStoreInteractor) cacheSchemaKey(entityType qdata.EntityType) string {
	return entityType.AsString()
}

func (me *RedisStoreInteractor) GetEntitySchema(ctx context.Context, entityType qdata.EntityType) (*qdata.EntitySchema, error) {
	// Try cache first
	if me.cacheEnabled && me.schemaCache != nil {
		if v, found := me.schemaCache.Get(me.cacheSchemaKey(entityType)); found {
			if schema, ok := v.(*qdata.EntitySchema); ok {
				return schema.Clone(), nil
			}
		}
	}

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

	// Store in cache
	if me.cacheEnabled && me.schemaCache != nil {
		me.schemaCache.Set(me.cacheSchemaKey(entityType), schema.Clone(), cache.DefaultExpiration)
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

	// Invalidate schema cache
	if me.cacheEnabled && me.schemaCache != nil {
		me.schemaCache.Delete(me.cacheSchemaKey(schema.EntityType))
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

	if fieldSchema.ValueType.IsNil() {
		return nil, fmt.Errorf("field %s in entity type %s has no value type", fieldType, entityType)
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
		req.Err = nil

		indirectEntity, indirectField, err := ir.Resolve(ctx, req.EntityId, req.FieldType)
		if err != nil {
			req.Err = err
			errs = append(errs, err)
			continue
		}

		entity := new(qdata.Entity).Init(indirectEntity)

		if authorizer, ok := qcontext.GetAuthorizer(ctx); ok {
			if !authorizer.CanRead(ctx, new(qdata.Field).Init(indirectEntity, indirectField)) {
				req.Err = fmt.Errorf("permission denied for field %s in entity type %s", indirectField, entity.EntityType)
				errs = append(errs, req.Err)
				continue
			}
		}

		// Try cache first
		var field *qdata.Field
		cacheHit := false
		if me.cacheEnabled && me.fieldCache != nil {
			if v, found := me.fieldCache.Get(me.cacheFieldKey(indirectEntity, indirectField)); found {
				if f, ok := v.(*qdata.Field); ok {
					field = f.Clone()
					cacheHit = true
				}
			}
		}

		if !cacheHit {
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

				field, err = new(qdata.Field).FromBytes(b)
				if err != nil {
					return err
				}

				return nil
			})

			if err != nil {
				req.Err = fmt.Errorf("failed to read field %s in entity type %s: %v", indirectField, entity.EntityType, err)
				errs = append(errs, req.Err)
				continue
			}
		}

		// Store in cache or update existing cache and TTL
		if me.cacheEnabled && me.fieldCache != nil && field != nil {
			me.fieldCache.Set(me.cacheFieldKey(indirectEntity, indirectField), field.Clone(), cache.DefaultExpiration)
		}

		req.Value.FromValue(field.Value)
		req.WriteTime.FromTime(field.WriteTime.AsTime())
		req.WriterId.FromString(field.WriterId.AsString())
		req.Success = true

		me.readEventSig.Emit(qdata.ReadEventArgs{
			Ctx: ctx,
			Req: req,
		})
	}

	return qdata.AccumulateErrors(errs...)
}

func (me *RedisStoreInteractor) Write(ctx context.Context, reqs ...*qdata.Request) error {
	ir := qdata.NewIndirectionResolver(me)
	errs := make([]error, 0)

	for _, req := range reqs {
		req.Success = false
		req.Err = nil

		indirectEntity, indirectField, err := ir.Resolve(ctx, req.EntityId, req.FieldType)
		if err != nil {
			req.Err = err
			errs = append(errs, err)
			continue
		}

		// Check if the field is part of the entity type schema
		schema, err := me.GetFieldSchema(ctx, indirectEntity.GetEntityType(), indirectField)
		if err != nil {
			req.Err = fmt.Errorf("schema not found for field %s in entity type %s: %v", indirectField, indirectEntity.GetEntityType(), err)
			errs = append(errs, req.Err)
			continue
		}

		if req.Value == nil {
			req.Value = schema.ValueType.NewValue()
		}

		// Check if the subject is allowed to write to the field
		if authorizer, ok := qcontext.GetAuthorizer(ctx); ok {
			if !authorizer.CanWrite(ctx, new(qdata.Field).Init(req.EntityId, req.FieldType)) {
				req.Err = fmt.Errorf("permission denied for field %s in entity type %s", req.FieldType, req.EntityId.GetEntityType())
				errs = append(errs, req.Err)
				continue
			} else {
				subjectId := authorizer.SubjectId()
				req.WriterId = &subjectId
			}
		}

		if req.Value == nil {
			req.Value = schema.ValueType.NewValue()
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
			req.Err = fmt.Errorf("failed to write field %s in entity type %s: %v", req.FieldType, req.EntityId.GetEntityType(), err)
			errs = append(errs, req.Err)
			continue
		}

		// Invalidate field cache
		if me.cacheEnabled && me.fieldCache != nil {
			me.fieldCache.Delete(me.cacheFieldKey(indirectEntity, indirectField))
		}

		me.publisherSig.Emit(qdata.PublishNotificationArgs{
			Ctx:  ctx,
			Curr: req,
			Prev: oldReq,
		})

		req.Success = true

		me.writeEventSig.Emit(qdata.WriteEventArgs{
			Ctx: ctx,
			Req: req,
		})
	}

	return qdata.AccumulateErrors(errs...)
}

func (me *RedisStoreInteractor) InitializeSchema(ctx context.Context) error {
	// Nothing to initialize
	return nil
}

func (me *RedisStoreInteractor) CreateSnapshot(ctx context.Context) (*qdata.Snapshot, error) {
	var snapshot qdata.Snapshot

	err := me.core.WithClient(ctx, func(ctx context.Context, client *redis.Client) error {
		entityTypesRes := client.ZRange(ctx, me.keyBuilder.GetAllEntityTypesKey(), 0, -1)
		if entityTypesRes.Err() != nil {
			return entityTypesRes.Err()
		}
		entityTypes := entityTypesRes.Val()

		pipe := client.Pipeline()
		schemaCmds := make([]*redis.StringCmd, len(entityTypes))
		for i, et := range entityTypes {
			schemaCmds[i] = pipe.Get(ctx, me.keyBuilder.GetSchemaKey(qdata.EntityType(et)))
		}
		_, err := pipe.Exec(ctx)
		if err != nil && err != redis.Nil {
			return err
		}

		for _, cmd := range schemaCmds {
			if cmd.Err() == nil {
				b, _ := cmd.Bytes()
				schema := new(qdata.EntitySchema)
				schema.FromBytes(b)
				snapshot.Schemas = append(snapshot.Schemas, schema)
			}
		}

		for _, et := range entityTypes {
			entitiesRes := client.ZRange(ctx, me.keyBuilder.GetAllEntitiesKey(qdata.EntityType(et)), 0, -1)
			if entitiesRes.Err() != nil {
				return entitiesRes.Err()
			}
			entityIds := entitiesRes.Val()

			// Pipeline HGETALL for each entity
			pipe := client.Pipeline()
			entityCmds := make([]*redis.MapStringStringCmd, len(entityIds))
			for i, eid := range entityIds {
				entityCmds[i] = pipe.HGetAll(ctx, me.keyBuilder.GetEntityKey(qdata.EntityId(eid)))
			}
			_, err := pipe.Exec(ctx)
			if err != nil && err != redis.Nil {
				return err
			}
			for i, cmd := range entityCmds {
				if cmd.Err() == nil {
					entity := new(qdata.Entity).Init(qdata.EntityId(entityIds[i]))
					for _, val := range cmd.Val() {
						field, err := new(qdata.Field).FromBytes([]byte(val))
						if err == nil {
							entity.Fields[field.FieldType] = field
						}
					}
					snapshot.Entities = append(snapshot.Entities, entity)
				}
			}
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	return &snapshot, nil
}

func (me *RedisStoreInteractor) RestoreSnapshot(ctx context.Context, ss *qdata.Snapshot) error {
	if me.cacheEnabled {
		if me.fieldCache != nil {
			me.fieldCache.Flush()
		}
		if me.schemaCache != nil {
			me.schemaCache.Flush()
		}
	}

	return me.core.WithClient(ctx, func(ctx context.Context, client *redis.Client) error {
		result := client.FlushDB(ctx)
		if result.Err() != nil {
			return result.Err()
		}

		pipe := client.Pipeline()

		for _, schema := range ss.Schemas {
			b, err := schema.AsBytes()
			if err != nil {
				return err
			}

			pipe.Set(ctx, me.keyBuilder.GetSchemaKey(schema.EntityType), b, 0)
			pipe.ZAdd(ctx, me.keyBuilder.GetAllEntityTypesKey(), redis.Z{
				Score:  float64(0),
				Member: schema.EntityType.AsString(),
			})
		}

		for _, entity := range ss.Entities {
			entityKey := me.keyBuilder.GetEntityKey(entity.EntityId)
			fields := make(map[string]interface{})
			for _, field := range entity.Fields {
				b, err := field.AsBytes()
				if err != nil {
					return err
				}
				fields[field.FieldType.AsString()] = b
			}
			if len(fields) > 0 {
				pipe.HSet(ctx, entityKey, fields)
			}
			pipe.ZAdd(ctx, me.keyBuilder.GetAllEntitiesKey(entity.EntityType), redis.Z{
				Score:  0,
				Member: entity.EntityId.AsString(),
			})
		}

		_, err := pipe.Exec(ctx)
		return err
	})
}

func (me *RedisStoreInteractor) ReadEvent() qss.Signal[qdata.ReadEventArgs] {
	return me.readEventSig
}

func (me *RedisStoreInteractor) WriteEvent() qss.Signal[qdata.WriteEventArgs] {
	return me.writeEventSig
}
