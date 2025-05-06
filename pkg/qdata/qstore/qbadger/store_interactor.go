package qbadger

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/rqure/qlib/pkg/qcontext"
	"github.com/rqure/qlib/pkg/qdata"
	"github.com/rqure/qlib/pkg/qlog"
	"github.com/rqure/qlib/pkg/qss"
)

// Sorted set implementation for BadgerDB
type SortedSet struct {
	Items map[string]float64
}

func NewSortedSet() *SortedSet {
	return &SortedSet{
		Items: make(map[string]float64),
	}
}

func (s *SortedSet) Add(member string, score float64) {
	s.Items[member] = score
}

func (s *SortedSet) Remove(member string) {
	delete(s.Items, member)
}

func (s *SortedSet) GetRange(start, stop int64) []string {
	// Convert to slice and sort
	type Item struct {
		Member string
		Score  float64
	}

	items := make([]Item, 0, len(s.Items))
	for member, score := range s.Items {
		items = append(items, Item{Member: member, Score: score})
	}

	// Sort by score (and member for ties)
	// Using a simple bubble sort for clarity
	for i := 0; i < len(items)-1; i++ {
		for j := 0; j < len(items)-i-1; j++ {
			if items[j].Score > items[j+1].Score ||
				(items[j].Score == items[j+1].Score && items[j].Member > items[j+1].Member) {
				items[j], items[j+1] = items[j+1], items[j]
			}
		}
	}

	// Get requested range
	result := []string{}
	if stop < 0 {
		stop = int64(len(items)) - 1
	}
	if start < 0 {
		start = 0
	}

	for i := start; i <= stop && i < int64(len(items)); i++ {
		result = append(result, items[i].Member)
	}

	return result
}

// BadgerStoreInteractor implements BadgerDB-specific storage mechanisms
type BadgerStoreInteractor struct {
	core          BadgerCore
	keyBuilder    *KeyBuilder
	publisherSig  qss.Signal[qdata.PublishNotificationArgs]
	readEventSig  qss.Signal[qdata.ReadEventArgs]
	writeEventSig qss.Signal[qdata.WriteEventArgs]
	clientId      *qdata.EntityId
}

// NewStoreInteractor creates a new BadgerDB store interactor
func NewStoreInteractor(core BadgerCore, opts ...func(*BadgerStoreInteractor)) qdata.StoreInteractor {
	r := &BadgerStoreInteractor{
		core:          core,
		keyBuilder:    NewKeyBuilder("qos"),
		publisherSig:  qss.New[qdata.PublishNotificationArgs](),
		readEventSig:  qss.New[qdata.ReadEventArgs](),
		writeEventSig: qss.New[qdata.WriteEventArgs](),
	}
	for _, opt := range opts {
		opt(r)
	}
	return r
}

// getSortedSet retrieves a sorted set from BadgerDB
func (me *BadgerStoreInteractor) getSortedSet(txn *badger.Txn, key string) (*SortedSet, error) {
	item, err := txn.Get([]byte(key))
	if err != nil {
		if err == badger.ErrKeyNotFound {
			return NewSortedSet(), nil
		}
		return nil, err
	}

	set := NewSortedSet()
	err = item.Value(func(val []byte) error {
		return json.Unmarshal(val, &set.Items)
	})

	if err != nil {
		return nil, err
	}
	return set, nil
}

// setSortedSet stores a sorted set in BadgerDB
func (me *BadgerStoreInteractor) setSortedSet(txn *badger.Txn, key string, set *SortedSet) error {
	data, err := json.Marshal(set.Items)
	if err != nil {
		return err
	}
	return txn.Set([]byte(key), data)
}

func (me *BadgerStoreInteractor) CreateEntity(ctx context.Context, entityType qdata.EntityType, parentId qdata.EntityId, name string) (*qdata.Entity, error) {
	var entity *qdata.Entity
	err := me.core.WithWriteTxn(ctx, func(txn *badger.Txn) error {
		entityId := qdata.GenerateEntityId(entityType)

		// Keep generating IDs until we find one that doesn't exist
		for {
			exists, err := me.EntityExists(ctx, entityId)
			if err != nil {
				return err
			}

			if !exists {
				break
			}

			entityId = qdata.GenerateEntityId(entityType)
		}

		entity = new(qdata.Entity).Init(entityId, qdata.EOEntityType(entityType))
		schema, err := me.GetEntitySchema(ctx, entityType)
		if err != nil {
			return err
		}

		// Add entity to the sorted set of entities
		err = me.core.WithWriteTxn(ctx, func(txn *badger.Txn) error {
			set, err := me.getSortedSet(txn, me.keyBuilder.GetAllEntitiesKey(entityType))
			if err != nil {
				return err
			}
			set.Add(entityId.AsString(), 0) // Use 0 as the score
			return me.setSortedSet(txn, me.keyBuilder.GetAllEntitiesKey(entityType), set)
		})

		if err != nil {
			return err
		}

		// Create the initial fields based on the schema
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

		// If this entity has a parent, we need to add this entity to the parent's children
		if parentId != "" {
			req := new(qdata.Request).Init(parentId, qdata.FTChildren)
			err = me.Read(ctx, req)
			if err == nil {
				children := req.Value.GetEntityList()
				children = append(children, entityId)
				req.Value.FromEntityList(children)
				reqs = append(reqs, req)
			} else {
				return fmt.Errorf("failed to read parent entity: %v", err)
			}
		}

		// Write all the fields
		err = me.Write(ctx, reqs...)
		if err != nil {
			return err
		}

		return nil
	})

	if err != nil {
		return nil, fmt.Errorf("failed to create entity: %v", err)
	}

	return entity, nil
}

func (me *BadgerStoreInteractor) DeleteEntity(ctx context.Context, entityId qdata.EntityId) error {
	return me.core.WithWriteTxn(ctx, func(txn *badger.Txn) error {
		var errs []error

		// First, get the entity's children
		req := new(qdata.Request).Init(entityId, qdata.FTChildren)
		err := me.Read(ctx, req)
		if err == nil {
			// Recursively delete all children
			children := req.Value.GetEntityList()
			for _, childId := range children {
				if err := me.DeleteEntity(ctx, childId); err != nil {
					errs = append(errs, err)
				}
			}
		}

		// Remove from the sorted set of entities
		err = me.core.WithWriteTxn(ctx, func(txn *badger.Txn) error {
			set, err := me.getSortedSet(txn, me.keyBuilder.GetAllEntitiesKey(entityId.GetEntityType()))
			if err != nil {
				return err
			}
			set.Remove(entityId.AsString())
			return me.setSortedSet(txn, me.keyBuilder.GetAllEntitiesKey(entityId.GetEntityType()), set)
		})
		if err != nil {
			errs = append(errs, err)
		}

		// Delete all fields for this entity
		err = me.core.WithWriteTxn(ctx, func(txn *badger.Txn) error {
			// Use a prefix seek to find all keys for this entity
			prefix := []byte(me.keyBuilder.GetEntityKey(entityId))
			opts := badger.DefaultIteratorOptions
			opts.PrefetchSize = 10
			opts.Prefix = prefix

			it := txn.NewIterator(opts)
			defer it.Close()

			for it.Seek(prefix); it.Valid(); it.Next() {
				key := it.Item().Key()
				if err := txn.Delete(key); err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			errs = append(errs, err)
		}

		// Check if there were any errors during deletion
		if len(errs) > 0 {
			return qdata.AccumulateErrors(errs...)
		}

		return nil
	})
}

func (me *BadgerStoreInteractor) PrepareQuery(sql string, args ...any) (*qdata.PageResult[qdata.QueryRow], error) {
	qlog.Trace("PrepareQuery called with SQL: %s, args: %v", sql, args)
	pageOpts := []qdata.PageOpts{}
	typeHintOpts := []qdata.TypeHintOpts{}
	queryEngine := qdata.QEExprLang
	otherArgs := []any{}

	for _, arg := range args {
		switch arg := arg.(type) {
		case qdata.PageOpts:
			pageOpts = append(pageOpts, arg)
		case qdata.TypeHintOpts:
			typeHintOpts = append(typeHintOpts, arg)
		case qdata.QueryEngineType:
			queryEngine = arg
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
			if queryEngine == qdata.QEExprLang {
				qlog.Trace("Using ExprEvaluator for query")
				evaluator := qdata.NewExprEvaluator(me, parsedQuery)
				if evaluator.TryCompile() {
					return evaluator.ExecuteWithPagination(ctx, pageConfig.PageSize, pageConfig.CursorId, typeHintOpts...)
				} else {
					return nil, fmt.Errorf("query engine '%s' cannot evaluate this query", queryEngine)
				}
			}

			if queryEngine == qdata.QESqlite {
				qlog.Trace("Using SQLite for query")
				builder, err := qdata.NewSQLiteBuilder(me)
				if err != nil {
					return nil, err
				}

				result, err := builder.QueryWithPagination(ctx, parsedQuery, pageConfig.PageSize, pageConfig.CursorId, typeHintOpts...)
				if err != nil {
					// Clean up if there was an error
					builder.Close()
					return nil, err
				}

				result.Cleanup = builder.Close

				return result, nil
			}

			return nil, fmt.Errorf("query engine '%s' not supported for this query", queryEngine)
		},
	}, nil
}

func (me *BadgerStoreInteractor) FindEntities(entityType qdata.EntityType, pageOpts ...qdata.PageOpts) (*qdata.PageResult[qdata.EntityId], error) {
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

			err := me.core.WithReadTxn(ctx, func(txn *badger.Txn) error {
				set, err := me.getSortedSet(txn, me.keyBuilder.GetAllEntitiesKey(entityType))
				if err != nil {
					return err
				}

				members := set.GetRange(pageConfig.CursorId, pageConfig.CursorId+pageConfig.PageSize-1)

				for _, member := range members {
					entities = append(entities, qdata.EntityId(member))
				}

				if len(members) < int(pageConfig.PageSize) {
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

func (me *BadgerStoreInteractor) GetEntityTypes(pageOpts ...qdata.PageOpts) (*qdata.PageResult[qdata.EntityType], error) {
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

			err := me.core.WithReadTxn(ctx, func(txn *badger.Txn) error {
				set, err := me.getSortedSet(txn, me.keyBuilder.GetAllEntityTypesKey())
				if err != nil {
					return err
				}

				members := set.GetRange(pageConfig.CursorId, pageConfig.CursorId+pageConfig.PageSize-1)

				for _, member := range members {
					entityTypes = append(entityTypes, qdata.EntityType(member))
				}

				if len(members) < int(pageConfig.PageSize) {
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

func (me *BadgerStoreInteractor) EntityExists(ctx context.Context, entityId qdata.EntityId) (bool, error) {
	exists := false

	err := me.core.WithReadTxn(ctx, func(txn *badger.Txn) error {
		set, err := me.getSortedSet(txn, me.keyBuilder.GetAllEntitiesKey(entityId.GetEntityType()))
		if err != nil {
			return err
		}

		// Check if the entity ID exists in the set
		for member := range set.Items {
			if member == entityId.AsString() {
				exists = true
				break
			}
		}

		return nil
	})

	if err != nil {
		return false, err
	}

	return exists, nil
}

func (me *BadgerStoreInteractor) FieldExists(ctx context.Context, entityType qdata.EntityType, fieldType qdata.FieldType) (bool, error) {
	schema, err := me.GetEntitySchema(ctx, entityType)
	if err != nil {
		return false, err
	}

	_, ok := schema.Fields[fieldType]
	return ok, nil
}

func (me *BadgerStoreInteractor) GetEntitySchema(ctx context.Context, entityType qdata.EntityType) (*qdata.EntitySchema, error) {
	schema := new(qdata.EntitySchema).Init(entityType)
	var schemaBytes []byte

	err := me.core.WithReadTxn(ctx, func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(me.keyBuilder.GetSchemaKey(entityType)))
		if err != nil {
			return err
		}

		return item.Value(func(val []byte) error {
			schemaBytes = append([]byte{}, val...)
			return nil
		})
	})

	if err != nil {
		if err == badger.ErrKeyNotFound {
			return nil, fmt.Errorf("schema not found for entity type %s", entityType)
		}
		return nil, err
	}

	if _, err := schema.FromBytes(schemaBytes); err != nil {
		return nil, err
	}

	return schema, nil
}

func (me *BadgerStoreInteractor) SetEntitySchema(ctx context.Context, schema *qdata.EntitySchema) error {
	return me.core.WithWriteTxn(ctx, func(txn *badger.Txn) error {
		// Ensure the default fields are set
		schema.Field(qdata.FTName, qdata.FSOValueType(qdata.VTString), qdata.FSORank(0))
		schema.Field(qdata.FTDescription, qdata.FSOValueType(qdata.VTString), qdata.FSORank(1))
		schema.Field(qdata.FTParent, qdata.FSOValueType(qdata.VTEntityReference), qdata.FSORank(2))
		schema.Field(qdata.FTChildren, qdata.FSOValueType(qdata.VTEntityList), qdata.FSORank(3))

		oldSchema, oldSchemaErr := me.GetEntitySchema(ctx, schema.EntityType)
		removedFields := make(qdata.FieldTypeSlice, 0)
		newFields := make(qdata.FieldTypeSlice, 0)

		// Save the schema in BadgerDB and update the entity types index
		schemaBytes, err := schema.AsBytes()
		if err != nil {
			return err
		}

		// Add to entity types set
		set, err := me.getSortedSet(txn, me.keyBuilder.GetAllEntityTypesKey())
		if err != nil {
			return err
		}
		set.Add(schema.EntityType.AsString(), 0)
		if err := me.setSortedSet(txn, me.keyBuilder.GetAllEntityTypesKey(), set); err != nil {
			return err
		}

		// Save the schema
		err = txn.Set([]byte(me.keyBuilder.GetSchemaKey(schema.EntityType)), schemaBytes)

		if err != nil {
			return err
		}

		errs := make([]error, 0)

		if oldSchemaErr == nil {
			// Find removed fields
			for _, oldField := range oldSchema.Fields {
				if _, ok := schema.Fields[oldField.FieldType]; !ok {
					removedFields = append(removedFields, oldField.FieldType)
				}
			}

			// Find new fields
			for _, newField := range schema.Fields {
				if _, ok := oldSchema.Fields[newField.FieldType]; !ok {
					newFields = append(newFields, newField.FieldType)
				}
			}

			// Update existing entities with the schema changes
			iter, err := me.FindEntities(schema.EntityType)
			if err != nil {
				return err
			}

			iter.ForEach(ctx, func(entityId qdata.EntityId) bool {
				// Remove deleted fields
				if len(removedFields) > 0 {
					err := me.core.WithWriteTxn(ctx, func(txn *badger.Txn) error {
						for _, field := range removedFields {
							if err := txn.Delete([]byte(me.keyBuilder.GetEntityFieldKey(entityId, field))); err != nil {
								return err
							}
						}
						return nil
					})
					if err != nil {
						errs = append(errs, fmt.Errorf("failed to remove fields (%+v) from entity %s: %w", removedFields, entityId.AsString(), err))
					}
				}

				// Add new fields
				reqs := make([]*qdata.Request, 0)
				for _, newField := range newFields {
					req := new(qdata.Request).Init(entityId, newField)
					vt := schema.Fields[newField].ValueType
					req.Value.FromValue(vt.NewValue())
					reqs = append(reqs, req)
				}
				err = me.Write(ctx, reqs...)
				if err != nil {
					errs = append(errs, fmt.Errorf("failed to add fields (%+v) to entity %s: %w", newFields, entityId.AsString(), err))
				}

				return true
			})
		}

		err = qdata.AccumulateErrors(errs...)
		if err != nil {
			return err
		}

		return nil
	})
}

func (me *BadgerStoreInteractor) GetFieldSchema(ctx context.Context, entityType qdata.EntityType, fieldType qdata.FieldType) (*qdata.FieldSchema, error) {
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

func (me *BadgerStoreInteractor) SetFieldSchema(ctx context.Context, entityType qdata.EntityType, fieldType qdata.FieldType, schema *qdata.FieldSchema) error {
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

func (me *BadgerStoreInteractor) PublishNotifications() qss.Signal[qdata.PublishNotificationArgs] {
	return me.publisherSig
}

func (me *BadgerStoreInteractor) Read(ctx context.Context, reqs ...*qdata.Request) error {
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

		// Read field from BadgerDB
		var fieldBytes []byte
		err = me.core.WithReadTxn(ctx, func(txn *badger.Txn) error {
			item, err := txn.Get([]byte(me.keyBuilder.GetEntityFieldKey(indirectEntity, indirectField)))
			if err != nil {
				return err
			}

			return item.Value(func(val []byte) error {
				fieldBytes = append([]byte{}, val...)
				return nil
			})
		})

		if err != nil {
			if err == badger.ErrKeyNotFound {
				req.Err = fmt.Errorf("field %s not found in entity %s", indirectField, entity.EntityId)
			} else {
				req.Err = fmt.Errorf("failed to read field %s in entity type %s: %v", indirectField, entity.EntityType, err)
			}
			errs = append(errs, req.Err)
			continue
		}

		field, err = new(qdata.Field).FromBytes(fieldBytes)
		if err != nil {
			req.Err = fmt.Errorf("failed to deserialize field %s in entity type %s: %v", indirectField, entity.EntityType, err)
			errs = append(errs, req.Err)
			continue
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

func (me *BadgerStoreInteractor) Write(ctx context.Context, reqs ...*qdata.Request) error {
	return me.core.WithWriteTxn(ctx, func(txn *badger.Txn) error {
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

			if req.Value.IsNil() {
				req.Value.FromValue(schema.ValueType.NewValue())
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

			// Write the field to BadgerDB
			fieldBytes, err := req.AsField().AsBytes()
			if err != nil {
				req.Err = fmt.Errorf("failed to serialize field %s in entity type %s: %v", req.FieldType, req.EntityId.GetEntityType(), err)
				errs = append(errs, req.Err)
				continue
			}

			err = txn.Set([]byte(me.keyBuilder.GetEntityFieldKey(indirectEntity, indirectField)), fieldBytes)
			if err != nil {
				req.Err = fmt.Errorf("failed to write field %s in entity type %s: %v", req.FieldType, req.EntityId.GetEntityType(), err)
				errs = append(errs, req.Err)
				continue
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
	})
}

func (me *BadgerStoreInteractor) InitializeSchema(ctx context.Context) error {
	// Nothing to initialize for BadgerDB
	return nil
}

func (me *BadgerStoreInteractor) CreateSnapshot(ctx context.Context) (*qdata.Snapshot, error) {
	var snapshot qdata.Snapshot

	// Get all entity types
	var entityTypes []string
	err := me.core.WithReadTxn(ctx, func(txn *badger.Txn) error {
		set, err := me.getSortedSet(txn, me.keyBuilder.GetAllEntityTypesKey())
		if err != nil {
			return err
		}

		for member := range set.Items {
			entityTypes = append(entityTypes, member)
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	// Get all schemas
	for _, et := range entityTypes {
		schema, err := me.GetEntitySchema(ctx, qdata.EntityType(et))
		if err == nil {
			snapshot.Schemas = append(snapshot.Schemas, schema)
		}
	}

	// Get all entities for each entity type
	for _, et := range entityTypes {
		entityType := qdata.EntityType(et)

		// Get all entity IDs for this type
		var entityIds []string
		err := me.core.WithReadTxn(ctx, func(txn *badger.Txn) error {
			set, err := me.getSortedSet(txn, me.keyBuilder.GetAllEntitiesKey(entityType))
			if err != nil {
				return err
			}

			for member := range set.Items {
				entityIds = append(entityIds, member)
			}

			return nil
		})

		if err != nil {
			return nil, err
		}

		// Get all fields for each entity
		for _, eid := range entityIds {
			entityId := qdata.EntityId(eid)
			entity := new(qdata.Entity).Init(entityId)

			err := me.core.WithReadTxn(ctx, func(txn *badger.Txn) error {
				prefix := []byte(me.keyBuilder.GetEntityKey(entityId))
				opts := badger.DefaultIteratorOptions
				opts.PrefetchSize = 10
				opts.Prefix = prefix

				it := txn.NewIterator(opts)
				defer it.Close()

				for it.Seek(prefix); it.Valid(); it.Next() {
					item := it.Item()
					key := string(item.Key())
					fieldParts := strings.Split(key, ":")

					if len(fieldParts) < 3 {
						continue
					}

					fieldType := qdata.FieldType(fieldParts[len(fieldParts)-1])

					var fieldBytes []byte
					err := item.Value(func(val []byte) error {
						fieldBytes = append([]byte{}, val...)
						return nil
					})

					if err != nil {
						continue
					}

					field, err := new(qdata.Field).FromBytes(fieldBytes)
					if err != nil {
						continue
					}

					entity.Fields[fieldType] = field
				}

				return nil
			})

			if err != nil {
				return nil, err
			}

			if len(entity.Fields) > 0 {
				snapshot.Entities = append(snapshot.Entities, entity)
			}
		}
	}

	return &snapshot, nil
}

func (me *BadgerStoreInteractor) RestoreSnapshot(ctx context.Context, ss *qdata.Snapshot) error {
	// Delete all existing data
	err := me.core.WithWriteTxn(ctx, func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			if err := txn.Delete(it.Item().Key()); err != nil {
				return err
			}
		}

		return nil
	})

	if err != nil {
		return err
	}

	// Restore schemas
	for _, schema := range ss.Schemas {
		schemaBytes, err := schema.AsBytes()
		if err != nil {
			return err
		}

		err = me.core.WithWriteTxn(ctx, func(txn *badger.Txn) error {
			// Add to entity types set
			set, err := me.getSortedSet(txn, me.keyBuilder.GetAllEntityTypesKey())
			if err != nil {
				return err
			}
			set.Add(schema.EntityType.AsString(), 0)
			if err := me.setSortedSet(txn, me.keyBuilder.GetAllEntityTypesKey(), set); err != nil {
				return err
			}

			// Save the schema
			return txn.Set([]byte(me.keyBuilder.GetSchemaKey(schema.EntityType)), schemaBytes)
		})

		if err != nil {
			return err
		}
	}

	// Restore entities and their fields
	for _, entity := range ss.Entities {
		err := me.core.WithWriteTxn(ctx, func(txn *badger.Txn) error {
			// Add entity to the entities set
			set, err := me.getSortedSet(txn, me.keyBuilder.GetAllEntitiesKey(entity.EntityType))
			if err != nil {
				return err
			}
			set.Add(entity.EntityId.AsString(), 0)
			if err := me.setSortedSet(txn, me.keyBuilder.GetAllEntitiesKey(entity.EntityType), set); err != nil {
				return err
			}

			// Write all fields
			for fieldType, field := range entity.Fields {
				fieldBytes, err := field.AsBytes()
				if err != nil {
					return err
				}

				if err := txn.Set([]byte(me.keyBuilder.GetEntityFieldKey(entity.EntityId, fieldType)), fieldBytes); err != nil {
					return err
				}
			}

			return nil
		})

		if err != nil {
			return err
		}
	}

	return nil
}

func (me *BadgerStoreInteractor) ReadEvent() qss.Signal[qdata.ReadEventArgs] {
	return me.readEventSig
}

func (me *BadgerStoreInteractor) WriteEvent() qss.Signal[qdata.WriteEventArgs] {
	return me.writeEventSig
}
