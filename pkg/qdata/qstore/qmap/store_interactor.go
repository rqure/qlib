package qmap

import (
	"context"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/expr-lang/expr"
	"github.com/rqure/qlib/pkg/qcontext"
	"github.com/rqure/qlib/pkg/qdata"
	"github.com/rqure/qlib/pkg/qlog"
	"github.com/rqure/qlib/pkg/qss"
)

// MapStoreInteractor implements StoreInteractor for map-based storage
type MapStoreInteractor struct {
	core MapCore

	publisherSig           qss.Signal[qdata.PublishNotificationArgs]
	readEventSig           qss.Signal[qdata.ReadEventArgs]
	writeEventSig          qss.Signal[qdata.WriteEventArgs]
	interactorConnected    qss.Signal[qdata.ConnectedArgs]
	interactorDisconnected qss.Signal[qdata.DisconnectedArgs]
	clientId               *qdata.EntityId
}

// NewStoreInteractor creates a new map-based store interactor
func NewStoreInteractor(core MapCore) qdata.StoreInteractor {
	interactor := &MapStoreInteractor{
		core:                   core,
		publisherSig:           qss.New[qdata.PublishNotificationArgs](),
		readEventSig:           qss.New[qdata.ReadEventArgs](),
		writeEventSig:          qss.New[qdata.WriteEventArgs](),
		interactorConnected:    qss.New[qdata.ConnectedArgs](),
		interactorDisconnected: qss.New[qdata.DisconnectedArgs](),
	}

	core.Connected().Connect(interactor.onConnected)
	core.Disconnected().Connect(interactor.onDisconnected)

	core.SetSnapshotManager(interactor)

	return interactor
}

func (i *MapStoreInteractor) onConnected(args qdata.ConnectedArgs) {
	i.interactorConnected.Emit(args)
}

func (i *MapStoreInteractor) onDisconnected(args qdata.DisconnectedArgs) {
	i.interactorDisconnected.Emit(args)
}

// CreateEntity creates a new entity
func (i *MapStoreInteractor) CreateEntity(ctx context.Context, entityType qdata.EntityType, parentId qdata.EntityId, name string) (*qdata.Entity, error) {
	var entity *qdata.Entity

	err := i.core.WithWriteLock(ctx, func(ctx context.Context) error {
		entityId := qdata.GenerateEntityId(entityType)

		// Keep generating IDs until we find one that doesn't exist
		for {
			exists, err := i.EntityExists(ctx, entityId)
			if err != nil {
				return err
			}

			if !exists {
				break
			}

			entityId = qdata.GenerateEntityId(entityType)
		}

		err := i.core.CreateEntity(entityId)
		if err != nil {
			return err
		}

		entity = new(qdata.Entity).Init(entityId, qdata.EOEntityType(entityType))
		schema, err := i.GetEntitySchema(ctx, entityType)
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
			err = i.Read(ctx, req)
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
		err = i.Write(ctx, reqs...)
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

// DeleteEntity deletes an entity and its children
func (i *MapStoreInteractor) DeleteEntity(ctx context.Context, entityId qdata.EntityId) error {
	return i.core.WithWriteLock(ctx, func(ctx context.Context) error {
		var errs []error

		// First, get the entity's children
		req := new(qdata.Request).Init(entityId, qdata.FTChildren)
		err := i.Read(ctx, req)
		if err == nil {
			// Recursively delete all children
			children := req.Value.GetEntityList()
			for _, childId := range children {
				if err := i.DeleteEntity(ctx, childId); err != nil {
					errs = append(errs, err)
				}
			}
		}

		// Delete all fields for this entity
		fields, err := i.core.ListEntityFields(entityId)
		if err != nil {
			errs = append(errs, err)
		} else {
			for _, field := range fields {
				if err := i.core.DeleteField(entityId, field); err != nil {
					errs = append(errs, err)
				}
			}
		}

		// Delete the entity itself
		if err := i.core.DeleteEntity(entityId); err != nil {
			errs = append(errs, err)
		}

		// Check if there were any errors during deletion
		if len(errs) > 0 {
			return qdata.AccumulateErrors(errs...)
		}

		return nil
	})
}

// PrepareQuery processes and executes a query
func (i *MapStoreInteractor) PrepareQuery(sql string, args ...any) (*qdata.PageResult[qdata.QueryRow], error) {
	qlog.Trace("PrepareQuery called with SQL: %s, args: %v", sql, args)
	pageOpts := []qdata.PageOpts{}
	typeHintOpts := []qdata.TypeHintOpts{}
	queryEngine := qdata.QESqlite
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
			var result *qdata.PageResult[qdata.QueryRow]

			// Wrap the query execution in a read lock
			err := i.core.WithReadLock(ctx, func(ctx context.Context) error {
				// Format and parse the query
				fmtQuery := fmt.Sprintf(sql, otherArgs...)
				qlog.Trace("Formatted query: %s", fmtQuery)
				parsedQuery, err := qdata.ParseQuery(ctx, fmtQuery, i)
				if err != nil {
					qlog.Error("Failed to parse query: %v", err)
					result = &qdata.PageResult[qdata.QueryRow]{
						Items:    []qdata.QueryRow{},
						CursorId: -1,
						NextPage: nil,
					}
					return err
				}

				if queryEngine == qdata.QESqlite {
					qlog.Trace("Using SQLite for query")
					builder, err := qdata.NewSQLiteBuilder(i)
					if err != nil {
						return err
					}

					r, err := builder.QueryWithPagination(ctx, parsedQuery, pageConfig.PageSize, pageConfig.CursorId, typeHintOpts...)
					if err != nil {
						// Clean up if there was an error
						builder.Close()
						return err
					}

					r.Cleanup = builder.Close
					result = r
					return nil
				}

				return fmt.Errorf("query engine '%s' not supported for this query", queryEngine)
			})

			if err != nil {
				if result == nil {
					result = &qdata.PageResult[qdata.QueryRow]{
						Items:    []qdata.QueryRow{},
						CursorId: -1,
						NextPage: nil,
					}
				}
				return result, err
			}

			return result, nil
		},
	}, nil
}

// Find searches for entities matching criteria
func (i *MapStoreInteractor) Find(ctx context.Context, entityType qdata.EntityType, fieldTypes []qdata.FieldType, conditionFns ...interface{}) ([]*qdata.Entity, error) {
	var results []*qdata.Entity

	err := i.core.WithReadLock(ctx, func(ctx context.Context) error {
		results = make([]*qdata.Entity, 0)

		iter, err := i.FindEntities(entityType)
		if err != nil {
			return err
		}
		defer iter.Close()

		reqs := make([]*qdata.Request, 0)
		iter.ForEach(ctx, func(entityId qdata.EntityId) bool {
			entity := new(qdata.Entity).Init(entityId)
			results = append(results, entity)

			for _, fieldType := range fieldTypes {
				reqs = append(reqs, entity.Field(fieldType).AsReadRequest())
			}

			return true
		})

		if len(reqs) > 0 {
			err = i.Read(ctx, reqs...)
			if err != nil {
				return err
			}
		}

		for _, conditionFn := range conditionFns {
			switch conditionFn := conditionFn.(type) {
			case func(entity *qdata.Entity) bool:
				results = slices.DeleteFunc(results, func(e *qdata.Entity) bool {
					return !conditionFn(e)
				})
			case string:
				program, err := expr.Compile(conditionFn)
				if err != nil {
					return fmt.Errorf("failed to compile condition function: %v", err)
				}
				results = slices.DeleteFunc(results, func(entity *qdata.Entity) bool {
					params := make(map[string]interface{})
					for _, fieldType := range fieldTypes {
						params[fieldType.AsString()] = entity.Field(fieldType).Value.GetRaw()
					}
					r, err := expr.Run(program, params)
					if err != nil {
						qlog.Warn("failed to run condition function '%s': %v", conditionFn, err)
						return true
					}
					b, ok := r.(bool)
					if !ok {
						qlog.Warn("condition function '%s' did not return a boolean value", conditionFn)
						return true
					}
					return !b
				})
			default:
				return fmt.Errorf("unsupported condition function type: %T", conditionFn)
			}
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	return results, nil
}

// FindEntities returns paginated entity ids of a specific type
func (i *MapStoreInteractor) FindEntities(entityType qdata.EntityType, pageOpts ...qdata.PageOpts) (*qdata.PageResult[qdata.EntityId], error) {
	pageConfig := qdata.DefaultPageConfig().ApplyOpts(pageOpts...)

	if pageConfig.PageSize <= 0 {
		pageConfig.PageSize = 100
	}

	return &qdata.PageResult[qdata.EntityId]{
		Items:    []qdata.EntityId{},
		CursorId: pageConfig.CursorId,
		NextPage: func(ctx context.Context) (*qdata.PageResult[qdata.EntityId], error) {
			var entities []qdata.EntityId
			var nextCursorId int64 = -1 // Default to no more results

			err := i.core.WithReadLock(ctx, func(ctx context.Context) error {
				// Get all entities of this type - now more efficient with nested map
				allEntities, err := i.core.ListEntities(entityType)
				if err != nil {
					return err
				}

				// Sort entities by ID (we'll use numeric part for sorting)
				slices.SortFunc(allEntities, func(a, b qdata.EntityId) int {
					aId := a.AsInt()
					bId := b.AsInt()
					return int(aId - bId)
				})

				// Apply pagination
				startIdx := 0
				if pageConfig.CursorId > 0 {
					// Find the starting point based on cursor
					for idx, entity := range allEntities {
						if entity.AsInt() > pageConfig.CursorId {
							startIdx = idx
							break
						}
					}
				} else if pageConfig.CursorId < 0 {
					// Negative cursor means we've reached the end
					return nil
				}

				// Calculate end index for pagination
				endIdx := startIdx + int(pageConfig.PageSize)
				if endIdx > len(allEntities) {
					endIdx = len(allEntities)
				}

				// Extract the page of results
				if startIdx < len(allEntities) {
					entities = allEntities[startIdx:endIdx]

					// Set the next cursor if there are more results
					if endIdx < len(allEntities) {
						nextCursorId = allEntities[endIdx-1].AsInt()
					} else {
						nextCursorId = -1 // No more results
					}
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
					if nextCursorId < 0 {
						return &qdata.PageResult[qdata.EntityId]{
							Items:    []qdata.EntityId{},
							CursorId: -1,
							NextPage: nil,
						}, nil
					}
					return i.FindEntities(entityType,
						qdata.POCursorId(nextCursorId),
						qdata.POPageSize(pageConfig.PageSize))
				},
			}, nil
		},
	}, nil
}

// GetEntityTypes lists all entity types
func (i *MapStoreInteractor) GetEntityTypes(pageOpts ...qdata.PageOpts) (*qdata.PageResult[qdata.EntityType], error) {
	pageConfig := qdata.DefaultPageConfig().ApplyOpts(pageOpts...)

	if pageConfig.PageSize <= 0 {
		pageConfig.PageSize = 100
	}

	return &qdata.PageResult[qdata.EntityType]{
		Items:    []qdata.EntityType{},
		CursorId: pageConfig.CursorId,
		NextPage: func(ctx context.Context) (*qdata.PageResult[qdata.EntityType], error) {
			var entityTypes []qdata.EntityType
			var nextCursorId int64 = -1

			err := i.core.WithReadLock(ctx, func(ctx context.Context) error {
				// Get all schema types - now directly from schemas map
				allTypes, err := i.core.ListEntityTypes()
				if err != nil {
					return err
				}

				// Sort schema types by their string representation
				slices.SortFunc(allTypes, func(a, b qdata.EntityType) int {
					return strings.Compare(a.AsString(), b.AsString())
				})

				// Apply pagination
				startIdx := 0
				if pageConfig.CursorId > 0 {
					// Find starting point based on cursor
					startIdStr := strconv.FormatInt(pageConfig.CursorId, 10)
					for idx, t := range allTypes {
						if t.AsString() > startIdStr {
							startIdx = idx
							break
						}
					}
				} else if pageConfig.CursorId < 0 {
					// Negative cursor means we've reached the end
					return nil
				}

				// Calculate end index for pagination
				endIdx := startIdx + int(pageConfig.PageSize)
				if endIdx > len(allTypes) {
					endIdx = len(allTypes)
				}

				// Extract the page of results
				if startIdx < len(allTypes) {
					entityTypes = allTypes[startIdx:endIdx]

					// Set next cursor if there are more results
					if endIdx < len(allTypes) {
						nextCursorId = allTypes[endIdx-1].AsInt()
					} else {
						nextCursorId = -1 // No more results
					}
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
					if nextCursorId < 0 {
						return &qdata.PageResult[qdata.EntityType]{
							Items:    []qdata.EntityType{},
							CursorId: -1,
							NextPage: nil,
						}, nil
					}
					return i.GetEntityTypes(
						qdata.POCursorId(nextCursorId),
						qdata.POPageSize(pageConfig.PageSize))
				},
			}, nil
		},
	}, nil
}

// EntityExists checks if an entity exists
func (i *MapStoreInteractor) EntityExists(ctx context.Context, entityId qdata.EntityId) (bool, error) {
	var exists bool
	err := i.core.WithReadLock(ctx, func(ctx context.Context) error {
		exists = i.core.EntityExists(entityId)
		return nil
	})
	return exists, err
}

// FieldExists checks if a field exists in an entity type schema
func (i *MapStoreInteractor) FieldExists(ctx context.Context, entityType qdata.EntityType, fieldType qdata.FieldType) (bool, error) {
	var exists bool

	err := i.core.WithReadLock(ctx, func(ctx context.Context) error {
		schema, err := i.GetEntitySchema(ctx, entityType)
		if err != nil {
			return err
		}

		_, ok := schema.Fields[fieldType]
		exists = ok
		return nil
	})

	return exists, err
}

// GetEntitySchema retrieves an entity schema
func (i *MapStoreInteractor) GetEntitySchema(ctx context.Context, entityType qdata.EntityType) (*qdata.EntitySchema, error) {
	var schema *qdata.EntitySchema
	var exists bool

	err := i.core.WithReadLock(ctx, func(ctx context.Context) error {
		schema, exists = i.core.GetSchema(entityType)
		return nil
	})

	if err != nil {
		return nil, err
	}

	if !exists {
		return nil, fmt.Errorf("schema not found for entity type %s", entityType)
	}

	return schema, nil
}

// SetEntitySchema stores an entity schema
func (i *MapStoreInteractor) SetEntitySchema(ctx context.Context, schema *qdata.EntitySchema) error {
	return i.core.WithWriteLock(ctx, func(ctx context.Context) error {
		// Apply default fields from configuration
		err := ApplyDefaultFields(schema)
		if err != nil {
			qlog.Warn("Using fallback defaults: %v", err)
		}

		oldSchema, oldSchemaErr := i.GetEntitySchema(ctx, schema.EntityType)
		removedFields := make(qdata.FieldTypeSlice, 0)
		newFields := make(qdata.FieldTypeSlice, 0)

		// Save the schema
		err = i.core.SetSchema(schema.EntityType, schema)
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
			iter, err := i.FindEntities(schema.EntityType)
			if err != nil {
				return err
			}

			iter.ForEach(ctx, func(entityId qdata.EntityId) bool {
				// Remove deleted fields
				for _, field := range removedFields {
					if err := i.core.DeleteField(entityId, field); err != nil {
						errs = append(errs, fmt.Errorf("failed to remove field %s from entity %s: %w", field, entityId, err))
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
				err = i.Write(ctx, reqs...)
				if err != nil {
					errs = append(errs, fmt.Errorf("failed to add fields (%+v) to entity %s: %w", newFields, entityId, err))
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

// GetFieldSchema retrieves a field schema
func (i *MapStoreInteractor) GetFieldSchema(ctx context.Context, entityType qdata.EntityType, fieldType qdata.FieldType) (*qdata.FieldSchema, error) {
	schema, err := i.GetEntitySchema(ctx, entityType)
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

// SetFieldSchema updates a field schema
func (i *MapStoreInteractor) SetFieldSchema(ctx context.Context, entityType qdata.EntityType, fieldType qdata.FieldType, schema *qdata.FieldSchema) error {
	return i.core.WithWriteLock(ctx, func(ctx context.Context) error {
		entitySchema, err := i.GetEntitySchema(ctx, entityType)
		if err != nil {
			return err
		}

		entitySchema.Fields[fieldType] = schema

		return i.SetEntitySchema(ctx, entitySchema)
	})
}

// Read reads field values from storage
func (i *MapStoreInteractor) Read(ctx context.Context, reqs ...*qdata.Request) error {
	return i.core.WithReadLock(ctx, func(ctx context.Context) error {
		ir := qdata.NewIndirectionResolver(i)
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

			var field *qdata.Field
			var exists bool

			// Note: We're already inside a read lock from WithReadLock above,
			// so we don't need another lock for the individual field access
			field, exists = i.core.GetField(indirectEntity, indirectField)

			if !exists {
				req.Err = fmt.Errorf("field %s not found in entity %s", indirectField, entity.EntityId)
				errs = append(errs, req.Err)
				continue
			}

			req.Value.FromValue(field.Value)
			if req.WriteTime == nil {
				req.WriteTime = new(qdata.WriteTime)
			}
			req.WriteTime.FromTime(field.WriteTime.AsTime())
			if req.WriterId == nil {
				req.WriterId = new(qdata.EntityId)
			}
			req.WriterId.FromString(field.WriterId.AsString())
			req.Success = true

			i.readEventSig.Emit(qdata.ReadEventArgs{
				Ctx: ctx,
				Req: req,
			})
		}

		return qdata.AccumulateErrors(errs...)
	})
}

// Write writes field values to storage
func (i *MapStoreInteractor) Write(ctx context.Context, reqs ...*qdata.Request) error {
	return i.core.WithWriteLock(ctx, func(ctx context.Context) error {
		ir := qdata.NewIndirectionResolver(i)
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
			schema, err := i.GetFieldSchema(ctx, indirectEntity.GetEntityType(), indirectField)
			if err != nil {
				req.Err = fmt.Errorf("schema not found for field %s in entity type %s: %v", indirectField, indirectEntity.GetEntityType(), err)
				errs = append(errs, req.Err)
				continue
			}

			if req.Value == nil {
				req.Value = schema.ValueType.NewValue()
			} else if req.Value.Type() != schema.ValueType {
				qlog.Warn("Type mismatch for field %s in entity type %s: expected %s, got %s", indirectField, indirectEntity.GetEntityType(), schema.ValueType, req.Value.Type())
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
			i.Read(ctx, oldReq)
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
				if i.clientId == nil && appName != "" {
					clients, err := i.Find(ctx,
						qdata.ETClient,
						[]qdata.FieldType{qdata.FTName},
						func(e *qdata.Entity) bool { return e.Field(qdata.FTName).Value.GetString() == appName })

					if err == nil && len(clients) > 0 {
						i.clientId = &clients[0].EntityId
					}
				}

				if i.clientId != nil {
					*wr = *i.clientId
				}

				req.WriterId = wr
			}

			// Create a field object from the request
			field := req.AsField()

			// Write the field to storage
			err = i.core.SetField(indirectEntity, indirectField, field)
			if err != nil {
				req.Err = fmt.Errorf("failed to write field %s in entity type %s: %v", req.FieldType, req.EntityId.GetEntityType(), err)
				errs = append(errs, req.Err)
				continue
			}

			i.publisherSig.Emit(qdata.PublishNotificationArgs{
				Ctx:  ctx,
				Curr: req,
				Prev: oldReq,
			})

			req.Success = true

			i.writeEventSig.Emit(qdata.WriteEventArgs{
				Ctx: ctx,
				Req: req,
			})
		}

		return qdata.AccumulateErrors(errs...)
	})
}

// InitializeSchema initializes the basic schema for the storage
func (i *MapStoreInteractor) InitializeSchema(ctx context.Context) error {
	return i.core.WithWriteLock(ctx, func(ctx context.Context) error {
		// Nothing special needed for map-based storage
		return nil
	})
}

// CreateSnapshot creates a snapshot of the entire database
func (i *MapStoreInteractor) CreateSnapshot(ctx context.Context) (*qdata.Snapshot, error) {
	var snapshot *qdata.Snapshot

	err := i.core.WithReadLock(ctx, func(ctx context.Context) error {
		snapshot = &qdata.Snapshot{}

		// Get all entity types
		entityTypesResult, err := i.GetEntityTypes()
		if err != nil {
			return fmt.Errorf("failed to get entity types: %w", err)
		}

		// Get all schemas and entities using ForEach
		entityTypesResult.ForEach(ctx, func(et qdata.EntityType) bool {
			// Get schema for this entity type
			schema, err := i.GetEntitySchema(ctx, et)
			if err == nil {
				snapshot.Schemas = append(snapshot.Schemas, schema)
			} else {
				qlog.Warn("Failed to get schema for entity type %s: %v", et, err)
				// Continue with next entity type even if this one fails
				return true
			}

			// Get all entity IDs for this type
			entityIdsResult, err := i.FindEntities(et)
			if err != nil {
				qlog.Warn("Failed to find entities of type %s: %v", et, err)
				return true
			}
			defer entityIdsResult.Close()

			// Process all entities using ForEach
			entityIdsResult.ForEach(ctx, func(entityId qdata.EntityId) bool {
				entity := new(qdata.Entity).Init(entityId)

				// Get the entity's schema to know which fields to fetch
				schema, err := i.GetEntitySchema(ctx, entityId.GetEntityType())
				if err != nil {
					qlog.Warn("Failed to get schema for entity %s: %v", entityId, err)
					return true // Continue with next entity
				}

				// Create read requests for each field in the schema
				requests := make([]*qdata.Request, 0, len(schema.Fields))
				for fieldType := range schema.Fields {
					requests = append(requests, new(qdata.Request).Init(entityId, fieldType))
				}

				// Read all fields at once
				err = i.Read(ctx, requests...)
				if err != nil {
					qlog.Warn("Some fields couldn't be read for entity %s: %v", entityId, err)
					// Continue with any fields that were successfully read
				}

				// Add successful reads to the entity
				for _, req := range requests {
					if req.Success {
						field := req.AsField()
						entity.Fields[req.FieldType] = field
					}
				}

				if len(entity.Fields) > 0 {
					snapshot.Entities = append(snapshot.Entities, entity)
				}

				return true // Continue to next entity
			})

			return true // Continue to next entity type
		})

		return nil
	})

	if err != nil {
		return nil, err
	}

	return snapshot, nil
}

// RestoreSnapshot restores a database from a snapshot
func (i *MapStoreInteractor) RestoreSnapshot(ctx context.Context, ss *qdata.Snapshot) error {
	return i.core.WithWriteLock(ctx, func(ctx context.Context) error {
		errs := make([]error, 0)

		i.core.SetSnapshot(ss)

		return qdata.AccumulateErrors(errs...)
	})
}

// PublishNotifications returns the signal for notification publishing
func (i *MapStoreInteractor) PublishNotifications() qss.Signal[qdata.PublishNotificationArgs] {
	return i.publisherSig
}

// ReadEvent returns the signal for read events
func (i *MapStoreInteractor) ReadEvent() qss.Signal[qdata.ReadEventArgs] {
	return i.readEventSig
}

// WriteEvent returns the signal for write events
func (i *MapStoreInteractor) WriteEvent() qss.Signal[qdata.WriteEventArgs] {
	return i.writeEventSig
}

// InteractorConnected returns the signal for connection events
func (i *MapStoreInteractor) InteractorConnected() qss.Signal[qdata.ConnectedArgs] {
	return i.interactorConnected
}

// InteractorDisconnected returns the signal for disconnection events
func (i *MapStoreInteractor) InteractorDisconnected() qss.Signal[qdata.DisconnectedArgs] {
	return i.interactorDisconnected
}
