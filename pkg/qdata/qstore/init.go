package qstore

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/rqure/qlib/pkg/qdata"
	"github.com/rqure/qlib/pkg/qlog"
)

func Initialize(ctx context.Context, s qdata.StoreInteractor) error {
	// Load the schema configuration
	configPath := filepath.Join("schemas.yaml")
	config, err := LoadSchemaConfig(configPath)
	if err != nil {
		return fmt.Errorf("failed to load schema config: %w", err)
	}

	// Create entity schemas from config
	for _, schemaConfig := range config.EntitySchemas {
		schema := ConvertToEntitySchema(schemaConfig)
		err := ensureEntitySchema(ctx, s, schema)
		if err != nil {
			return fmt.Errorf("failed to ensure entity schema for %s: %w", schemaConfig.Name, err)
		}
	}

	// Process initial entities
	entityMap := make(map[string]qdata.EntityId)
	for _, entityConfig := range config.InitialEntities {
		err := processInitialEntity(ctx, s, entityConfig, nil, entityMap)
		if err != nil {
			return fmt.Errorf("failed to process initial entities: %w", err)
		}
	}

	// Process schema updates (with write permissions that may reference created entities)
	for _, schemaUpdate := range config.EntitySchemas {
		// Convert entity name reference to the actual entity ID
		schema := UpdateSchemaPermissions(ctx, s, schemaUpdate)
		err := ensureEntitySchema(ctx, s, schema)
		if err != nil {
			return fmt.Errorf("failed to update schema for %s: %w", schemaUpdate.Name, err)
		}
	}

	return nil
}

// processInitialEntity creates an entity and its children based on config
func processInitialEntity(ctx context.Context, s qdata.StoreInteractor, config InitialEntityConfig, parent *qdata.Entity, entityMap map[string]qdata.EntityId) error {
	var parentPath []string

	if parent != nil {
		err := s.Read(ctx, parent.Field("Name").AsReadRequest())
		if err != nil {
			return fmt.Errorf("failed to read parent name: %w", err)
		}

		parentPath = []string{parent.Field("Name").Value.GetString()}
	}

	// Build the full path
	fullPath := append(parentPath, config.Path)

	// Create the entity
	entityType := qdata.EntityType(config.Type)
	entity, err := ensureEntity(ctx, s, entityType, fullPath...)
	if err != nil {
		return fmt.Errorf("failed to create entity %s: %w", strings.Join(fullPath, "/"), err)
	}

	// Store the entity ID by its path for reference
	pathKey := strings.Join(fullPath, "/")
	entityMap[pathKey] = entity.EntityId
	entityMap[config.Path] = entity.EntityId // Also store just by name for simple references

	// Set fields if any
	for _, field := range config.Fields {
		fieldType := qdata.FieldType(field.Name)
		fieldSchema, err := s.GetFieldSchema(ctx, entityType, fieldType)
		if err != nil {
			return fmt.Errorf("failed to get field schema for %s.%s: %w", entityType, fieldType, err)
		}

		entity.Field(fieldType).Value.FromValue(fieldSchema.ValueType.NewValue(field.Value))
		err = s.Write(ctx, entity.Field(fieldType).AsWriteRequest())
		if err != nil {
			return fmt.Errorf("failed to set field %s for entity %s: %w", field.Name, pathKey, err)
		}
	}

	// Process children
	for _, childConfig := range config.Children {
		err := processInitialEntity(ctx, s, childConfig, entity, entityMap)
		if err != nil {
			return err
		}
	}

	return nil
}

// Helper functions moved from init_store_worker
func ensureEntitySchema(ctx context.Context, s qdata.StoreInteractor, schema *qdata.EntitySchema) error {
	actualSchema, err := s.GetEntitySchema(ctx, schema.EntityType)
	if err == nil {
		for _, field := range schema.Fields {
			actualSchema.Fields[field.FieldType] = field
		}
	} else {
		actualSchema = schema
	}

	err = s.SetEntitySchema(ctx, actualSchema)
	if err != nil {
		return fmt.Errorf("failed to set entity schema for %s: %w", schema.EntityType, err)
	}

	qlog.Info("Ensured entity schema: %s", schema.EntityType)
	return nil
}

func ensureEntity(ctx context.Context, store qdata.StoreInteractor, entityType qdata.EntityType, path ...string) (*qdata.Entity, error) {
	// The first element should be the root entity
	if len(path) == 0 {
		return nil, fmt.Errorf("path cannot be empty")
	}

	roots, err := store.Find(ctx, qdata.ETRoot, []qdata.FieldType{})
	if err != nil {
		return nil, fmt.Errorf("failed to find root: %w", err)
	}

	var currentNode *qdata.Entity
	if len(roots) == 0 {
		if entityType == qdata.ETRoot {
			root, err := store.CreateEntity(ctx, qdata.ETRoot, "", path[0])
			if err != nil {
				return nil, fmt.Errorf("failed to create root entity: %w", err)
			}
			return new(qdata.Entity).Init(root.EntityId), nil
		} else {
			return nil, fmt.Errorf("root entity not found")
		}
	} else {
		currentNode = roots[0]
	}

	// Create the last item in the path
	// Return early if the intermediate entities are not found
	lastIndex := len(path) - 2
	for i, name := range path[1:] {
		err := store.Read(ctx, currentNode.Field("Children").AsReadRequest())
		if err != nil {
			return nil, fmt.Errorf("failed to read children of entity '%s': %w", currentNode.EntityId, err)
		}

		children := currentNode.Field("Children").Value.GetEntityList()

		found := false
		for _, childId := range children {
			child := new(qdata.Entity).Init(childId)

			err = store.Read(ctx,
				child.Field("Name").AsReadRequest(),
				child.Field("Children").AsReadRequest(),
			)
			if err != nil {
				return nil, fmt.Errorf("failed to read child entity '%s': %w", child.EntityId, err)
			}

			if child.Field("Name").Value.GetString() == name {
				currentNode = child
				found = true
				break
			}
		}

		if !found && i == lastIndex {
			et, err := store.CreateEntity(ctx, entityType, currentNode.EntityId, name)
			if err != nil {
				return nil, fmt.Errorf("failed to create entity '%s': %w", name, err)
			}
			return new(qdata.Entity).Init(et.EntityId), nil
		} else if !found {
			return nil, fmt.Errorf("entity '%s' not found in path '%s'", name, strings.Join(path, "/"))
		}
	}

	if currentNode == nil {
		return nil, fmt.Errorf("current node is nil for path '%s'", strings.Join(path, "/"))
	}

	return new(qdata.Entity).Init(currentNode.EntityId), nil
}
