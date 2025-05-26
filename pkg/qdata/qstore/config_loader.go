package qstore

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/rqure/qlib/pkg/qdata"
	"github.com/rqure/qlib/pkg/qlog"
	"github.com/rqure/qlib/pkg/qprotobufs"
	"gopkg.in/yaml.v3"
)

// SchemaConfig represents the YAML configuration for schemas and initial entities
type SchemaConfig struct {
	EntitySchemas   []EntitySchemaConfig  `yaml:"entitySchemas"`
	InitialEntities []InitialEntityConfig `yaml:"initialEntities"`
}

// EntitySchemaConfig represents an entity schema in the config
type EntitySchemaConfig struct {
	Name   string              `yaml:"name"`
	Fields []FieldSchemaConfig `yaml:"fields"`
}

// FieldSchemaConfig represents a field schema in the config
type FieldSchemaConfig struct {
	Name             string   `yaml:"name"`
	Type             string   `yaml:"type"`
	Rank             int32    `yaml:"rank"`
	ChoiceOptions    []string `yaml:"choiceOptions,omitempty"`
	WritePermissions []string `yaml:"writePermissions,omitempty"`
	ReadPermissions  []string `yaml:"readPermissions,omitempty"`
}

// InitialEntityConfig represents an entity to be created
type InitialEntityConfig struct {
	Type     string                `yaml:"type"`
	Path     string                `yaml:"path"`
	Children []InitialEntityConfig `yaml:"children,omitempty"`
	Fields   []FieldValueConfig    `yaml:"fields,omitempty"`
}

// FieldValueConfig represents a field value to be set
type FieldValueConfig struct {
	Name  string `yaml:"name"`
	Value string `yaml:"value"`
}

// LoadSchemaConfig loads the YAML config file
func LoadSchemaConfig(filePath string) (*SchemaConfig, error) {
	data, err := os.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	var config SchemaConfig
	err = yaml.Unmarshal(data, &config)
	if err != nil {
		return nil, fmt.Errorf("failed to parse config file: %w", err)
	}

	return &config, nil
}

func findClosestValueType(valueType string) (qdata.ValueType, error) {
	valueTypeMap := map[string]qdata.ValueType{}
	for _, vt := range qdata.ValueTypes {
		valueTypeMap[strings.ToLower(vt.AsString())] = vt
	}

	if vt, ok := valueTypeMap[strings.ToLower(valueType)]; ok {
		return vt, nil
	}

	return "", fmt.Errorf("unknown value type: %s", valueType)
}

// ConvertToEntitySchema converts a config to a qdata.EntitySchema
func ConvertToEntitySchema(config EntitySchemaConfig) *qdata.EntitySchema {
	entityType := qdata.EntityType(config.Name)
	fields := make([]*qprotobufs.DatabaseFieldSchema, 0, len(config.Fields))

	for _, fieldConfig := range config.Fields {
		vt, err := findClosestValueType(fieldConfig.Type)
		if err != nil {
			qlog.Error("Invalid value type '%s' for field '%s' in entity '%s': %v", fieldConfig.Type, fieldConfig.Name, config.Name, err)
			continue // Skip this field if the type is invalid
		}

		field := &qprotobufs.DatabaseFieldSchema{
			Name: qdata.FieldType(fieldConfig.Name).AsString(),
			Type: vt.AsString(),
			Rank: fieldConfig.Rank,
		}

		if len(fieldConfig.ChoiceOptions) > 0 {
			field.ChoiceOptions = fieldConfig.ChoiceOptions
		}

		fields = append(fields, field)
	}

	return new(qdata.EntitySchema).FromEntitySchemaPb(&qprotobufs.DatabaseEntitySchema{
		Name:   entityType.AsString(),
		Fields: fields,
	})
}

func UpdateSchemaPermissions(s qdata.StoreInteractor, config EntitySchemaConfig) *qdata.EntitySchema {
	entityType := qdata.EntityType(config.Name)
	fields := make([]*qprotobufs.DatabaseFieldSchema, 0, len(config.Fields))

	for _, fieldConfig := range config.Fields {
		vt, err := findClosestValueType(fieldConfig.Type)
		if err != nil {
			qlog.Error("Invalid value type '%s' for field '%s' in entity '%s': %v", fieldConfig.Type, fieldConfig.Name, config.Name, err)
			continue // Skip this field if the type is invalid
		}

		field := &qprotobufs.DatabaseFieldSchema{
			Name: qdata.FieldType(fieldConfig.Name).AsString(),
			Type: vt.AsString(),
			Rank: fieldConfig.Rank,
		}

		if len(fieldConfig.ChoiceOptions) > 0 {
			field.ChoiceOptions = fieldConfig.ChoiceOptions
		}

		for _, permName := range fieldConfig.WritePermissions {
			permissions, err := s.Find(
				context.Background(),
				qdata.ETPermission,
				[]qdata.FieldType{qdata.FTName},
				fmt.Sprintf("Name == %q", permName),
			)
			if err != nil {
				qlog.Error("Failed to find permission '%s' for field '%s' in entity '%s': %v", permName, fieldConfig.Name, config.Name, err)
				continue // Skip this field if the permission lookup fails
			}
			if len(permissions) == 0 {
				qlog.Error("Permission '%s' not found for field '%s' in entity '%s'", permName, fieldConfig.Name, config.Name)
				continue // Skip this field if the permission is not found
			}
			field.WritePermissions = append(field.WritePermissions, permissions[0].EntityId.AsString())
		}

		for _, permName := range fieldConfig.ReadPermissions {
			permissions, err := s.Find(
				context.Background(),
				qdata.ETPermission,
				[]qdata.FieldType{qdata.FTName},
				fmt.Sprintf("Name == %q", permName),
			)
			if err != nil {
				qlog.Error("Failed to find permission '%s' for field '%s' in entity '%s': %v", permName, fieldConfig.Name, config.Name, err)
				continue // Skip this field if the permission lookup fails
			}
			if len(permissions) == 0 {
				qlog.Error("Permission '%s' not found for field '%s' in entity '%s'", permName, fieldConfig.Name, config.Name)
				continue // Skip this field if the permission is not found
			}
			field.ReadPermissions = append(field.ReadPermissions, permissions[0].EntityId.AsString())
		}

		fields = append(fields, field)
	}

	return new(qdata.EntitySchema).FromEntitySchemaPb(&qprotobufs.DatabaseEntitySchema{
		Name:   entityType.AsString(),
		Fields: fields,
	})
}
