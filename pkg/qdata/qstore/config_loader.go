package qstore

import (
	"fmt"
	"os"

	"github.com/rqure/qlib/pkg/qdata"
	"github.com/rqure/qlib/pkg/qprotobufs"
	"gopkg.in/yaml.v3"
)

// SchemaConfig represents the YAML configuration for schemas and initial entities
type SchemaConfig struct {
	EntitySchemas   []EntitySchemaConfig  `yaml:"entitySchemas"`
	InitialEntities []InitialEntityConfig `yaml:"initialEntities"`
	SchemaUpdates   []EntitySchemaConfig  `yaml:"schemaUpdates"`
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

// ConvertToEntitySchema converts a config to a qdata.EntitySchema
func ConvertToEntitySchema(config EntitySchemaConfig) *qdata.EntitySchema {
	entityType := qdata.EntityType(config.Name)
	fields := make([]*qprotobufs.DatabaseFieldSchema, 0, len(config.Fields))

	for _, fieldConfig := range config.Fields {
		field := &qprotobufs.DatabaseFieldSchema{
			Name: qdata.FieldType(fieldConfig.Name).AsString(),
			Type: qdata.ValueType(fieldConfig.Type).AsString(),
			Rank: fieldConfig.Rank,
		}

		if len(fieldConfig.ChoiceOptions) > 0 {
			field.ChoiceOptions = fieldConfig.ChoiceOptions
		}

		if len(fieldConfig.WritePermissions) > 0 {
			field.WritePermissions = fieldConfig.WritePermissions
		}

		fields = append(fields, field)
	}

	return new(qdata.EntitySchema).FromEntitySchemaPb(&qprotobufs.DatabaseEntitySchema{
		Name:   entityType.AsString(),
		Fields: fields,
	})
}
