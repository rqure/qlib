package qmap

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/rqure/qlib/pkg/qdata"
	"gopkg.in/yaml.v3"
)

// DefaultFieldConfig represents a default field configuration in YAML
type DefaultFieldConfig struct {
	Name string `yaml:"name"`
	Type string `yaml:"type"`
	Rank int    `yaml:"rank"`
}

// ConfigDefaults contains the default configurations
type ConfigDefaults struct {
	DefaultFields []DefaultFieldConfig `yaml:"defaultFields"`
}

var (
	defaultConfig     *ConfigDefaults
	defaultConfigOnce sync.Once
	defaultConfigErr  error
)

// GetDefaultConfig loads the default configuration from YAML
func GetDefaultConfig() (*ConfigDefaults, error) {
	defaultConfigOnce.Do(func() {
		// Try to find configuration file in several locations
		locations := []string{
			"schemas.yaml", // Current directory
			filepath.Join("qdata", "qstore", "schemas.yaml"), // Relative path
			"/workspace/qlib/pkg/qdata/qstore/schemas.yaml",  // Absolute path
		}

		var configData []byte
		var err error

		for _, loc := range locations {
			configData, err = os.ReadFile(loc)
			if err == nil {
				break
			}
		}

		if err != nil {
			defaultConfigErr = fmt.Errorf("could not find schemas.yaml: %w", err)
			return
		}

		config := &ConfigDefaults{}
		err = yaml.Unmarshal(configData, config)
		if err != nil {
			defaultConfigErr = fmt.Errorf("failed to parse config file: %w", err)
			return
		}

		defaultConfig = config
	})

	return defaultConfig, defaultConfigErr
}

func FindClosestValueType(valueType string) (qdata.ValueType, error) {
	valueTypeMap := map[string]qdata.ValueType{}
	for _, vt := range qdata.ValueTypes {
		valueTypeMap[strings.ToLower(vt.AsString())] = vt
	}

	if vt, ok := valueTypeMap[strings.ToLower(valueType)]; ok {
		return vt, nil
	}

	return "", fmt.Errorf("unknown value type: %s", valueType)
}

// ApplyDefaultFields applies the default fields from config to a schema
func ApplyDefaultFields(schema *qdata.EntitySchema) error {
	config, err := GetDefaultConfig()
	if err != nil {
		// Fall back to hardcoded defaults if config can't be loaded
		schema.Field(qdata.FTName, qdata.FSOValueType(qdata.VTString), qdata.FSORank(0))
		schema.Field(qdata.FTDescription, qdata.FSOValueType(qdata.VTString), qdata.FSORank(1))
		schema.Field(qdata.FTParent, qdata.FSOValueType(qdata.VTEntityReference), qdata.FSORank(2))
		schema.Field(qdata.FTChildren, qdata.FSOValueType(qdata.VTEntityList), qdata.FSORank(3))
		return fmt.Errorf("falling back to hardcoded defaults: %w", err)
	}

	// Apply defaults from config
	for _, field := range config.DefaultFields {
		fieldType := qdata.FieldType(field.Name)
		valueType, err := FindClosestValueType(field.Type)
		if err != nil {
			return fmt.Errorf("invalid value type '%s' for field '%s': %w", field.Type, field.Name, err)
		}
		schema.Field(fieldType, qdata.FSOValueType(valueType), qdata.FSORank(field.Rank))
	}

	return nil
}
