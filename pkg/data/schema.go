package data

import (
	"slices"
)

type ISchemaValidator interface {
	AddEntity(entityId string, fields ...string)
	IsValid() bool
	Validate() bool
	ValidationRequired()
}

type SchemaValidator struct {
	db                 IDatabase
	entities           map[string][]string
	validationRequired bool
	isValid            bool
}

func NewSchemaValidator(db IDatabase) ISchemaValidator {
	return &SchemaValidator{
		db: db,
		entities: map[string][]string{
			"Root":    {"SchemaUpdateTrigger"},
			"Service": {"Leader", "Candidates", "HeartbeatTrigger", "ApplicationName", "FailOverTrigger"},
		},
		validationRequired: true,
	}
}

func (v *SchemaValidator) AddEntity(entityType string, fields ...string) {
	v.entities[entityType] = fields
}

func (v *SchemaValidator) Validate() bool {
	for entityType, fields := range v.entities {
		schema := v.db.GetEntitySchema(entityType)
		if schema == nil {
			Error("[SchemaValidator] Schema does not exist: %v", entityType)
			return false
		}

		for _, field := range fields {
			if !slices.Contains(schema.Fields, field) {
				Error("[SchemaValidator] Field does not exist: %v->%v", entityType, field)
				return false
			}
		}
	}
	return true
}

func (v *SchemaValidator) ValidationRequired() {
	v.validationRequired = true
}

func (v *SchemaValidator) IsValid() bool {
	if v.validationRequired {
		v.isValid = v.Validate()
		v.validationRequired = false
	}

	return v.isValid
}
