package data

import (
	"fmt"

	"github.com/rqure/qlib/pkg/log"
)

// EntityFieldValidator ensures that entities and their fields exist in the schema
type EntityFieldValidator interface {
	// RegisterEntityFields registers required fields for an entity type
	RegisterEntityFields(entityType string, fields ...string)
	// ValidateFields checks if all registered entity fields exist in the schema
	ValidateFields() error
}

type entityFieldValidatorImpl struct {
	store    Store
	entities map[string][]string
}

func NewEntityFieldValidator(store Store) EntityFieldValidator {
	return &entityFieldValidatorImpl{
		store: store,
		entities: map[string][]string{
			"Root":    {"SchemaUpdateTrigger"},
			"Service": {"Leader", "Candidates", "HeartbeatTrigger", "ApplicationName", "FailOverTrigger"},
		},
	}
}

func (v *entityFieldValidatorImpl) RegisterEntityFields(entityType string, fields ...string) {
	v.entities[entityType] = fields
}

func (v *entityFieldValidatorImpl) ValidateFields() error {
	for entityType, fields := range v.entities {
		schema := v.store.GetEntitySchema(entityType)
		if schema == nil {
			log.Error("[EntityFieldValidator::ValidateFields] Schema does not exist: %v", entityType)
			return fmt.Errorf("schema does not exist: %s", entityType)
		}

		for _, f := range fields {
			fsc := schema.GetField(f)
			if fsc == nil {
				log.Error("[EntityFieldValidator::ValidateFields] Field does not exist: %v->%v", entityType, f)
				return fmt.Errorf("field does not exist: %s->%s", entityType, f)
			}
		}
	}
	return nil
}
