package qdata

import (
	"context"
	"fmt"
)

// EntityFieldValidator ensures that entities and their fields exist in the schema
type EntityFieldValidator interface {
	// RegisterEntityFields registers required fields for an entity type
	RegisterEntityFields(entityType EntityType, fields ...FieldType)
	// ValidateFields checks if all registered entity fields exist in the schema
	ValidateFields(context.Context) error
}

type entityFieldValidatorImpl struct {
	store    *Store
	entities map[EntityType][]FieldType
}

func NewEntityFieldValidator(store *Store) EntityFieldValidator {
	return &entityFieldValidatorImpl{
		store: store,
		entities: map[EntityType][]FieldType{
			ETRoot:                 {FTName, FTDescription, FTParent, FTChildren},
			ETFolder:               {FTName, FTDescription, FTParent, FTChildren},
			ETPermission:           {FTName, FTDescription, FTParent, FTChildren},
			ETAreaOfResponsibility: {FTName, FTDescription, FTParent, FTChildren},
			ETRole:                 {FTName, FTDescription, FTParent, FTChildren, FTAreasOfResponsibilities},
			ETUser:                 {FTName, FTDescription, FTParent, FTChildren, FTRoles, FTAreasOfResponsibilities, FTSourceOfTruth, FTKeycloakId, FTEmail, FTFirstName, FTLastName, FTIsEmailVerified, FTIsEnabled, FTJSON},
			ETClient:               {FTName, FTDescription, FTParent, FTChildren, FTLogLevel, FTQLibLogLevel},
			ETSessionController:    {FTName, FTDescription, FTParent, FTChildren, FTLastEventTime, FTLogout},
		},
	}
}

func (v *entityFieldValidatorImpl) RegisterEntityFields(entityType EntityType, fields ...FieldType) {
	v.entities[entityType] = fields
}

func (v *entityFieldValidatorImpl) ValidateFields(ctx context.Context) error {
	for entityType, fields := range v.entities {
		schema, err := v.store.GetEntitySchema(ctx, entityType)
		if err != nil {
			return fmt.Errorf("schema does not exist: %s", entityType)
		}

		for _, f := range fields {
			fsc := schema.Fields[f]
			if fsc == nil {
				return fmt.Errorf("field does not exist: %s->%s", entityType, f)
			}
		}
	}
	return nil
}
