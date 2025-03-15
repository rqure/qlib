package qdata

import (
	"context"
	"fmt"

	"github.com/rqure/qlib/pkg/qlog"
)

// EntityFieldValidator ensures that entities and their fields exist in the schema
type EntityFieldValidator interface {
	// RegisterEntityFields registers required fields for an entity type
	RegisterEntityFields(entityType string, fields ...string)
	// ValidateFields checks if all registered entity fields exist in the schema
	ValidateFields(context.Context) error
}

type entityFieldValidatorImpl struct {
	store    Store
	entities map[string][]string
}

func NewEntityFieldValidator(store Store) EntityFieldValidator {
	return &entityFieldValidatorImpl{
		store: store,
		entities: map[string][]string{
			"Root":                 {"Name", "Description", "Parent", "Children", "SchemaUpdateTrigger"},
			"Folder":               {"Name", "Description", "Parent", "Children"},
			"Permission":           {"Name", "Description", "Parent", "Children"},
			"AreaOfResponsibility": {"Name", "Description", "Parent", "Children"},
			"Role":                 {"Name", "Description", "Parent", "Children", "Permissions", "AreasOfResponsibilities"},
			"User":                 {"Name", "Description", "Parent", "Children", "Roles", "SelectedRole", "Permissions", "TotalPermissions", "AreasOfResponsibilities", "SelectedAORs", "SourceOfTruth", "KeycloakId", "Email", "FirstName", "LastName", "IsEmailVerified", "IsEnabled", "JSON"},
			"Client":               {"Name", "Description", "Parent", "Children", "LogLevel", "QLibLogLevel", "Permissions"},
			"SessionController":    {"Name", "Description", "Parent", "Children", "LastEventTime", "Logout"},
		},
	}
}

func (v *entityFieldValidatorImpl) RegisterEntityFields(entityType string, fields ...string) {
	v.entities[entityType] = fields
}

func (v *entityFieldValidatorImpl) ValidateFields(ctx context.Context) error {
	for entityType, fields := range v.entities {
		schema := v.store.GetEntitySchema(ctx, entityType)
		if schema == nil {
			qlog.Error("Schema does not exist: %v", entityType)
			return fmt.Errorf("schema does not exist: %s", entityType)
		}

		for _, f := range fields {
			fsc := schema.GetField(f)
			if fsc == nil {
				qlog.Error("Field does not exist: %v->%v", entityType, f)
				return fmt.Errorf("field does not exist: %s->%s", entityType, f)
			}
		}
	}
	return nil
}
