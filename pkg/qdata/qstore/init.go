package qstore

import (
	"context"
	"fmt"
	"strings"

	"github.com/rqure/qlib/pkg/qdata"
	"github.com/rqure/qlib/pkg/qlog"
	"github.com/rqure/qlib/pkg/qprotobufs"
)

func Initialize(ctx context.Context, s qdata.StoreInteractor) error {
	// Create entity schemas (copied from InitStoreWorker.OnStoreConnected)
	err := ensureEntitySchema(ctx, s, new(qdata.EntitySchema).FromEntitySchemaPb(&qprotobufs.DatabaseEntitySchema{
		Name: qdata.ETRoot.AsString(),
		Fields: []*qprotobufs.DatabaseFieldSchema{
			{Name: qdata.FTSchemaChanged.AsString(), Type: qdata.VTString.AsString()},          // written value is the entity type that had its schema changed
			{Name: qdata.FTEntityCreated.AsString(), Type: qdata.VTEntityReference.AsString()}, // written value is the entity id that was created
			{Name: qdata.FTEntityDeleted.AsString(), Type: qdata.VTEntityReference.AsString()}, // written value is the entity id that was deleted
		},
	}))
	if err != nil {
		return fmt.Errorf("failed to ensure entity schema: %v", err)
	}

	err = ensureEntitySchema(ctx, s, new(qdata.EntitySchema).FromEntitySchemaPb(&qprotobufs.DatabaseEntitySchema{
		Name:   qdata.ETFolder.AsString(),
		Fields: []*qprotobufs.DatabaseFieldSchema{},
	}))
	if err != nil {
		return fmt.Errorf("failed to ensure entity schema: %v", err)
	}

	err = ensureEntitySchema(ctx, s, new(qdata.EntitySchema).FromEntitySchemaPb(&qprotobufs.DatabaseEntitySchema{
		Name: qdata.ETPermission.AsString(),
		Fields: []*qprotobufs.DatabaseFieldSchema{
			{Name: qdata.FTPolicy.AsString(), Type: qdata.VTString.AsString(), Rank: 5},
		},
	}))
	if err != nil {
		return fmt.Errorf("failed to ensure entity schema: %v", err)
	}

	err = ensureEntitySchema(ctx, s, new(qdata.EntitySchema).FromEntitySchemaPb(&qprotobufs.DatabaseEntitySchema{
		Name:   qdata.ETAreaOfResponsibility.AsString(),
		Fields: []*qprotobufs.DatabaseFieldSchema{},
	}))
	if err != nil {
		return fmt.Errorf("failed to ensure entity schema: %v", err)
	}

	err = ensureEntitySchema(ctx, s, new(qdata.EntitySchema).FromEntitySchemaPb(&qprotobufs.DatabaseEntitySchema{
		Name:   qdata.ETRole.AsString(),
		Fields: []*qprotobufs.DatabaseFieldSchema{},
	}))
	if err != nil {
		return fmt.Errorf("failed to ensure entity schema: %v", err)
	}

	err = ensureEntitySchema(ctx, s, new(qdata.EntitySchema).FromEntitySchemaPb(&qprotobufs.DatabaseEntitySchema{
		Name: qdata.ETUser.AsString(),
		Fields: []*qprotobufs.DatabaseFieldSchema{
			{Name: qdata.FTRoles.AsString(), Type: qdata.VTEntityList.AsString(), Rank: 5},
			{Name: qdata.FTAreasOfResponsibilities.AsString(), Type: qdata.VTEntityList.AsString(), Rank: 6},
			{Name: qdata.FTSourceOfTruth.AsString(), Type: qdata.VTChoice.AsString(), ChoiceOptions: []string{"QOS", "Keycloak"}, Rank: 7},
			{Name: qdata.FTKeycloakId.AsString(), Type: qdata.VTString.AsString(), Rank: 8},
			{Name: qdata.FTEmail.AsString(), Type: qdata.VTString.AsString(), Rank: 9},
			{Name: qdata.FTFirstName.AsString(), Type: qdata.VTString.AsString(), Rank: 10},
			{Name: qdata.FTLastName.AsString(), Type: qdata.VTString.AsString(), Rank: 11},
			{Name: qdata.FTIsEmailVerified.AsString(), Type: qdata.VTBool.AsString(), Rank: 12},
			{Name: qdata.FTIsEnabled.AsString(), Type: qdata.VTBool.AsString(), Rank: 13},
			{Name: qdata.FTJSON.AsString(), Type: qdata.VTString.AsString(), Rank: 14},
		},
	}))
	if err != nil {
		return fmt.Errorf("failed to ensure entity schema: %v", err)
	}

	err = ensureEntitySchema(ctx, s, new(qdata.EntitySchema).FromEntitySchemaPb(&qprotobufs.DatabaseEntitySchema{
		Name: qdata.ETClient.AsString(),
		Fields: []*qprotobufs.DatabaseFieldSchema{
			{Name: qdata.FTLogLevel.AsString(), Type: qdata.VTChoice.AsString(), ChoiceOptions: []string{"Trace", "Debug", "Info", "Warn", "Error", "Panic"}, Rank: 5},
			{Name: qdata.FTQLibLogLevel.AsString(), Type: qdata.VTChoice.AsString(), ChoiceOptions: []string{"Trace", "Debug", "Info", "Warn", "Error", "Panic"}, Rank: 6},
			{Name: qdata.FTReadsPerSecond.AsString(), Type: qdata.VTInt.AsString(), Rank: 7},
			{Name: qdata.FTWritesPerSecond.AsString(), Type: qdata.VTInt.AsString(), Rank: 8},
		},
	}))
	if err != nil {
		return fmt.Errorf("failed to ensure entity schema: %v", err)
	}

	err = ensureEntitySchema(ctx, s, new(qdata.EntitySchema).FromEntitySchemaPb(&qprotobufs.DatabaseEntitySchema{
		Name: qdata.ETSessionController.AsString(),
		Fields: []*qprotobufs.DatabaseFieldSchema{
			{Name: qdata.FTLastEventTime.AsString(), Type: qdata.VTTimestamp.AsString(), Rank: 5},
			{Name: qdata.FTLogout.AsString(), Type: qdata.VTEntityReference.AsString(), Rank: 6},
		},
	}))
	if err != nil {
		return fmt.Errorf("failed to ensure entity schema: %v", err)
	}

	// Create root entity
	_, err = ensureEntity(ctx, s, qdata.ETRoot, "Root")
	if err != nil {
		return fmt.Errorf("failed to create root entity: %v", err)
	}

	// Create the security models
	_, err = ensureEntity(ctx, s, qdata.ETFolder, "Root", "Security Models")
	if err != nil {
		return fmt.Errorf("failed to create security models folder: %v", err)
	}

	_, err = ensureEntity(ctx, s, qdata.ETFolder, "Root", "Security Models", "Permissions")
	if err != nil {
		return fmt.Errorf("failed to create permissions folder: %v", err)
	}

	kernelPermission, err := ensureEntity(ctx, s, qdata.ETPermission, "Root", "Security Models", "Permissions", "Kernel")
	if err != nil {
		return fmt.Errorf("failed to create system permission: %v", err)
	}
	// Policies are written in the tengo scripting language
	kernelPermission.Field("Policy").Value.FromString(`
				contains := func(l, v) {
					for i in l {
						if i == v {
							return true
						}
					}
					return false
				}
	
				ALLOW := false
				if contains(["Client"], SUBJECT.entityType()) {
					STORE.read(
						SUBJECT.field("Name").asReadRequest())
					
					name := SUBJECT.field("Name").value.getString()
					ALLOW = contains(["qinitdb", "qsql", "qcore"], name)
				} else if contains(["User"], SUBJECT.entityType()) {
					 SUBJECT.read(
						SUBJECT.field("Name").asReadRequest())
					
					name := SUBJECT.field("Name").value.getString()
					ALLOW = contains(["qei"], name)
				}
			`)
	err = s.Write(ctx, kernelPermission.Field("Policy").AsWriteRequest())
	if err != nil {
		return fmt.Errorf("failed to write kernel permission policy: %v", err)
	}

	_, err = ensureEntity(ctx, s, qdata.ETFolder, "Root", "Security Models", "Areas of Responsibility")
	if err != nil {
		return fmt.Errorf("failed to create areas of responsibility folder: %v", err)
	}

	_, err = ensureEntity(ctx, s, qdata.ETAreaOfResponsibility, "Root", "Security Models", "Areas of Responsibility", "System")
	if err != nil {
		return fmt.Errorf("failed to create system area of responsibility: %v", err)
	}

	_, err = ensureEntity(ctx, s, qdata.ETFolder, "Root", "Security Models", "Roles")
	if err != nil {
		return fmt.Errorf("failed to create roles folder: %v", err)
	}

	adminRole, err := ensureEntity(ctx, s, qdata.ETRole, "Root", "Security Models", "Roles", "Admin")
	if err != nil {
		return fmt.Errorf("failed to create admin role: %v", err)
	}

	_, err = ensureEntity(ctx, s, qdata.ETFolder, "Root", "Security Models", "Users")
	if err != nil {
		return fmt.Errorf("failed to create users folder: %v", err)
	}

	adminUser, err := ensureEntity(ctx, s, qdata.ETUser, "Root", "Security Models", "Users", "qei")
	if err != nil {
		return fmt.Errorf("failed to create admin user: %v", err)
	}

	_, err = ensureEntity(ctx, s, qdata.ETFolder, "Root", "Security Models", "Clients")
	if err != nil {
		return fmt.Errorf("failed to create clients folder: %v", err)
	}

	_, err = ensureEntity(ctx, s, qdata.ETClient, "Root", "Security Models", "Clients", "qinitdb")
	if err != nil {
		return fmt.Errorf("failed to create initdb client: %v", err)
	}

	_, err = ensureEntity(ctx, s, qdata.ETClient, "Root", "Security Models", "Clients", "qsql")
	if err != nil {
		return fmt.Errorf("failed to create initdb client: %v", err)
	}

	_, err = ensureEntity(ctx, s, qdata.ETClient, "Root", "Security Models", "Clients", "qcore")
	if err != nil {
		return fmt.Errorf("failed to create qcore client: %v", err)
	}

	adminUser.Field(qdata.FTRoles).Value.FromEntityList([]qdata.EntityId{adminRole.EntityId})
	adminUser.Field(qdata.FTSourceOfTruth).Value.FromChoice(0)
	err = s.Write(ctx,
		adminUser.Field(qdata.FTRoles).AsWriteRequest(),
		adminUser.Field(qdata.FTSourceOfTruth).AsWriteRequest())
	if err != nil {
		return fmt.Errorf("failed to write admin user roles: %v", err)
	}

	err = ensureEntitySchema(ctx, s, new(qdata.EntitySchema).FromEntitySchemaPb(&qprotobufs.DatabaseEntitySchema{
		Name: qdata.ETPermission.AsString(),
		Fields: []*qprotobufs.DatabaseFieldSchema{
			{
				Name: qdata.FTPolicy.AsString(),
				Type: qdata.VTString.AsString(),
				Rank: 5,
				WritePermissions: []string{
					kernelPermission.EntityId.AsString(),
				},
			},
		},
	}))
	if err != nil {
		return fmt.Errorf("failed to update schema: %v", err)
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

	iter, err := store.PrepareQuery(`SELECT "$EntityId" FROM Root`)
	if err != nil {
		return nil, fmt.Errorf("failed to prepare query: %w", err)
	}
	defer iter.Close()

	var currentNode *qdata.Entity
	if !iter.Next(ctx) {
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
		currentNode = iter.Get().AsEntity()
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
