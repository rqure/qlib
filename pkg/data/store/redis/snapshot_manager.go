package redis

import (
	"context"

	"github.com/rqure/qlib/pkg/data"
	"github.com/rqure/qlib/pkg/data/field"
	"github.com/rqure/qlib/pkg/data/request"
	"github.com/rqure/qlib/pkg/data/snapshot"
	"github.com/rqure/qlib/pkg/log"
)

type SnapshotManager struct {
	core          Core
	schemaManager data.SchemaManager
	entityManager data.EntityManager
	fieldOperator data.FieldOperator
}

func NewSnapshotManager(core Core) data.ModifiableSnapshotManager {
	return &SnapshotManager{core: core}
}

func (me *SnapshotManager) SetSchemaManager(manager data.SchemaManager) {
	me.schemaManager = manager
}

func (me *SnapshotManager) SetEntityManager(manager data.EntityManager) {
	me.entityManager = manager
}

func (me *SnapshotManager) SetFieldOperator(operator data.FieldOperator) {
	me.fieldOperator = operator
}

func (me *SnapshotManager) CreateSnapshot(ctx context.Context) data.Snapshot {
	ss := snapshot.New()

	usedTypes := map[string]bool{}
	for _, entityType := range me.entityManager.GetEntityTypes(ctx) {
		if schema := me.schemaManager.GetEntitySchema(ctx, entityType); schema != nil {
			for _, entityId := range me.entityManager.FindEntities(ctx, entityType) {
				usedTypes[entityType] = true

				if entity := me.entityManager.GetEntity(ctx, entityId); entity != nil {
					ss.AppendEntity(entity)

					for _, fieldName := range schema.GetFieldNames() {
						r := request.New().
							SetEntityId(entityId).
							SetFieldName(fieldName)

						me.fieldOperator.Read(ctx, r)
						if r.IsSuccessful() {
							ss.AppendField(field.FromRequest(r))
						}
					}
				}
			}

			if usedTypes[entityType] {
				ss.AppendSchema(schema)
			}
		}
	}

	return ss
}

func (me *SnapshotManager) RestoreSnapshot(ctx context.Context, ss data.Snapshot) {
	log.Info("Restoring snapshot...")

	me.core.GetClient().FlushDB(ctx)

	for _, schema := range ss.GetSchemas() {
		me.schemaManager.SetEntitySchema(ctx, schema)
	}

	for _, entity := range ss.GetEntities() {
		me.entityManager.SetEntity(ctx, entity)
		me.core.GetClient().SAdd(ctx, me.core.GetKeyGen().GetEntityTypeKey(entity.GetType()), entity.GetId())
	}

	for _, field := range ss.GetFields() {
		me.fieldOperator.Write(ctx, request.FromField(field))
	}

	log.Info("Snapshot restored.")
}
