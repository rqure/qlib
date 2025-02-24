package nats

import (
	"context"

	"github.com/rqure/qlib/pkg/data"
	"github.com/rqure/qlib/pkg/data/snapshot"
	"github.com/rqure/qlib/pkg/log"
	"github.com/rqure/qlib/pkg/protobufs"
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

func (s *SnapshotManager) SetSchemaManager(sm data.SchemaManager) {
	s.schemaManager = sm
}

func (s *SnapshotManager) SetEntityManager(em data.EntityManager) {
	s.entityManager = em
}

func (s *SnapshotManager) SetFieldOperator(fo data.FieldOperator) {
	s.fieldOperator = fo
}

func (s *SnapshotManager) CreateSnapshot(ctx context.Context) data.Snapshot {
	msg := &protobufs.ApiConfigCreateSnapshotRequest{}

	resp, err := s.core.Request(ctx, s.core.GetKeyGenerator().GetReadSubject(), msg)
	if err != nil {
		return nil
	}

	var response protobufs.ApiConfigCreateSnapshotResponse
	if err := resp.Payload.UnmarshalTo(&response); err != nil {
		return nil
	}

	if response.Status != protobufs.ApiConfigCreateSnapshotResponse_SUCCESS {
		return nil
	}

	return snapshot.FromPb(response.Snapshot)
}

func (s *SnapshotManager) RestoreSnapshot(ctx context.Context, ss data.Snapshot) {
	msg := &protobufs.ApiConfigRestoreSnapshotRequest{
		Snapshot: snapshot.ToPb(ss),
	}

	_, err := s.core.Request(ctx, s.core.GetKeyGenerator().GetWriteSubject(), msg)
	if err != nil {
		log.Error("Failed to restore snapshot: %v", err)
	}
}
