package qnats

import (
	"context"

	"github.com/rqure/qlib/pkg/qdata"
	"github.com/rqure/qlib/pkg/qdata/qsnapshot"
	"github.com/rqure/qlib/pkg/qlog"
	"github.com/rqure/qlib/pkg/qprotobufs"
)

type SnapshotManager struct {
	core          Core
	schemaManager qdata.SchemaManager
	entityManager qdata.EntityManager
	fieldOperator qdata.FieldOperator
}

func NewSnapshotManager(core Core) qdata.ModifiableSnapshotManager {
	return &SnapshotManager{core: core}
}

func (s *SnapshotManager) SetSchemaManager(sm qdata.SchemaManager) {
	s.schemaManager = sm
}

func (s *SnapshotManager) SetEntityManager(em qdata.EntityManager) {
	s.entityManager = em
}

func (s *SnapshotManager) SetFieldOperator(fo qdata.FieldOperator) {
	s.fieldOperator = fo
}

func (s *SnapshotManager) InitializeIfRequired(ctx context.Context) {
	// No initialization required for NATS
}

func (s *SnapshotManager) CreateSnapshot(ctx context.Context) qdata.Snapshot {
	msg := &qprotobufs.ApiConfigCreateSnapshotRequest{}

	resp, err := s.core.Request(ctx, s.core.GetKeyGenerator().GetReadSubject(), msg)
	if err != nil {
		return nil
	}

	var response qprotobufs.ApiConfigCreateSnapshotResponse
	if err := resp.Payload.UnmarshalTo(&response); err != nil {
		return nil
	}

	if response.Status != qprotobufs.ApiConfigCreateSnapshotResponse_SUCCESS {
		return nil
	}

	return qsnapshot.FromPb(response.Snapshot)
}

func (s *SnapshotManager) RestoreSnapshot(ctx context.Context, ss qdata.Snapshot) {
	msg := &qprotobufs.ApiConfigRestoreSnapshotRequest{
		Snapshot: qsnapshot.ToPb(ss),
	}

	_, err := s.core.Request(ctx, s.core.GetKeyGenerator().GetWriteSubject(), msg)
	if err != nil {
		qlog.Error("Failed to restore snapshot: %v", err)
	}
}
