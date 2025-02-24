package web

import (
	"context"

	"github.com/rqure/qlib/pkg/data"
	"github.com/rqure/qlib/pkg/data/snapshot"
	"github.com/rqure/qlib/pkg/log"
	"github.com/rqure/qlib/pkg/protobufs"
	web "github.com/rqure/qlib/pkg/web/go"
	"google.golang.org/protobuf/types/known/anypb"
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
	msg := web.NewMessage()
	msg.Header = &protobufs.ApiHeader{}
	msg.Payload, _ = anypb.New(&protobufs.ApiConfigCreateSnapshotRequest{})

	response := s.core.SendAndWait(ctx, msg)
	if response == nil {
		log.Error("Received nil response")
		return nil
	}

	var resp protobufs.ApiConfigCreateSnapshotResponse
	if err := response.Payload.UnmarshalTo(&resp); err != nil {
		log.Error("Failed to unmarshal response: %v", err)
		return nil
	}

	if resp.Status != protobufs.ApiConfigCreateSnapshotResponse_SUCCESS {
		return nil
	}

	return snapshot.FromPb(resp.Snapshot)
}

func (s *SnapshotManager) RestoreSnapshot(ctx context.Context, ss data.Snapshot) {
	msg := web.NewMessage()
	msg.Header = &protobufs.ApiHeader{}
	msg.Payload, _ = anypb.New(&protobufs.ApiConfigRestoreSnapshotRequest{
		Snapshot: snapshot.ToPb(ss),
	})

	s.core.SendAndWait(ctx, msg)
}
