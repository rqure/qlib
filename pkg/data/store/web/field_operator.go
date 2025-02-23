package web

import (
	"context"

	"github.com/rqure/qlib/pkg/data"
	"github.com/rqure/qlib/pkg/data/field"
	"github.com/rqure/qlib/pkg/data/request"
	"github.com/rqure/qlib/pkg/log"
	"github.com/rqure/qlib/pkg/protobufs"
	web "github.com/rqure/qlib/pkg/web/go"
	"google.golang.org/protobuf/types/known/anypb"
)

type FieldOperator struct {
	core                  Core
	schemaManager         data.SchemaManager
	entityManager         data.EntityManager
	notificationPublisher data.NotificationPublisher
	transformer           data.Transformer
}

func NewFieldOperator(core Core) data.ModifiableFieldOperator {
	return &FieldOperator{core: core}
}

func (f *FieldOperator) SetSchemaManager(sm data.SchemaManager) {
	f.schemaManager = sm
}

func (f *FieldOperator) SetEntityManager(em data.EntityManager) {
	f.entityManager = em
}

func (f *FieldOperator) SetNotificationPublisher(np data.NotificationPublisher) {
	f.notificationPublisher = np
}

func (f *FieldOperator) SetTransformer(t data.Transformer) {
	f.transformer = t
}

func (f *FieldOperator) Read(ctx context.Context, requests ...data.Request) {
	msg := web.NewMessage()
	msg.Header = &protobufs.WebHeader{}

	dbRequests := make([]*protobufs.DatabaseRequest, len(requests))
	for i, r := range requests {
		dbRequests[i] = request.ToPb(r)
	}

	msg.Payload, _ = anypb.New(&protobufs.WebRuntimeDatabaseRequest{
		RequestType: protobufs.WebRuntimeDatabaseRequest_READ,
		Requests:    dbRequests,
	})

	response := f.core.SendAndWait(ctx, msg)
	if response == nil {
		log.Error("Received nil response")
		return
	}

	var resp protobufs.WebRuntimeDatabaseResponse
	if err := response.Payload.UnmarshalTo(&resp); err != nil {
		log.Error("Failed to unmarshal response: %v", err)
		return
	}

	for i, r := range resp.Response {
		if i >= len(requests) {
			break
		}
		requests[i].SetValue(field.FromAnyPb(r.Value))
		requests[i].SetSuccessful(r.Success)
		if r.WriteTime != nil {
			wt := r.WriteTime.Raw.AsTime()
			requests[i].SetWriteTime(&wt)
		}
		if r.WriterId != nil {
			wr := r.WriterId.Raw
			requests[i].SetWriter(&wr)
		}
	}
}

func (f *FieldOperator) Write(ctx context.Context, requests ...data.Request) {
	msg := web.NewMessage()
	msg.Header = &protobufs.WebHeader{}

	dbRequests := make([]*protobufs.DatabaseRequest, len(requests))
	for i, r := range requests {
		dbRequests[i] = request.ToPb(r)
	}

	msg.Payload, _ = anypb.New(&protobufs.WebRuntimeDatabaseRequest{
		RequestType: protobufs.WebRuntimeDatabaseRequest_WRITE,
		Requests:    dbRequests,
	})

	response := f.core.SendAndWait(ctx, msg)
	if response == nil {
		log.Error("Received nil response")
		return
	}

	var resp protobufs.WebRuntimeDatabaseResponse
	if err := response.Payload.UnmarshalTo(&resp); err != nil {
		log.Error("Failed to unmarshal response: %v", err)
		return
	}

	for i, r := range resp.Response {
		if i >= len(requests) {
			break
		}
		requests[i].SetSuccessful(r.Success)
	}

	// Handle notifications
	for _, r := range requests {
		if r.IsSuccessful() {
			oldReq := request.New().
				SetEntityId(r.GetEntityId()).
				SetFieldName(r.GetFieldName())
			f.notificationPublisher.PublishNotifications(ctx, r, oldReq)
		}
	}
}
