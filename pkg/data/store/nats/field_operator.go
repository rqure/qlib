package nats

import (
	"context"

	"github.com/rqure/qlib/pkg/data"
	"github.com/rqure/qlib/pkg/data/field"
	"github.com/rqure/qlib/pkg/data/request"
	"github.com/rqure/qlib/pkg/protobufs"
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
	msg := &protobufs.ApiRuntimeDatabaseRequest{
		RequestType: protobufs.ApiRuntimeDatabaseRequest_READ,
		Requests:    make([]*protobufs.DatabaseRequest, len(requests)),
	}

	for i, r := range requests {
		msg.Requests[i] = request.ToPb(r)
	}

	resp, err := f.core.Request(ctx, f.core.GetKeyGenerator().GetFieldSubject(requests[0].GetFieldName(), requests[0].GetEntityId()), msg)
	if err != nil {
		return
	}

	var response protobufs.ApiRuntimeDatabaseResponse
	if err := resp.Payload.UnmarshalTo(&response); err != nil {
		return
	}

	for i, r := range response.Response {
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
	msg := &protobufs.ApiRuntimeDatabaseRequest{
		RequestType: protobufs.ApiRuntimeDatabaseRequest_WRITE,
		Requests:    make([]*protobufs.DatabaseRequest, len(requests)),
	}

	for i, r := range requests {
		msg.Requests[i] = request.ToPb(r)
	}

	resp, err := f.core.Request(ctx, f.core.GetKeyGenerator().GetFieldSubject(requests[0].GetFieldName(), requests[0].GetEntityId()), msg)
	if err != nil {
		return
	}

	var response protobufs.ApiRuntimeDatabaseResponse
	if err := resp.Payload.UnmarshalTo(&response); err != nil {
		return
	}

	for i, r := range response.Response {
		if i >= len(requests) {
			break
		}
		requests[i].SetSuccessful(r.Success)
	}
}
