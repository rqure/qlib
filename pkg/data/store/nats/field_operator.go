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

func (me *FieldOperator) SetSchemaManager(sm data.SchemaManager) {
	me.schemaManager = sm
}

func (me *FieldOperator) SetEntityManager(em data.EntityManager) {
	me.entityManager = em
}

func (me *FieldOperator) SetNotificationPublisher(np data.NotificationPublisher) {
	me.notificationPublisher = np
}

func (me *FieldOperator) SetTransformer(t data.Transformer) {
	me.transformer = t
}

func (me *FieldOperator) Read(ctx context.Context, requests ...data.Request) {
	msg := &protobufs.ApiRuntimeDatabaseRequest{
		RequestType: protobufs.ApiRuntimeDatabaseRequest_READ,
		Requests:    make([]*protobufs.DatabaseRequest, len(requests)),
	}

	for i, r := range requests {
		msg.Requests[i] = request.ToPb(r)
	}

	resp, err := me.core.Request(ctx, me.core.GetKeyGenerator().GetReadSubject(), msg)
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

func (me *FieldOperator) Write(ctx context.Context, requests ...data.Request) {
	msg := &protobufs.ApiRuntimeDatabaseRequest{
		RequestType: protobufs.ApiRuntimeDatabaseRequest_WRITE,
		Requests:    make([]*protobufs.DatabaseRequest, len(requests)),
	}

	for i, r := range requests {
		msg.Requests[i] = request.ToPb(r)
	}

	resp, err := me.core.Request(ctx, me.core.GetKeyGenerator().GetWriteSubject(), msg)
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

func (me *FieldOperator) AuthorizedRead(ctx context.Context, authorizer data.FieldAuthorizer, requests ...data.Request) {
	me.Read(ctx, requests...)
}

func (me *FieldOperator) AuthorizedWrite(ctx context.Context, authorizer data.FieldAuthorizer, requests ...data.Request) {
	me.Write(ctx, requests...)
}
