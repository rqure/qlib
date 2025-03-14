package qnats

import (
	"context"

	"github.com/rqure/qlib/pkg/qapp"
	"github.com/rqure/qlib/pkg/qdata"
	"github.com/rqure/qlib/pkg/qdata/qfield"
	"github.com/rqure/qlib/pkg/qdata/qquery"
	"github.com/rqure/qlib/pkg/qdata/qrequest"
	"github.com/rqure/qlib/pkg/qlog"
	"github.com/rqure/qlib/pkg/qprotobufs"
)

type FieldOperator struct {
	core                  Core
	schemaManager         qdata.SchemaManager
	entityManager         qdata.EntityManager
	notificationPublisher qdata.NotificationPublisher
	clientId              *string
}

func NewFieldOperator(core Core) qdata.ModifiableFieldOperator {
	return &FieldOperator{core: core}
}

func (me *FieldOperator) SetSchemaManager(sm qdata.SchemaManager) {
	me.schemaManager = sm
}

func (me *FieldOperator) SetEntityManager(em qdata.EntityManager) {
	me.entityManager = em
}

func (me *FieldOperator) SetNotificationPublisher(np qdata.NotificationPublisher) {
	me.notificationPublisher = np
}

func (me *FieldOperator) Read(ctx context.Context, requests ...qdata.Request) {
	msg := &qprotobufs.ApiRuntimeDatabaseRequest{
		RequestType: qprotobufs.ApiRuntimeDatabaseRequest_READ,
		Requests:    make([]*qprotobufs.DatabaseRequest, len(requests)),
	}

	for i, r := range requests {
		msg.Requests[i] = qrequest.ToPb(r)
	}

	resp, err := me.core.Request(ctx, me.core.GetKeyGenerator().GetReadSubject(), msg)
	if err != nil {
		return
	}

	var response qprotobufs.ApiRuntimeDatabaseResponse
	if err := resp.Payload.UnmarshalTo(&response); err != nil {
		return
	}

	for i, r := range response.Response {
		if i >= len(requests) {
			break
		}
		requests[i].SetValue(qfield.FromAnyPb(&r.Value))
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

func (me *FieldOperator) Write(ctx context.Context, requests ...qdata.Request) {
	msg := &qprotobufs.ApiRuntimeDatabaseRequest{
		RequestType: qprotobufs.ApiRuntimeDatabaseRequest_WRITE,
		Requests:    make([]*qprotobufs.DatabaseRequest, len(requests)),
	}

	for i, r := range requests {
		writer := r.GetWriter()
		if writer == nil || *writer == "" {
			if me.clientId == nil {
				clients := qquery.New(&qdata.LimitedStore{
					FieldOperator:         me,
					EntityManager:         me.entityManager,
					NotificationPublisher: me.notificationPublisher,
					SchemaManager:         me.schemaManager,
				}).Select().
					From("Client").
					Where("Name").Equals(qapp.GetName()).
					Execute(ctx)

				if len(clients) == 0 {
					qlog.Error("Failed to get client id")
				} else {
					if len(clients) > 1 {
						qlog.Warn("Multiple clients found: %v", clients)
					}

					clientId := clients[0].GetId()
					me.clientId = &clientId
				}
			}

			if me.clientId != nil {
				r.SetWriter(me.clientId)
			}
		}
		msg.Requests[i] = qrequest.ToPb(r)
	}

	resp, err := me.core.Request(ctx, me.core.GetKeyGenerator().GetWriteSubject(), msg)
	if err != nil {
		return
	}

	var response qprotobufs.ApiRuntimeDatabaseResponse
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
