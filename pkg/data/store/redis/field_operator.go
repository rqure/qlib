package redis

import (
	"context"
	"encoding/base64"
	"strings"

	"github.com/rqure/qlib/pkg/data"
	"github.com/rqure/qlib/pkg/data/field"
	"github.com/rqure/qlib/pkg/data/request"
	"github.com/rqure/qlib/pkg/log"
	"github.com/rqure/qlib/pkg/protobufs"
	"google.golang.org/protobuf/proto"
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

func (me *FieldOperator) SetSchemaManager(manager data.SchemaManager) {
	me.schemaManager = manager
}

func (me *FieldOperator) SetEntityManager(manager data.EntityManager) {
	me.entityManager = manager
}

func (me *FieldOperator) SetNotificationPublisher(publisher data.NotificationPublisher) {
	me.notificationPublisher = publisher
}

func (me *FieldOperator) SetTransformer(transformer data.Transformer) {
	me.transformer = transformer
}

func (me *FieldOperator) Read(ctx context.Context, requests ...data.Request) {
	for _, r := range requests {
		r.SetSuccessful(false)

		indirectField, indirectEntity := me.resolveIndirection(ctx, r.GetFieldName(), r.GetEntityId())
		if indirectField == "" || indirectEntity == "" {
			continue
		}

		fieldBytes, err := me.core.GetClient().Get(ctx, me.core.GetKeyGen().GetFieldKey(indirectField, indirectEntity)).Bytes()
		if err != nil {
			continue
		}

		fieldPb := &protobufs.DatabaseField{}
		if err := proto.Unmarshal(fieldBytes, fieldPb); err != nil {
			log.Error("Failed to unmarshal field: %v", err)
			continue
		}

		f := field.FromFieldPb(fieldPb)
		r.SetValue(f.GetValue())

		writeTime := f.GetWriteTime()
		writer := f.GetWriter()
		r.SetWriteTime(&writeTime)
		r.SetWriter(&writer)

		r.SetSuccessful(true)
	}
}

func (me *FieldOperator) Write(ctx context.Context, requests ...data.Request) {
	for _, r := range requests {
		r.SetSuccessful(false)

		indirectField, indirectEntity := me.resolveIndirection(ctx, r.GetFieldName(), r.GetEntityId())
		if indirectField == "" || indirectEntity == "" {
			continue
		}

		oldReq := request.New().
			SetEntityId(r.GetEntityId()).
			SetFieldName(r.GetFieldName())
		me.Read(ctx, oldReq)

		// Handle transformations
		if oldReq.IsSuccessful() && oldReq.GetValue().IsTransformation() && !r.GetValue().IsTransformation() {
			src := oldReq.GetValue().GetTransformation()
			me.transformer.Transform(ctx, src, r)
			r.SetValue(oldReq.GetValue())
		} else if oldReq.IsSuccessful() && r.GetWriteOpt() == data.WriteChanges {
			if proto.Equal(field.ToAnyPb(oldReq.GetValue()), field.ToAnyPb(r.GetValue())) {
				r.SetSuccessful(true)
				continue
			}
		}

		f := field.FromRequest(r)
		fieldPb := field.ToFieldPb(f)
		fieldBytes, err := proto.Marshal(fieldPb)
		if err != nil {
			log.Error("Failed to marshal field: %v", err)
			continue
		}

		err = me.core.GetClient().Set(ctx, me.core.GetKeyGen().GetFieldKey(indirectField, indirectEntity),
			base64.StdEncoding.EncodeToString(fieldBytes), 0).Err()
		if err != nil {
			log.Error("Failed to save field: %v", err)
			continue
		}

		me.notificationPublisher.PublishNotifications(ctx, r, oldReq)
		r.SetSuccessful(true)
	}
}

func (me *FieldOperator) resolveIndirection(ctx context.Context, indirectField, entityId string) (string, string) {
	fields := strings.Split(indirectField, "->")

	if len(fields) == 1 {
		return indirectField, entityId
	}

	for _, f := range fields[:len(fields)-1] {
		r := request.New().SetEntityId(entityId).SetFieldName(f)
		me.Read(ctx, r)

		if r.IsSuccessful() {
			v := r.GetValue()
			if v.IsEntityReference() {
				entityId = v.GetEntityReference()
				if entityId == "" {
					return "", ""
				}
				continue
			}
			return "", ""
		}

		// Try parent/child resolution
		entity := me.entityManager.GetEntity(ctx, entityId)
		if entity == nil {
			return "", ""
		}

		if parent := me.entityManager.GetEntity(ctx, entity.GetParentId()); parent != nil && parent.GetName() == f {
			entityId = parent.GetId()
			continue
		}

		foundChild := false
		for _, childId := range entity.GetChildrenIds() {
			if child := me.entityManager.GetEntity(ctx, childId); child != nil && child.GetName() == f {
				entityId = child.GetId()
				foundChild = true
				break
			}
		}

		if !foundChild {
			return "", ""
		}
	}

	return fields[len(fields)-1], entityId
}
