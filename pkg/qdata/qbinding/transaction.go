package qbinding

import (
	"context"

	"github.com/rqure/qlib/pkg/qdata"
)

type Transaction struct {
	impl      qdata.FieldOperator
	readReqs  []qdata.Request
	writeReqs []qdata.Request
	tx        func(ctx context.Context)
}

func NewTransaction(fieldOperator qdata.FieldOperator) qdata.Transaction {
	return &Transaction{
		impl:      fieldOperator,
		readReqs:  []qdata.Request{},
		writeReqs: []qdata.Request{},
	}
}

func (me *Transaction) AsFieldOperator() qdata.FieldOperator {
	return me
}

func (me *Transaction) Read(ctx context.Context, reqs ...qdata.Request) {
	me.readReqs = append(me.readReqs, reqs...)
}

func (me *Transaction) Write(ctx context.Context, reqs ...qdata.Request) {
	cloned := make([]qdata.Request, len(reqs))
	for i, r := range reqs {
		cloned[i] = r.Clone()
	}
	me.writeReqs = append(me.writeReqs, cloned...)
}

func (me *Transaction) Commit(ctx context.Context) {
	if me.tx == nil {
		return
	}

	me.tx(context.WithValue(ctx, qdata.TransactionKey, me.AsFieldOperator()))

	if len(me.readReqs) > 0 {
		me.impl.Read(ctx, me.readReqs...)
	}

	if len(me.writeReqs) > 0 {
		me.impl.Write(ctx, me.writeReqs...)
	}

	me.readReqs = []qdata.Request{}
	me.writeReqs = []qdata.Request{}
}

func (me *Transaction) Prepare(tx func(ctx context.Context)) qdata.TransactionCommitter {
	me.tx = tx
	return me
}
