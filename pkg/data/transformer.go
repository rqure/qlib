package data

import (
	"github.com/d5/tengo/v2"
	"github.com/d5/tengo/v2/stdlib"
	qlog "github.com/rqure/qlib/pkg/logging"
	pb "github.com/rqure/qlib/pkg/protobufs"
)

type ITransformer interface {
	Transform(*pb.Transformation, IField)
	ProcessPending()
}

type TengoTransformer struct {
	db ITengoDatabase
}

func NewTransformer(db IDatabase) ITransformer {
	return &TengoTransformer{
		db: NewTengoDatabase(db),
	}
}

func (t *TengoTransformer) Transform(transformation *pb.Transformation, field IField) {
	// Check if there is a script to execute
	if len(transformation.Raw) == 0 {
		return
	}

	script := tengo.NewScript([]byte(transformation.Raw))
	script.SetImports(stdlib.GetModuleMap(stdlib.AllModuleNames()...))
	script.Add("db", t.db.ToTengoMap())
	script.Add("field", NewTengoField(field).ToTengoMap())

	_, err := script.Run()
	if err != nil {
		qlog.Error("[Transformer::Transform] Failed to execute script: %v", err)
	}
}

func (t *TengoTransformer) ProcessPending() {
	for _, pending := range t.db.PopAvailableJobs() {
		_, err := pending.task.Call()
		if err != nil {
			qlog.Error("[Transformer::ProcessPending] Failed to execute script: %v", err)
		}
	}
}
