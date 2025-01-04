package transformer

import (
	"context"

	"github.com/d5/tengo/v2"
	"github.com/d5/tengo/v2/stdlib"
	"github.com/rqure/qlib/pkg/data"
	"github.com/rqure/qlib/pkg/data/binding"
	"github.com/rqure/qlib/pkg/log"
)

type Transformer struct {
	store *TengoStore
}

func NewTransformer(s data.Store) data.Transformer {
	return &Transformer{
		store: NewTengoStore(s),
	}
}

func (t *Transformer) Transform(ctx context.Context, src string, req data.Request) {
	// Check if there is a script to execute
	if len(src) == 0 {
		return
	}

	f := binding.NewField(&t.store.impl, req.GetEntityId(), req.GetFieldName())
	f.SetValue(req.GetValue())

	script := tengo.NewScript([]byte(src))
	script.SetImports(stdlib.GetModuleMap(stdlib.AllModuleNames()...))
	script.Add("STORE", t.store.ToTengoMap(ctx))
	script.Add("FIELD", NewTengoField(f).ToTengoMap(ctx))

	_, err := script.Run()
	if err != nil {
		log.Error("Failed to execute script: %v", err)
	}
}

func (t *Transformer) ProcessPending() {
	for _, pending := range t.store.PopAvailableJobs() {
		_, err := pending.task.Call()
		if err != nil {
			log.Error("Failed to execute script: %v", err)
		}
	}
}
