package transformer

import (
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

func (t *Transformer) Transform(src string, req data.Request) {
	// Check if there is a script to execute
	if len(src) == 0 {
		return
	}

	f := binding.New(t.store.impl, req.GetEntityId(), req.GetFieldName())

	script := tengo.NewScript([]byte(src))
	script.SetImports(stdlib.GetModuleMap(stdlib.AllModuleNames()...))
	script.Add("STORE", t.store.ToTengoMap())
	script.Add("FIELD", NewTengoField(f).ToTengoMap())

	_, err := script.Run()
	if err != nil {
		log.Error("[Transformer::Transform] Failed to execute script: %v", err)
	}
}

func (t *Transformer) ProcessPending() {
	for _, pending := range t.store.PopAvailableJobs() {
		_, err := pending.task.Call()
		if err != nil {
			log.Error("[Transformer::ProcessPending] Failed to execute script: %v", err)
		}
	}
}
