package qscripting

import (
	"context"

	"github.com/d5/tengo/v2"
	"github.com/d5/tengo/v2/stdlib"
)

type Executor interface {
	Execute(ctx context.Context, args map[string]ObjectConverterFn) (interface{}, error)
}

type executor struct {
	src string
}

func NewExecutor(src string) Executor {
	return &executor{
		src: src,
	}
}

func (me *executor) Execute(ctx context.Context, args map[string]ObjectConverterFn) (interface{}, error) {
	script := tengo.NewScript([]byte(me.src))
	script.SetImports(stdlib.GetModuleMap(stdlib.AllModuleNames()...))

	args["CTX"] = Context(ctx)

	for name, converter := range args {
		script.Add(name, converter())
	}

	return script.Run()
}
