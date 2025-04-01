package qscripting

import (
	"context"

	"github.com/d5/tengo/v2"
	"github.com/d5/tengo/v2/stdlib"
)

type Executor interface {
	Execute(ctx context.Context, args map[string]ObjectConverterFn) error
}

type executor struct {
	src      string
	compiled *tengo.Compiled
}

func NewExecutor(src string) Executor {
	return &executor{
		src: src,
	}
}

func (me *executor) Init(src string) {
	me.src = src
	me.compiled = nil
}

func (me *executor) Execute(ctx context.Context, args map[string]ObjectConverterFn) error {
	args["CTX"] = Context(ctx)

	if me.compiled == nil {
		script := tengo.NewScript([]byte(me.src))
		script.SetImports(stdlib.GetModuleMap(stdlib.AllModuleNames()...))

		for name, converter := range args {
			script.Add(name, converter())
		}

		compiled, err := script.Compile()
		if err != nil {
			return err
		}

		me.compiled = compiled
	}

	for name, converter := range args {
		if err := me.compiled.Set(name, converter()); err != nil {
			return err
		}
	}

	return me.compiled.RunContext(ctx)
}
