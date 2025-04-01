package qscripting

import (
	"context"

	"github.com/d5/tengo/v2"
	"github.com/d5/tengo/v2/stdlib"
)

type Executor interface {
	Execute(ctx context.Context, in map[string]ObjectConverterFn) (map[string]interface{}, error)
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

func (me *executor) Execute(ctx context.Context, in map[string]ObjectConverterFn) (map[string]interface{}, error) {
	in["CTX"] = Context(ctx)
	in["OUT"] = Output()

	if me.compiled == nil {
		script := tengo.NewScript([]byte(me.src))
		script.SetImports(stdlib.GetModuleMap(stdlib.AllModuleNames()...))

		for name, converter := range in {
			script.Add(name, converter())
		}

		compiled, err := script.Compile()
		if err != nil {
			return nil, err
		}

		me.compiled = compiled
	}

	for name, converter := range in {
		if err := me.compiled.Set(name, converter()); err != nil {
			return nil, err
		}
	}

	err := me.compiled.RunContext(ctx)
	if err != nil {
		return nil, err
	}

	return me.compiled.Get("OUT").Map(), nil
}
