package data

import "context"

// Transformer calls scripts to transform field values of type Transformation
type Transformer interface {
	Transform(context.Context, string, Request)
	ProcessPending()
}
