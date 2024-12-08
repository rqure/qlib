package data

// Transformer calls scripts to transform field values of type Transformation
type Transformer interface {
	Transform(string, Request)
	ProcessPending()
}
