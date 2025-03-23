package qvalue

import "github.com/rqure/qlib/pkg/qdata"

type Value struct {
	qdata.ValueTypeProvider
	qdata.RawValueProvider

	qdata.ModifiableBinaryFileValue
	qdata.ModifiableBoolValue
	qdata.ModifiableChoiceValue
	qdata.ModifiableEntityListValue
	qdata.ModifiableIntValue
	qdata.ModifiableStringValue
	qdata.ModifiableTimestampValue
	qdata.ModifiableEntityReferenceValue
	qdata.ModifiableFloatValue
}
