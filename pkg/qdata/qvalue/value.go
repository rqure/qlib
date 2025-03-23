package qvalue

import "github.com/rqure/qlib/pkg/qdata"

type Value struct {
	qdata.ValueTypeProvider

	qdata.RawProvider
	qdata.IntProvider
	qdata.FloatProvider
	qdata.StringProvider
	qdata.BoolProvider
	qdata.BinaryFileProvider
	qdata.EntityReferenceProvider
	qdata.TimestampProvider
	qdata.ChoiceProvider
	qdata.EntityListProvider

	qdata.RawReceiver
	qdata.IntReceiver
	qdata.FloatReceiver
	qdata.StringReceiver
	qdata.BoolReceiver
	qdata.BinaryFileReceiver
	qdata.EntityReferenceReceiver
	qdata.TimestampReceiver
	qdata.ChoiceReceiver
	qdata.EntityListReceiver
}
