package qvalue

import "github.com/rqure/qlib/pkg/qdata"

func New(vType qdata.ValueType) *qdata.Value {
	switch vType {
	case qdata.IntType:
		return NewInt()
	case qdata.FloatType:
		return NewFloat()
	case qdata.StringType:
		return NewString()
	case qdata.EntityReferenceType:
		return NewEntityReference()
	case qdata.TimestampType:
		return NewTimestamp()
	case qdata.BoolType:
		return NewBool()
	case qdata.BinaryFileType:
		return NewBinaryFile()
	case qdata.ChoiceType:
		return NewChoice()
	case qdata.EntityListType:
		return NewEntityList()
	}

	return nil
}
