package qpostgres

import "github.com/rqure/qlib/pkg/qdata"

func getTableForType(valueType qdata.ValueType) string {
	switch valueType {
	case qdata.VTInt:
		return "Ints"
	case qdata.VTFloat:
		return "Floats"
	case qdata.VTString:
		return "Strings"
	case qdata.VTBool:
		return "Bools"
	case qdata.VTBinaryFile:
		return "BinaryFiles"
	case qdata.VTEntityReference:
		return "EntityReferences"
	case qdata.VTTimestamp:
		return "Timestamps"
	case qdata.VTChoice:
		return "Choices"
	case qdata.VTEntityList:
		return "EntityLists"
	default:
		return ""
	}
}
