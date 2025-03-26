package qpostgres

import "github.com/rqure/qlib/pkg/qdata"

func getTableForType(valueType qdata.ValueType) string {
	switch valueType {
	case qdata.Int:
		return "Ints"
	case qdata.Float:
		return "Floats"
	case qdata.String:
		return "Strings"
	case qdata.Bool:
		return "Bools"
	case qdata.BinaryFile:
		return "BinaryFiles"
	case qdata.EntityReference:
		return "EntityReferences"
	case qdata.Timestamp:
		return "Timestamps"
	case qdata.Choice:
		return "Choices"
	case qdata.EntityList:
		return "EntityLists"
	default:
		return ""
	}
}
