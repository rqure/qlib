package qpostgres

import (
	"time"

	"github.com/rqure/qlib/pkg/qdata"
	"github.com/rqure/qlib/pkg/qdata/qfield"
	"github.com/rqure/qlib/pkg/qlog"
	"github.com/rqure/qlib/pkg/qprotobufs"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func getTableForType(fieldType string) string {
	switch fieldType {
	case "Int":
		return "Ints"
	case "Float":
		return "Floats"
	case "String":
		return "Strings"
	case "Bool":
		return "Bools"
	case "BinaryFile":
		return "BinaryFiles"
	case "EntityReference":
		return "EntityReferences"
	case "Timestamp":
		return "Timestamps"
	case "Choice":
		return "Choices"
	case "EntityList":
		return "EntityLists"
	default:
		return ""
	}
}

func convertToValue(fieldType string, value interface{}) qdata.Value {
	if value == nil {
		return nil
	}

	v := qfield.NewValue()
	switch fieldType {
	case "Int":
		v.SetInt(value)
	case "Float":
		v.SetFloat(value)
	case "String":
		v.SetString(value)
	case "Bool":
		v.SetBool(value)
	case "BinaryFile":
		v.SetBinaryFile(value)
	case "EntityReference":
		v.SetEntityReference(value)
	case "Timestamp":
		switch t := value.(type) {
		case time.Time:
			v.SetTimestamp(t)
		default:
			qlog.Error("Invalid timestamp type: %T", value)
			return nil
		}
	case "Choice":
		v.SetChoice(value)
	case "EntityList":
		v.SetEntityList(value)
	default:
		return nil
	}

	return v
}

func fieldTypeToProtoType(fieldType string) *anypb.Any {
	switch fieldType {
	case "Int":
		a, err := anypb.New(&qprotobufs.Int{})
		if err != nil {
			qlog.Error("Failed to create anypb: %v", err)
			return nil
		}
		return a
	case "Float":
		a, err := anypb.New(&qprotobufs.Float{})
		if err != nil {
			qlog.Error("Failed to create anypb: %v", err)
			return nil
		}
		return a
	case "String":
		a, err := anypb.New(&qprotobufs.String{})
		if err != nil {
			qlog.Error("Failed to create anypb: %v", err)
			return nil
		}
		return a
	case "Bool":
		a, err := anypb.New(&qprotobufs.Bool{})
		if err != nil {
			qlog.Error("Failed to create anypb: %v", err)
			return nil
		}
		return a
	case "BinaryFile":
		a, err := anypb.New(&qprotobufs.BinaryFile{})
		if err != nil {
			qlog.Error("Failed to create anypb: %v", err)
			return nil
		}
		return a
	case "EntityReference":
		a, err := anypb.New(&qprotobufs.EntityReference{})
		if err != nil {
			qlog.Error("Failed to create anypb: %v", err)
			return nil
		}
		return a
	case "Timestamp":
		a, err := anypb.New(&qprotobufs.Timestamp{
			Raw: timestamppb.New(time.Unix(0, 0)),
		})
		if err != nil {
			qlog.Error("Failed to create anypb: %v", err)
			return nil
		}
		return a
	case "Choice":
		a, err := anypb.New(&qprotobufs.Choice{})
		if err != nil {
			qlog.Error("Failed to create anypb: %v", err)
			return nil
		}
		return a
	case "EntityList":
		a, err := anypb.New(&qprotobufs.EntityList{})
		if err != nil {
			qlog.Error("Failed to create anypb: %v", err)
			return nil
		}
		return a
	default:
		return nil
	}
}

func fieldValueToInterface(v qdata.Value) interface{} {
	if v == nil || v.IsNil() {
		return nil
	}

	switch {
	case v.IsInt():
		return v.GetInt()
	case v.IsFloat():
		return v.GetFloat()
	case v.IsString():
		return v.GetString()
	case v.IsBool():
		return v.GetBool()
	case v.IsBinaryFile():
		return v.GetBinaryFile()
	case v.IsEntityReference():
		return v.GetEntityReference()
	case v.IsTimestamp():
		return v.GetTimestamp()
	case v.IsChoice():
		return v.GetChoice().Index()
	case v.IsEntityList():
		list := v.GetEntityList()
		if list == nil || len(list.GetEntities()) == 0 {
			return []string{} // Return empty array instead of nil
		}
		return list.GetEntities()
	default:
		return nil
	}
}
