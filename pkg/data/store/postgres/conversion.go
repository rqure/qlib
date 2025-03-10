package postgres

import (
	"time"

	"github.com/rqure/qlib/pkg/data"
	"github.com/rqure/qlib/pkg/data/field"
	"github.com/rqure/qlib/pkg/log"
	"github.com/rqure/qlib/pkg/protobufs"
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

func convertToValue(fieldType string, value interface{}) data.Value {
	if value == nil {
		return nil
	}

	v := field.NewValue()
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
			log.Error("Invalid timestamp type: %T", value)
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
		a, err := anypb.New(&protobufs.Int{})
		if err != nil {
			log.Error("Failed to create anypb: %v", err)
			return nil
		}
		return a
	case "Float":
		a, err := anypb.New(&protobufs.Float{})
		if err != nil {
			log.Error("Failed to create anypb: %v", err)
			return nil
		}
		return a
	case "String":
		a, err := anypb.New(&protobufs.String{})
		if err != nil {
			log.Error("Failed to create anypb: %v", err)
			return nil
		}
		return a
	case "Bool":
		a, err := anypb.New(&protobufs.Bool{})
		if err != nil {
			log.Error("Failed to create anypb: %v", err)
			return nil
		}
		return a
	case "BinaryFile":
		a, err := anypb.New(&protobufs.BinaryFile{})
		if err != nil {
			log.Error("Failed to create anypb: %v", err)
			return nil
		}
		return a
	case "EntityReference":
		a, err := anypb.New(&protobufs.EntityReference{})
		if err != nil {
			log.Error("Failed to create anypb: %v", err)
			return nil
		}
		return a
	case "Timestamp":
		a, err := anypb.New(&protobufs.Timestamp{
			Raw: timestamppb.New(time.Unix(0, 0)),
		})
		if err != nil {
			log.Error("Failed to create anypb: %v", err)
			return nil
		}
		return a
	case "Choice":
		a, err := anypb.New(&protobufs.Choice{})
		if err != nil {
			log.Error("Failed to create anypb: %v", err)
			return nil
		}
		return a
	case "EntityList":
		a, err := anypb.New(&protobufs.EntityList{})
		if err != nil {
			log.Error("Failed to create anypb: %v", err)
			return nil
		}
		return a
	default:
		return nil
	}
}

func fieldValueToInterface(v data.Value) interface{} {
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
		return v.GetChoice()
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
