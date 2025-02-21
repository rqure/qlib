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
	case "protobufs.Int":
		return "Ints"
	case "protobufs.Float":
		return "Floats"
	case "protobufs.String":
		return "Strings"
	case "protobufs.Bool":
		return "Bools"
	case "protobufs.BinaryFile":
		return "BinaryFiles"
	case "protobufs.EntityReference":
		return "EntityReferences"
	case "protobufs.Timestamp":
		return "Timestamps"
	case "protobufs.Transformation":
		return "Transformations"
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
	case "protobufs.Int":
		v.SetInt(value)
	case "protobufs.Float":
		v.SetFloat(value)
	case "protobufs.String":
		v.SetString(value)
	case "protobufs.Bool":
		v.SetBool(value)
	case "protobufs.BinaryFile":
		v.SetBinaryFile(value)
	case "protobufs.EntityReference":
		v.SetEntityReference(value)
	case "protobufs.Timestamp":
		switch t := value.(type) {
		case time.Time:
			v.SetTimestamp(t)
		default:
			log.Error("Invalid timestamp type: %T", value)
			return nil
		}
	case "protobufs.Transformation":
		v.SetTransformation(value)
	default:
		return nil
	}

	return v
}

func fieldTypeToProtoType(fieldType string) *anypb.Any {
	switch fieldType {
	case "protobufs.Int":
		a, err := anypb.New(&protobufs.Int{})
		if err != nil {
			log.Error("Failed to create anypb: %v", err)
			return nil
		}
		return a
	case "protobufs.Float":
		a, err := anypb.New(&protobufs.Float{})
		if err != nil {
			log.Error("Failed to create anypb: %v", err)
			return nil
		}
		return a
	case "protobufs.String":
		a, err := anypb.New(&protobufs.String{})
		if err != nil {
			log.Error("Failed to create anypb: %v", err)
			return nil
		}
		return a
	case "protobufs.Bool":
		a, err := anypb.New(&protobufs.Bool{})
		if err != nil {
			log.Error("Failed to create anypb: %v", err)
			return nil
		}
		return a
	case "protobufs.BinaryFile":
		a, err := anypb.New(&protobufs.BinaryFile{})
		if err != nil {
			log.Error("Failed to create anypb: %v", err)
			return nil
		}
		return a
	case "protobufs.EntityReference":
		a, err := anypb.New(&protobufs.EntityReference{})
		if err != nil {
			log.Error("Failed to create anypb: %v", err)
			return nil
		}
		return a
	case "protobufs.Timestamp":
		a, err := anypb.New(&protobufs.Timestamp{
			Raw: timestamppb.New(time.Unix(0, 0)),
		})
		if err != nil {
			log.Error("Failed to create anypb: %v", err)
			return nil
		}
		return a
	case "protobufs.Transformation":
		a, err := anypb.New(&protobufs.Transformation{})
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
	case v.IsTransformation():
		return v.GetTransformation()
	default:
		return nil
	}
}
