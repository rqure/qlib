package qscripting

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/d5/tengo/v2"
	"github.com/rqure/qlib/pkg/qdata"
)

type ObjectConverterFn func() tengo.Object

func tengoBool(b bool) tengo.Object {
	if b {
		return tengo.TrueValue
	}
	return tengo.FalseValue
}

// Helper functions to convert between Go and Tengo types
func toString(obj tengo.Object) (string, error) {
	if obj == tengo.UndefinedValue {
		return "", errors.New("undefined value")
	}
	if str, ok := tengo.ToString(obj); ok {
		return str, nil
	}
	return "", fmt.Errorf("expected string, got %s", obj.TypeName())
}

func toInt(obj tengo.Object) (int, error) {
	if obj == tengo.UndefinedValue {
		return 0, errors.New("undefined value")
	}
	if num, ok := obj.(*tengo.Int); ok {
		return int(num.Value), nil
	}
	return 0, fmt.Errorf("expected int, got %s", obj.TypeName())
}

func toEntityId(obj tengo.Object) (qdata.EntityId, error) {
	str, err := toString(obj)
	if err != nil {
		return "", err
	}
	return qdata.EntityId(str), nil
}

func toEntityType(obj tengo.Object) (qdata.EntityType, error) {
	str, err := toString(obj)
	if err != nil {
		return "", err
	}
	return qdata.EntityType(str), nil
}

func toFieldType(obj tengo.Object) (qdata.FieldType, error) {
	str, err := toString(obj)
	if err != nil {
		return "", err
	}
	return qdata.FieldType(str), nil
}

func toBool(obj tengo.Object) (bool, error) {
	if obj == tengo.UndefinedValue {
		return false, errors.New("undefined value")
	}
	if b, ok := obj.(*tengo.Bool); ok {
		return !b.IsFalsy(), nil
	}
	return false, fmt.Errorf("expected bool, got %s", obj.TypeName())
}

func toFloat(obj tengo.Object) (float64, error) {
	if obj == tengo.UndefinedValue {
		return 0, errors.New("undefined value")
	}
	if num, ok := obj.(*tengo.Float); ok {
		return num.Value, nil
	}
	return 0, fmt.Errorf("expected float, got %s", obj.TypeName())
}

func entityToTengo(entity *qdata.Entity) tengo.Object {
	if entity == nil {
		return tengo.UndefinedValue
	}

	return &tengo.Map{
		Value: map[string]tengo.Object{
			"entityId": &tengo.UserFunction{
				Name: "entityId",
				Value: func(args ...tengo.Object) (tengo.Object, error) {
					if len(args) != 0 {
						return nil, tengo.ErrWrongNumArguments
					}
					return &tengo.String{Value: entity.EntityId.AsString()}, nil
				},
			},
			"entityType": &tengo.UserFunction{
				Name: "entityType",
				Value: func(args ...tengo.Object) (tengo.Object, error) {
					if len(args) != 0 {
						return nil, tengo.ErrWrongNumArguments
					}
					return &tengo.String{Value: entity.EntityType.AsString()}, nil
				},
			},
			"fields": &tengo.UserFunction{
				Name: "fields",
				Value: func(args ...tengo.Object) (tengo.Object, error) {
					if len(args) != 0 {
						return nil, tengo.ErrWrongNumArguments
					}

					fields := make(map[string]tengo.Object)
					for fieldType, field := range entity.Fields {
						fields[fieldType.AsString()] = fieldToTengo(field)
					}

					fieldsMap := &tengo.Map{
						Value: fields,
					}
					return fieldsMap, nil
				},
			},
			"field": &tengo.UserFunction{
				Name: "field",
				Value: func(args ...tengo.Object) (tengo.Object, error) {
					if len(args) != 1 {
						return nil, tengo.ErrWrongNumArguments
					}

					fieldType, err := toFieldType(args[0])
					if err != nil {
						return nil, err
					}

					field := entity.Fields[fieldType]
					return fieldToTengo(field), nil
				},
			},
		},
	}
}

func fieldToTengo(field *qdata.Field) tengo.Object {
	if field == nil {
		return tengo.UndefinedValue
	}

	return &tengo.Map{
		Value: map[string]tengo.Object{
			"entityId": &tengo.UserFunction{
				Name: "entityId",
				Value: func(args ...tengo.Object) (tengo.Object, error) {
					if len(args) != 0 {
						return nil, tengo.ErrWrongNumArguments
					}
					return &tengo.String{Value: field.EntityId.AsString()}, nil
				},
			},
			"fieldType": &tengo.UserFunction{
				Name: "fieldType",
				Value: func(args ...tengo.Object) (tengo.Object, error) {
					if len(args) != 0 {
						return nil, tengo.ErrWrongNumArguments
					}
					return &tengo.String{Value: field.FieldType.AsString()}, nil
				},
			},
			"value": valueToTengo(field.Value),
			"writeTime": &tengo.UserFunction{
				Name: "writeTime",
				Value: func(args ...tengo.Object) (tengo.Object, error) {
					if len(args) != 0 {
						return nil, tengo.ErrWrongNumArguments
					}
					return &tengo.Time{Value: field.WriteTime.AsTime()}, nil
				},
			},
			"writerId": &tengo.UserFunction{
				Name: "writerId",
				Value: func(args ...tengo.Object) (tengo.Object, error) {
					if len(args) != 0 {
						return nil, tengo.ErrWrongNumArguments
					}
					return &tengo.String{Value: field.WriterId.AsString()}, nil
				},
			},
		},
	}
}

func valueToTengo(value *qdata.Value) tengo.Object {
	if value == nil {
		return tengo.UndefinedValue
	}

	return &tengo.Map{
		Value: map[string]tengo.Object{
			"type": &tengo.UserFunction{
				Name: "type",
				Value: func(args ...tengo.Object) (tengo.Object, error) {
					if len(args) != 0 {
						return nil, tengo.ErrWrongNumArguments
					}
					return &tengo.String{Value: value.Type().AsString()}, nil
				},
			},
			"isNil": &tengo.UserFunction{
				Name: "isNil",
				Value: func(args ...tengo.Object) (tengo.Object, error) {
					if len(args) != 0 {
						return nil, tengo.ErrWrongNumArguments
					}
					return tengoBool(value.IsNil()), nil
				},
			},
			"isInt": &tengo.UserFunction{
				Name: "isInt",
				Value: func(args ...tengo.Object) (tengo.Object, error) {
					if len(args) != 0 {
						return nil, tengo.ErrWrongNumArguments
					}
					return tengoBool(value.IsInt()), nil
				},
			},
			"isFloat": &tengo.UserFunction{
				Name: "isFloat",
				Value: func(args ...tengo.Object) (tengo.Object, error) {
					if len(args) != 0 {
						return nil, tengo.ErrWrongNumArguments
					}
					return tengoBool(value.IsFloat()), nil
				},
			},
			"isString": &tengo.UserFunction{
				Name: "isString",
				Value: func(args ...tengo.Object) (tengo.Object, error) {
					if len(args) != 0 {
						return nil, tengo.ErrWrongNumArguments
					}
					return tengoBool(value.IsString()), nil
				},
			},
			"isBool": &tengo.UserFunction{
				Name: "isBool",
				Value: func(args ...tengo.Object) (tengo.Object, error) {
					if len(args) != 0 {
						return nil, tengo.ErrWrongNumArguments
					}
					return tengoBool(value.IsBool()), nil
				},
			},
			"isBinaryFile": &tengo.UserFunction{
				Name: "isBinaryFile",
				Value: func(args ...tengo.Object) (tengo.Object, error) {
					if len(args) != 0 {
						return nil, tengo.ErrWrongNumArguments
					}
					return tengoBool(value.IsBinaryFile()), nil
				},
			},
			"isEntityReference": &tengo.UserFunction{
				Name: "isEntityReference",
				Value: func(args ...tengo.Object) (tengo.Object, error) {
					if len(args) != 0 {
						return nil, tengo.ErrWrongNumArguments
					}
					return tengoBool(value.IsEntityReference()), nil
				},
			},
			"isTimestamp": &tengo.UserFunction{
				Name: "isTimestamp",
				Value: func(args ...tengo.Object) (tengo.Object, error) {
					if len(args) != 0 {
						return nil, tengo.ErrWrongNumArguments
					}
					return tengoBool(value.IsTimestamp()), nil
				},
			},
			"isChoice": &tengo.UserFunction{
				Name: "isChoice",
				Value: func(args ...tengo.Object) (tengo.Object, error) {
					if len(args) != 0 {
						return nil, tengo.ErrWrongNumArguments
					}
					return tengoBool(value.IsChoice()), nil
				},
			},
			"isEntityList": &tengo.UserFunction{
				Name: "isEntityList",
				Value: func(args ...tengo.Object) (tengo.Object, error) {
					if len(args) != 0 {
						return nil, tengo.ErrWrongNumArguments
					}
					return tengoBool(value.IsEntityList()), nil
				},
			},
			"getInt": &tengo.UserFunction{
				Name: "getInt",
				Value: func(args ...tengo.Object) (tengo.Object, error) {
					if len(args) != 0 {
						return nil, tengo.ErrWrongNumArguments
					}

					if !value.IsInt() {
						return nil, fmt.Errorf("value is not an int")
					}

					return &tengo.Int{Value: int64(value.GetInt())}, nil
				},
			},
			"getFloat": &tengo.UserFunction{
				Name: "getFloat",
				Value: func(args ...tengo.Object) (tengo.Object, error) {
					if len(args) != 0 {
						return nil, tengo.ErrWrongNumArguments
					}

					if !value.IsFloat() {
						return nil, fmt.Errorf("value is not a float")
					}

					return &tengo.Float{Value: value.GetFloat()}, nil
				},
			},
			"getString": &tengo.UserFunction{
				Name: "getString",
				Value: func(args ...tengo.Object) (tengo.Object, error) {
					if len(args) != 0 {
						return nil, tengo.ErrWrongNumArguments
					}

					if !value.IsString() {
						return nil, fmt.Errorf("value is not a string")
					}

					return &tengo.String{Value: value.GetString()}, nil
				},
			},
			"getBool": &tengo.UserFunction{
				Name: "getBool",
				Value: func(args ...tengo.Object) (tengo.Object, error) {
					if len(args) != 0 {
						return nil, tengo.ErrWrongNumArguments
					}

					if !value.IsBool() {
						return nil, fmt.Errorf("value is not a bool")
					}

					return tengoBool(value.GetBool()), nil
				},
			},
			"getBinaryFile": &tengo.UserFunction{
				Name: "getBinaryFile",
				Value: func(args ...tengo.Object) (tengo.Object, error) {
					if len(args) != 0 {
						return nil, tengo.ErrWrongNumArguments
					}

					if !value.IsBinaryFile() {
						return nil, fmt.Errorf("value is not a binary file")
					}

					return &tengo.String{Value: value.GetBinaryFile()}, nil
				},
			},
			"getEntityReference": &tengo.UserFunction{
				Name: "getEntityReference",
				Value: func(args ...tengo.Object) (tengo.Object, error) {
					if len(args) != 0 {
						return nil, tengo.ErrWrongNumArguments
					}

					if !value.IsEntityReference() {
						return nil, fmt.Errorf("value is not an entity reference")
					}

					return &tengo.String{Value: string(value.GetEntityReference())}, nil
				},
			},
			"getTimestamp": &tengo.UserFunction{
				Name: "getTimestamp",
				Value: func(args ...tengo.Object) (tengo.Object, error) {
					if len(args) != 0 {
						return nil, tengo.ErrWrongNumArguments
					}

					if !value.IsTimestamp() {
						return nil, fmt.Errorf("value is not a timestamp")
					}

					return &tengo.Time{Value: value.GetTimestamp()}, nil
				},
			},
			"getChoice": &tengo.UserFunction{
				Name: "getChoice",
				Value: func(args ...tengo.Object) (tengo.Object, error) {
					if len(args) != 0 {
						return nil, tengo.ErrWrongNumArguments
					}

					if !value.IsChoice() {
						return nil, fmt.Errorf("value is not a choice")
					}

					return &tengo.Int{Value: int64(value.GetChoice())}, nil
				},
			},
			"getEntityList": &tengo.UserFunction{
				Name: "getEntityList",
				Value: func(args ...tengo.Object) (tengo.Object, error) {
					if len(args) != 0 {
						return nil, tengo.ErrWrongNumArguments
					}

					if !value.IsEntityList() {
						return nil, fmt.Errorf("value is not an entity list")
					}

					entityList := value.GetEntityList()
					entities := make([]tengo.Object, len(entityList))
					for i, entityId := range entityList {
						entities[i] = &tengo.String{Value: string(entityId)}
					}

					return &tengo.Array{Value: entities}, nil
				},
			},
			"setInt": &tengo.UserFunction{
				Name: "setInt",
				Value: func(args ...tengo.Object) (tengo.Object, error) {
					if len(args) != 1 {
						return nil, tengo.ErrWrongNumArguments
					}

					if !value.IsInt() {
						return nil, fmt.Errorf("value is not an int")
					}

					intVal, err := toInt(args[0])
					if err != nil {
						return nil, err
					}

					value.SetInt(intVal)
					return tengo.UndefinedValue, nil
				},
			},
			"setFloat": &tengo.UserFunction{
				Name: "setFloat",
				Value: func(args ...tengo.Object) (tengo.Object, error) {
					if len(args) != 1 {
						return nil, tengo.ErrWrongNumArguments
					}

					if !value.IsFloat() {
						return nil, fmt.Errorf("value is not a float")
					}

					floatVal, err := toFloat(args[0])
					if err != nil {
						return nil, err
					}

					value.SetFloat(floatVal)
					return tengo.UndefinedValue, nil
				},
			},
			"setString": &tengo.UserFunction{
				Name: "setString",
				Value: func(args ...tengo.Object) (tengo.Object, error) {
					if len(args) != 1 {
						return nil, tengo.ErrWrongNumArguments
					}

					if !value.IsString() {
						return nil, fmt.Errorf("value is not a string")
					}

					strVal, err := toString(args[0])
					if err != nil {
						return nil, err
					}

					value.SetString(strVal)
					return tengo.UndefinedValue, nil
				},
			},
			"setBool": &tengo.UserFunction{
				Name: "setBool",
				Value: func(args ...tengo.Object) (tengo.Object, error) {
					if len(args) != 1 {
						return nil, tengo.ErrWrongNumArguments
					}

					if !value.IsBool() {
						return nil, fmt.Errorf("value is not a bool")
					}

					boolVal, err := toBool(args[0])
					if err != nil {
						return nil, err
					}

					value.SetBool(boolVal)
					return tengo.UndefinedValue, nil
				},
			},
			"setBinaryFile": &tengo.UserFunction{
				Name: "setBinaryFile",
				Value: func(args ...tengo.Object) (tengo.Object, error) {
					if len(args) != 1 {
						return nil, tengo.ErrWrongNumArguments
					}

					if !value.IsBinaryFile() {
						return nil, fmt.Errorf("value is not a binary file")
					}

					strVal, err := toString(args[0])
					if err != nil {
						return nil, err
					}

					value.SetBinaryFile(strVal)
					return tengo.UndefinedValue, nil
				},
			},
			"setEntityReference": &tengo.UserFunction{
				Name: "setEntityReference",
				Value: func(args ...tengo.Object) (tengo.Object, error) {
					if len(args) != 1 {
						return nil, tengo.ErrWrongNumArguments
					}

					if !value.IsEntityReference() {
						return nil, fmt.Errorf("value is not an entity reference")
					}

					strVal, err := toString(args[0])
					if err != nil {
						return nil, err
					}

					value.SetEntityReference(qdata.EntityId(strVal))
					return tengo.UndefinedValue, nil
				},
			},
			"setTimestamp": &tengo.UserFunction{
				Name: "setTimestamp",
				Value: func(args ...tengo.Object) (tengo.Object, error) {
					if len(args) != 1 {
						return nil, tengo.ErrWrongNumArguments
					}

					if !value.IsTimestamp() {
						return nil, fmt.Errorf("value is not a timestamp")
					}

					strVal, err := toString(args[0])
					if err != nil {
						return nil, err
					}

					t, err := time.Parse(time.RFC3339, strVal)
					if err != nil {
						return nil, fmt.Errorf("invalid timestamp format: %v", err)
					}

					value.SetTimestamp(t)
					return tengo.UndefinedValue, nil
				},
			},
			"setChoice": &tengo.UserFunction{
				Name: "setChoice",
				Value: func(args ...tengo.Object) (tengo.Object, error) {
					if len(args) != 1 {
						return nil, tengo.ErrWrongNumArguments
					}

					if !value.IsChoice() {
						return nil, fmt.Errorf("value is not a choice")
					}

					intVal, err := toInt(args[0])
					if err != nil {
						return nil, err
					}

					value.SetChoice(intVal)
					return tengo.UndefinedValue, nil
				},
			},
			"setEntityList": &tengo.UserFunction{
				Name: "setEntityList",
				Value: func(args ...tengo.Object) (tengo.Object, error) {
					if len(args) != 1 {
						return nil, tengo.ErrWrongNumArguments
					}

					if !value.IsEntityList() {
						return nil, fmt.Errorf("value is not an entity list")
					}

					entityList, ok := args[0].(*tengo.Array)
					if !ok {
						return nil, fmt.Errorf("expected array, got %s", args[0].TypeName())
					}

					entityIds := make([]qdata.EntityId, len(entityList.Value))
					for i, item := range entityList.Value {
						entityId, err := toEntityId(item)
						if err != nil {
							return nil, err
						}
						entityIds[i] = entityId
					}

					value.SetEntityList(entityIds)
					return tengo.UndefinedValue, nil
				},
			},
		},
	}
}

func entitySchemaToTengo(schema *qdata.EntitySchema) tengo.Object {
	if schema == nil {
		return tengo.UndefinedValue
	}

	return &tengo.Map{
		Value: map[string]tengo.Object{
			"entityType": &tengo.UserFunction{
				Name: "entityType",
				Value: func(args ...tengo.Object) (tengo.Object, error) {
					if len(args) != 0 {
						return nil, tengo.ErrWrongNumArguments
					}
					return &tengo.String{Value: schema.EntityType.AsString()}, nil
				},
			},
			"fields": &tengo.UserFunction{
				Name: "fields",
				Value: func(args ...tengo.Object) (tengo.Object, error) {
					if len(args) != 0 {
						return nil, tengo.ErrWrongNumArguments
					}

					fields := make(map[string]tengo.Object)
					for fieldType, fieldSchema := range schema.Fields {
						fields[fieldType.AsString()] = fieldSchemaToTengo(fieldSchema)
					}

					fieldsMap := &tengo.Map{
						Value: fields,
					}

					return fieldsMap, nil
				},
			},
			"field": &tengo.UserFunction{
				Name: "field",
				Value: func(args ...tengo.Object) (tengo.Object, error) {
					if len(args) != 1 {
						return nil, tengo.ErrWrongNumArguments
					}

					fieldType, err := toFieldType(args[0])
					if err != nil {
						return nil, err
					}

					fieldSchema := schema.Fields[fieldType]
					return fieldSchemaToTengo(fieldSchema), nil
				},
			},
		},
	}
}

func fieldSchemaToTengo(schema *qdata.FieldSchema) tengo.Object {
	if schema == nil {
		return tengo.UndefinedValue
	}

	return &tengo.Map{
		Value: map[string]tengo.Object{
			"entityType": &tengo.UserFunction{
				Name: "entityType",
				Value: func(args ...tengo.Object) (tengo.Object, error) {
					if len(args) != 0 {
						return nil, tengo.ErrWrongNumArguments
					}
					return &tengo.String{Value: schema.EntityType.AsString()}, nil
				},
			},
			"fieldType": &tengo.UserFunction{
				Name: "fieldType",
				Value: func(args ...tengo.Object) (tengo.Object, error) {
					if len(args) != 0 {
						return nil, tengo.ErrWrongNumArguments
					}
					return &tengo.String{Value: schema.FieldType.AsString()}, nil
				},
			},
			"valueType": &tengo.UserFunction{
				Name: "valueType",
				Value: func(args ...tengo.Object) (tengo.Object, error) {
					if len(args) != 0 {
						return nil, tengo.ErrWrongNumArguments
					}
					return &tengo.String{Value: schema.ValueType.AsString()}, nil
				},
			},
			"rank": &tengo.UserFunction{
				Name: "rank",
				Value: func(args ...tengo.Object) (tengo.Object, error) {
					if len(args) != 0 {
						return nil, tengo.ErrWrongNumArguments
					}
					return &tengo.Int{Value: int64(schema.Rank)}, nil
				},
			},
			"readPermissions": &tengo.UserFunction{
				Name: "readPermissions",
				Value: func(args ...tengo.Object) (tengo.Object, error) {
					if len(args) != 0 {
						return nil, tengo.ErrWrongNumArguments
					}

					readPerms := make([]tengo.Object, len(schema.ReadPermissions))
					for i, p := range schema.ReadPermissions {
						readPerms[i] = &tengo.String{Value: p.AsString()}
					}

					return &tengo.Array{Value: readPerms}, nil
				},
			},
			"writePermissions": &tengo.UserFunction{
				Name: "writePermissions",
				Value: func(args ...tengo.Object) (tengo.Object, error) {
					if len(args) != 0 {
						return nil, tengo.ErrWrongNumArguments
					}

					writePerms := make([]tengo.Object, len(schema.WritePermissions))
					for i, p := range schema.WritePermissions {
						writePerms[i] = &tengo.String{Value: p.AsString()}
					}

					return &tengo.Array{Value: writePerms}, nil
				},
			},
			"choices": &tengo.UserFunction{
				Name: "choices",
				Value: func(args ...tengo.Object) (tengo.Object, error) {
					if len(args) != 0 {
						return nil, tengo.ErrWrongNumArguments
					}

					choices := make([]tengo.Object, len(schema.Choices))
					for i, choice := range schema.Choices {
						choices[i] = &tengo.String{Value: choice}
					}

					return &tengo.Array{Value: choices}, nil
				},
			},
		},
	}
}

func createRequestFromMap(requestMap *tengo.Map) (*qdata.Request, error) {
	entityIdObj := requestMap.Value["entityId"]
	fieldTypeObj := requestMap.Value["fieldType"]
	valueObj := requestMap.Value["value"]

	if entityIdObj == nil || fieldTypeObj == nil || valueObj == nil {
		return nil, errors.New("request must contain entityId, fieldType, and value")
	}

	entityId, err := toEntityId(entityIdObj)
	if err != nil {
		return nil, err
	}

	fieldType, err := toFieldType(fieldTypeObj)
	if err != nil {
		return nil, err
	}

	// Create a basic request
	req := new(qdata.Request).Init(entityId, fieldType)

	// Handle value - it should be a tengo map with type and value fields
	valueMap, ok := valueObj.(*tengo.Map)
	if !ok {
		return nil, fmt.Errorf("value must be a map, got %s", valueObj.TypeName())
	}

	typeObj := valueMap.Value["type"]
	actualValueObj := valueMap.Value["value"]

	if typeObj == nil || actualValueObj == nil {
		return nil, errors.New("value map must contain type and value keys")
	}

	valueType, err := toString(typeObj)
	if err != nil {
		return nil, err
	}

	switch qdata.ValueType(valueType) {
	case qdata.VTInt:
		intVal, err := toInt(actualValueObj)
		if err != nil {
			return nil, err
		}
		req.Value.FromInt(intVal)

	case qdata.VTFloat:
		floatVal, ok := actualValueObj.(*tengo.Float)
		if !ok {
			return nil, fmt.Errorf("expected float, got %s", actualValueObj.TypeName())
		}
		req.Value.FromFloat(floatVal.Value)

	case qdata.VTString:
		strVal, err := toString(actualValueObj)
		if err != nil {
			return nil, err
		}
		req.Value.FromString(strVal)

	case qdata.VTBool:
		boolVal, err := toBool(actualValueObj)
		if err != nil {
			return nil, err
		}
		req.Value.FromBool(boolVal)

	case qdata.VTBinaryFile:
		strVal, err := toString(actualValueObj)
		if err != nil {
			return nil, err
		}
		req.Value.FromBinaryFile(strVal)

	case qdata.VTEntityReference:
		strVal, err := toString(actualValueObj)
		if err != nil {
			return nil, err
		}
		req.Value.FromEntityReference(qdata.EntityId(strVal))

	case qdata.VTTimestamp:
		// For timestamp, we'll need to parse the string representation
		strVal, err := toString(actualValueObj)
		if err != nil {
			return nil, err
		}
		t, err := time.Parse(time.RFC3339, strVal)
		if err != nil {
			return nil, fmt.Errorf("invalid timestamp format: %v", err)
		}
		req.Value.FromTimestamp(t)

	case qdata.VTChoice:
		intVal, err := toInt(actualValueObj)
		if err != nil {
			return nil, err
		}
		req.Value.FromChoice(intVal)

	case qdata.VTEntityList:
		array, ok := actualValueObj.(*tengo.Array)
		if !ok {
			return nil, fmt.Errorf("expected array, got %s", actualValueObj.TypeName())
		}

		entityList := make([]qdata.EntityId, 0, len(array.Value))
		for _, item := range array.Value {
			str, err := toString(item)
			if err != nil {
				return nil, err
			}
			entityList = append(entityList, qdata.EntityId(str))
		}
		req.Value.FromEntityList(entityList)

	default:
		return nil, fmt.Errorf("unsupported value type: %s", valueType)
	}

	return req, nil
}

// Context creates a Tengo object representing the context
func Context(ctx context.Context) ObjectConverterFn {
	return func() tengo.Object {
		return &tengo.ImmutableMap{
			Value: map[string]tengo.Object{
				"_ctx": &Any{Value: ctx},
			},
		}
	}
}

// Helper function to extract context from the first argument
func extractContext(args []tengo.Object) (context.Context, error) {
	if len(args) == 0 {
		return nil, tengo.ErrWrongNumArguments
	}

	ctxMap, ok := args[0].(*tengo.Map)
	if !ok {
		return nil, fmt.Errorf("expected context map, got %s", args[0].TypeName())
	}

	ctxObj := ctxMap.Value["_ctx"]
	if ctxObj == nil {
		return nil, errors.New("context not found in map")
	}

	anyObj, ok := ctxObj.(*Any)
	if !ok {
		return nil, fmt.Errorf("expected Any object, got %s", ctxObj.TypeName())
	}

	ctx, ok := anyObj.Value.(context.Context)
	if !ok {
		return nil, fmt.Errorf("expected context.Context, got %T", anyObj.Value)
	}

	return ctx, nil
}

func Store(s qdata.StoreInteractor) ObjectConverterFn {
	return func() tengo.Object {
		storeMap := &tengo.Map{
			Value: map[string]tengo.Object{
				"createEntity": &tengo.UserFunction{
					Name: "createEntity",
					Value: func(args ...tengo.Object) (tengo.Object, error) {
						if len(args) != 4 {
							return nil, tengo.ErrWrongNumArguments
						}

						ctx, err := extractContext(args[:1])
						if err != nil {
							return nil, err
						}

						eType, err := toEntityType(args[1])
						if err != nil {
							return nil, err
						}

						parentId, err := toEntityId(args[2])
						if err != nil {
							return nil, err
						}

						name, err := toString(args[3])
						if err != nil {
							return nil, err
						}

						entityId := s.CreateEntity(ctx, eType, parentId, name)
						return &tengo.String{Value: string(entityId)}, nil
					},
				},
				"getEntity": &tengo.UserFunction{
					Name: "getEntity",
					Value: func(args ...tengo.Object) (tengo.Object, error) {
						if len(args) != 2 {
							return nil, tengo.ErrWrongNumArguments
						}

						ctx, err := extractContext(args[:1])
						if err != nil {
							return nil, err
						}

						entityId, err := toEntityId(args[1])
						if err != nil {
							return nil, err
						}

						entity := s.GetEntity(ctx, entityId)
						return entityToTengo(entity), nil
					},
				},
				"deleteEntity": &tengo.UserFunction{
					Name: "deleteEntity",
					Value: func(args ...tengo.Object) (tengo.Object, error) {
						if len(args) != 2 {
							return nil, tengo.ErrWrongNumArguments
						}

						ctx, err := extractContext(args[:1])
						if err != nil {
							return nil, err
						}

						entityId, err := toEntityId(args[1])
						if err != nil {
							return nil, err
						}

						s.DeleteEntity(ctx, entityId)
						return tengo.TrueValue, nil
					},
				},
				"findEntities": &tengo.UserFunction{
					Name: "findEntities",
					Value: func(args ...tengo.Object) (tengo.Object, error) {
						if len(args) != 2 {
							return nil, tengo.ErrWrongNumArguments
						}

						ctx, err := extractContext(args[:1])
						if err != nil {
							return nil, err
						}

						entityType, err := toEntityType(args[1])
						if err != nil {
							return nil, err
						}

						result := s.FindEntities(entityType)

						entities := make([]tengo.Object, 0)

						for result.Next(ctx) {
							entityId := result.Get()
							entities = append(entities, &tengo.String{Value: string(entityId)})
						}

						return &tengo.Array{Value: entities}, nil
					},
				},
				"getEntityTypes": &tengo.UserFunction{
					Name: "getEntityTypes",
					Value: func(args ...tengo.Object) (tengo.Object, error) {
						if len(args) != 1 {
							return nil, tengo.ErrWrongNumArguments
						}

						ctx, err := extractContext(args[:1])
						if err != nil {
							return nil, err
						}

						result := s.GetEntityTypes()

						entityTypes := make([]tengo.Object, 0)

						for result.Next(ctx) {
							entityType := result.Get()
							entityTypes = append(entityTypes, &tengo.String{Value: string(entityType)})
						}

						return &tengo.Array{Value: entityTypes}, nil
					},
				},
				"entityExists": &tengo.UserFunction{
					Name: "entityExists",
					Value: func(args ...tengo.Object) (tengo.Object, error) {
						if len(args) != 2 {
							return nil, tengo.ErrWrongNumArguments
						}

						ctx, err := extractContext(args[:1])
						if err != nil {
							return nil, err
						}

						entityId, err := toEntityId(args[1])
						if err != nil {
							return nil, err
						}

						exists := s.EntityExists(ctx, entityId)
						if exists {
							return tengo.TrueValue, nil
						}
						return tengo.FalseValue, nil
					},
				},
				"fieldExists": &tengo.UserFunction{
					Name: "fieldExists",
					Value: func(args ...tengo.Object) (tengo.Object, error) {
						if len(args) != 3 {
							return nil, tengo.ErrWrongNumArguments
						}

						ctx, err := extractContext(args[:1])
						if err != nil {
							return nil, err
						}

						entityType, err := toEntityType(args[1])
						if err != nil {
							return nil, err
						}

						fieldType, err := toFieldType(args[2])
						if err != nil {
							return nil, err
						}

						exists := s.FieldExists(ctx, entityType, fieldType)
						if exists {
							return tengo.TrueValue, nil
						}
						return tengo.FalseValue, nil
					},
				},
				"getEntitySchema": &tengo.UserFunction{
					Name: "getEntitySchema",
					Value: func(args ...tengo.Object) (tengo.Object, error) {
						if len(args) != 2 {
							return nil, tengo.ErrWrongNumArguments
						}

						ctx, err := extractContext(args[:1])
						if err != nil {
							return nil, err
						}

						entityType, err := toEntityType(args[1])
						if err != nil {
							return nil, err
						}

						schema := s.GetEntitySchema(ctx, entityType)
						return entitySchemaToTengo(schema), nil
					},
				},
				"getFieldSchema": &tengo.UserFunction{
					Name: "getFieldSchema",
					Value: func(args ...tengo.Object) (tengo.Object, error) {
						if len(args) != 3 {
							return nil, tengo.ErrWrongNumArguments
						}

						ctx, err := extractContext(args[:1])
						if err != nil {
							return nil, err
						}

						entityType, err := toEntityType(args[1])
						if err != nil {
							return nil, err
						}

						fieldType, err := toFieldType(args[2])
						if err != nil {
							return nil, err
						}

						schema := s.GetFieldSchema(ctx, entityType, fieldType)
						return fieldSchemaToTengo(schema), nil
					},
				},
				"read": &tengo.UserFunction{
					Name: "read",
					Value: func(args ...tengo.Object) (tengo.Object, error) {
						if len(args) < 2 {
							return nil, tengo.ErrWrongNumArguments
						}

						ctx, err := extractContext(args[:1])
						if err != nil {
							return nil, err
						}

						requests := make([]*qdata.Request, 0, len(args)-1)

						for _, arg := range args[1:] {
							reqMap, ok := arg.(*tengo.Map)
							if !ok {
								return nil, fmt.Errorf("expected map, got %s", arg.TypeName())
							}

							req, err := createRequestFromMap(reqMap)
							if err != nil {
								return nil, err
							}

							requests = append(requests, req)
						}

						s.Read(ctx, requests...)

						// Return array of success status
						results := make([]tengo.Object, len(requests))
						for i, req := range requests {
							if req.Success {
								results[i] = tengo.TrueValue
							} else {
								results[i] = tengo.FalseValue
							}
						}

						return &tengo.Array{Value: results}, nil
					},
				},
				"write": &tengo.UserFunction{
					Name: "write",
					Value: func(args ...tengo.Object) (tengo.Object, error) {
						if len(args) < 2 {
							return nil, tengo.ErrWrongNumArguments
						}

						ctx, err := extractContext(args[:1])
						if err != nil {
							return nil, err
						}

						requests := make([]*qdata.Request, 0, len(args)-1)

						for _, arg := range args[1:] {
							reqMap, ok := arg.(*tengo.Map)
							if !ok {
								return nil, fmt.Errorf("expected map, got %s", arg.TypeName())
							}

							req, err := createRequestFromMap(reqMap)
							if err != nil {
								return nil, err
							}

							requests = append(requests, req)
						}

						s.Write(ctx, requests...)

						// Return array of success status
						results := make([]tengo.Object, len(requests))
						for i, req := range requests {
							if req.Success {
								results[i] = tengo.TrueValue
							} else {
								results[i] = tengo.FalseValue
							}
						}

						return &tengo.Array{Value: results}, nil
					},
				},
				"initializeSchema": &tengo.UserFunction{
					Name: "initializeSchema",
					Value: func(args ...tengo.Object) (tengo.Object, error) {
						if len(args) != 1 {
							return nil, tengo.ErrWrongNumArguments
						}

						ctx, err := extractContext(args[:1])
						if err != nil {
							return nil, err
						}

						s.InitializeSchema(ctx)
						return tengo.TrueValue, nil
					},
				},
				"createSnapshot": &tengo.UserFunction{
					Name: "createSnapshot",
					Value: func(args ...tengo.Object) (tengo.Object, error) {
						if len(args) != 1 {
							return nil, tengo.ErrWrongNumArguments
						}

						ctx, err := extractContext(args[:1])
						if err != nil {
							return nil, err
						}

						s.CreateSnapshot(ctx)
						return tengo.TrueValue, nil
					},
				},
			},
		}

		return storeMap
	}
}

func Factory() ObjectConverterFn {
	return func() tengo.Object {
		return &tengo.Map{
			Value: map[string]tengo.Object{
				"newIntValue": &tengo.UserFunction{
					Name: "newIntValue",
					Value: func(args ...tengo.Object) (tengo.Object, error) {
						if len(args) != 1 {
							return nil, tengo.ErrWrongNumArguments
						}

						intVal, err := toInt(args[0])
						if err != nil {
							return nil, err
						}

						return valueToTengo(qdata.NewInt(intVal)), nil
					},
				},
				"newFloatValue": &tengo.UserFunction{
					Name: "newFloatValue",
					Value: func(args ...tengo.Object) (tengo.Object, error) {
						if len(args) != 1 {
							return nil, tengo.ErrWrongNumArguments
						}

						floatObj, ok := args[0].(*tengo.Float)
						if !ok {
							return nil, fmt.Errorf("expected float, got %s", args[0].TypeName())
						}

						return valueToTengo(qdata.NewFloat(floatObj.Value)), nil
					},
				},
				"newStringValue": &tengo.UserFunction{
					Name: "newStringValue",
					Value: func(args ...tengo.Object) (tengo.Object, error) {
						if len(args) != 1 {
							return nil, tengo.ErrWrongNumArguments
						}

						str, err := toString(args[0])
						if err != nil {
							return nil, err
						}

						return valueToTengo(qdata.NewString(str)), nil
					},
				},
				"newBoolValue": &tengo.UserFunction{
					Name: "newBoolValue",
					Value: func(args ...tengo.Object) (tengo.Object, error) {
						if len(args) != 1 {
							return nil, tengo.ErrWrongNumArguments
						}

						b, err := toBool(args[0])
						if err != nil {
							return nil, err
						}

						return valueToTengo(qdata.NewBool(b)), nil
					},
				},
				"newEntityReferenceValue": &tengo.UserFunction{
					Name: "newEntityReferenceValue",
					Value: func(args ...tengo.Object) (tengo.Object, error) {
						if len(args) != 1 {
							return nil, tengo.ErrWrongNumArguments
						}

						entityId, err := toEntityId(args[0])
						if err != nil {
							return nil, err
						}

						return valueToTengo(qdata.NewEntityReference(entityId)), nil
					},
				},
				"newTimestampValue": &tengo.UserFunction{
					Name: "newTimestampValue",
					Value: func(args ...tengo.Object) (tengo.Object, error) {
						if len(args) == 0 {
							// Current time if no args
							return valueToTengo(qdata.NewTimestamp(time.Now())), nil
						}

						if len(args) != 1 {
							return nil, tengo.ErrWrongNumArguments
						}

						timeStr, err := toString(args[0])
						if err != nil {
							return nil, err
						}

						t, err := time.Parse(time.RFC3339, timeStr)
						if err != nil {
							return nil, fmt.Errorf("invalid timestamp format: %v", err)
						}

						return valueToTengo(qdata.NewTimestamp(t)), nil
					},
				},
				"newBinaryFileValue": &tengo.UserFunction{
					Name: "newBinaryFileValue",
					Value: func(args ...tengo.Object) (tengo.Object, error) {
						if len(args) != 1 {
							return nil, tengo.ErrWrongNumArguments
						}

						data, err := toString(args[0])
						if err != nil {
							return nil, err
						}

						return valueToTengo(qdata.NewBinaryFile(data)), nil
					},
				},
				"newChoiceValue": &tengo.UserFunction{
					Name: "newChoiceValue",
					Value: func(args ...tengo.Object) (tengo.Object, error) {
						if len(args) != 1 {
							return nil, tengo.ErrWrongNumArguments
						}

						choice, err := toInt(args[0])
						if err != nil {
							return nil, err
						}

						return valueToTengo(qdata.NewChoice(choice)), nil
					},
				},
				"newEntityListValue": &tengo.UserFunction{
					Name: "newEntityListValue",
					Value: func(args ...tengo.Object) (tengo.Object, error) {
						if len(args) == 0 {
							// Empty list if no args
							return valueToTengo(qdata.NewEntityList()), nil
						}

						// If single arg is array
						if len(args) == 1 {
							if arr, ok := args[0].(*tengo.Array); ok {
								entityIds := make([]qdata.EntityId, 0, len(arr.Value))
								for _, item := range arr.Value {
									str, err := toString(item)
									if err != nil {
										return nil, err
									}
									entityIds = append(entityIds, qdata.EntityId(str))
								}
								return valueToTengo(qdata.NewEntityList(entityIds)), nil
							}
						}

						// Otherwise treat all args as entity IDs
						entityIds := make([]qdata.EntityId, 0, len(args))
						for _, arg := range args {
							str, err := toString(arg)
							if err != nil {
								return nil, err
							}
							entityIds = append(entityIds, qdata.EntityId(str))
						}

						return valueToTengo(qdata.NewEntityList(entityIds)), nil
					},
				},
				"newRequest": &tengo.UserFunction{
					Name: "newRequest",
					Value: func(args ...tengo.Object) (tengo.Object, error) {
						if len(args) != 3 {
							return nil, tengo.ErrWrongNumArguments
						}

						entityId, err := toEntityId(args[0])
						if err != nil {
							return nil, err
						}

						fieldType, err := toFieldType(args[1])
						if err != nil {
							return nil, err
						}

						valueMap, ok := args[2].(*tengo.Map)
						if !ok {
							return nil, fmt.Errorf("value must be a map, got %s", args[2].TypeName())
						}

						return &tengo.Map{
							Value: map[string]tengo.Object{
								"entityId":  &tengo.String{Value: string(entityId)},
								"fieldType": &tengo.String{Value: string(fieldType)},
								"value":     valueMap,
							},
						}, nil
					},
				},
			},
		}
	}
}
