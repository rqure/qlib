package transformer

import (
	"github.com/d5/tengo/v2"
	"github.com/rqure/qlib/pkg/data"
)

type TengoField struct {
	b data.FieldBinding
}

func NewTengoField(b data.FieldBinding) *TengoField {
	return &TengoField{b: b}
}

func (tf *TengoField) ToTengoMap() tengo.Object {
	return &tengo.Map{
		Value: map[string]tengo.Object{
			"readInt": &tengo.UserFunction{
				Name:  "readInt",
				Value: tf.ReadInt,
			},
			"readFloat": &tengo.UserFunction{
				Name:  "readFloat",
				Value: tf.ReadFloat,
			},
			"readString": &tengo.UserFunction{
				Name:  "readString",
				Value: tf.ReadString,
			},
			"readBool": &tengo.UserFunction{
				Name:  "readBool",
				Value: tf.ReadBool,
			},
			"readBinaryFile": &tengo.UserFunction{
				Name:  "readBinaryFile",
				Value: tf.ReadBinaryFile,
			},
			"readEntityReference": &tengo.UserFunction{
				Name:  "readEntityReference",
				Value: tf.ReadEntityReference,
			},
			"readTimestamp": &tengo.UserFunction{
				Name:  "readTimestamp",
				Value: tf.ReadTimestamp,
			},
			"getWriteTime": &tengo.UserFunction{
				Name:  "getWriteTime",
				Value: tf.GetWriteTime,
			},
			"getWriter": &tengo.UserFunction{
				Name:  "getWriter",
				Value: tf.GetWriter,
			},
			"getInt": &tengo.UserFunction{
				Name:  "getInt",
				Value: tf.GetInt,
			},
			"getFloat": &tengo.UserFunction{
				Name:  "getFloat",
				Value: tf.GetFloat,
			},
			"getString": &tengo.UserFunction{
				Name:  "getString",
				Value: tf.GetString,
			},
			"getBool": &tengo.UserFunction{
				Name:  "getBool",
				Value: tf.GetBool,
			},
			"getBinaryFile": &tengo.UserFunction{
				Name:  "getBinaryFile",
				Value: tf.GetBinaryFile,
			},
			"getEntityReference": &tengo.UserFunction{
				Name:  "getEntityReference",
				Value: tf.GetEntityReference,
			},
			"getTimestamp": &tengo.UserFunction{
				Name:  "getTimestamp",
				Value: tf.GetTimestamp,
			},
			"getId": &tengo.UserFunction{
				Name:  "getId",
				Value: tf.GetEntityId,
			},
			"getName": &tengo.UserFunction{
				Name:  "getName",
				Value: tf.GetFieldName,
			},
			"writeInt": &tengo.UserFunction{
				Name:  "writeInt",
				Value: tf.WriteInt,
			},
			"writeFloat": &tengo.UserFunction{
				Name:  "writeFloat",
				Value: tf.WriteFloat,
			},
			"writeString": &tengo.UserFunction{
				Name:  "writeString",
				Value: tf.WriteString,
			},
			"writeBool": &tengo.UserFunction{
				Name:  "writeBool",
				Value: tf.WriteBool,
			},
			"writeBinaryFile": &tengo.UserFunction{
				Name:  "writeBinaryFile",
				Value: tf.WriteBinaryFile,
			},
			"writeEntityReference": &tengo.UserFunction{
				Name:  "writeEntityReference",
				Value: tf.WriteEntityReference,
			},
			"writeTimestamp": &tengo.UserFunction{
				Name:  "writeTimestamp",
				Value: tf.WriteTimestamp,
			},
		},
	}
}

func (tf *TengoField) ReadInt(...tengo.Object) (tengo.Object, error) {
	return &tengo.Int{Value: tf.b.ReadInt()}, nil
}

func (tf *TengoField) ReadFloat(...tengo.Object) (tengo.Object, error) {
	return &tengo.Float{Value: tf.b.ReadFloat()}, nil
}

func (tf *TengoField) ReadString(...tengo.Object) (tengo.Object, error) {
	return &tengo.String{Value: tf.b.ReadString()}, nil
}

func (tf *TengoField) ReadBool(...tengo.Object) (tengo.Object, error) {
	if tf.b.ReadBool() {
		return tengo.TrueValue, nil
	}

	return tengo.FalseValue, nil
}

func (tf *TengoField) ReadBinaryFile(...tengo.Object) (tengo.Object, error) {
	return &tengo.String{Value: tf.b.ReadBinaryFile()}, nil
}

func (tf *TengoField) ReadEntityReference(...tengo.Object) (tengo.Object, error) {
	return &tengo.String{Value: tf.b.ReadEntityReference()}, nil
}

func (tf *TengoField) ReadTimestamp(...tengo.Object) (tengo.Object, error) {
	return &tengo.Time{Value: tf.b.ReadTimestamp()}, nil
}

func (tf *TengoField) GetInt(...tengo.Object) (tengo.Object, error) {
	return &tengo.Int{Value: tf.b.GetInt()}, nil
}

func (tf *TengoField) GetFloat(...tengo.Object) (tengo.Object, error) {
	return &tengo.Float{Value: tf.b.GetFloat()}, nil
}

func (tf *TengoField) GetString(...tengo.Object) (tengo.Object, error) {
	return &tengo.String{Value: tf.b.GetString()}, nil
}

func (tf *TengoField) GetBool(...tengo.Object) (tengo.Object, error) {
	if tf.b.GetBool() {
		return tengo.TrueValue, nil
	}

	return tengo.FalseValue, nil
}

func (tf *TengoField) GetBinaryFile(...tengo.Object) (tengo.Object, error) {
	return &tengo.String{Value: tf.b.GetBinaryFile()}, nil
}

func (tf *TengoField) GetEntityReference(...tengo.Object) (tengo.Object, error) {
	return &tengo.String{Value: tf.b.GetEntityReference()}, nil
}

func (tf *TengoField) GetTimestamp(...tengo.Object) (tengo.Object, error) {
	return &tengo.Time{Value: tf.b.GetTimestamp()}, nil
}

func (tf *TengoField) GetWriteTime(...tengo.Object) (tengo.Object, error) {
	return &tengo.Time{Value: tf.b.GetWriteTime()}, nil
}

func (tf *TengoField) GetWriter(...tengo.Object) (tengo.Object, error) {
	return &tengo.String{Value: tf.b.GetWriter()}, nil
}

func (tf *TengoField) GetEntityId(...tengo.Object) (tengo.Object, error) {
	return &tengo.String{Value: tf.b.GetEntityId()}, nil
}

func (tf *TengoField) GetFieldName(...tengo.Object) (tengo.Object, error) {
	return &tengo.String{Value: tf.b.GetFieldName()}, nil
}

func (tf *TengoField) WriteInt(args ...tengo.Object) (tengo.Object, error) {
	if len(args) < 1 {
		return nil, tengo.ErrWrongNumArguments
	}

	i, ok := tengo.ToInt(args[0])
	if !ok {
		return nil, &tengo.ErrInvalidArgumentType{
			Name:     "i",
			Expected: "int",
			Found:    args[0].TypeName(),
		}
	}

	tf.b.WriteInt(i)
	return tengo.UndefinedValue, nil
}

func (tf *TengoField) WriteFloat(args ...tengo.Object) (tengo.Object, error) {
	if len(args) < 1 {
		return nil, tengo.ErrWrongNumArguments
	}

	f, ok := tengo.ToFloat64(args[0])
	if !ok {
		return nil, &tengo.ErrInvalidArgumentType{
			Name:     "f",
			Expected: "float",
			Found:    args[0].TypeName(),
		}
	}

	tf.b.WriteFloat(f)
	return tengo.UndefinedValue, nil
}

func (tf *TengoField) WriteString(args ...tengo.Object) (tengo.Object, error) {
	if len(args) < 1 {
		return nil, tengo.ErrWrongNumArguments
	}

	s, ok := tengo.ToString(args[0])
	if !ok {
		return nil, &tengo.ErrInvalidArgumentType{
			Name:     "s",
			Expected: "string",
			Found:    args[0].TypeName(),
		}
	}

	tf.b.WriteString(s)
	return tengo.UndefinedValue, nil
}

func (tf *TengoField) WriteBool(args ...tengo.Object) (tengo.Object, error) {
	if len(args) < 1 {
		return nil, tengo.ErrWrongNumArguments
	}

	b, ok := tengo.ToBool(args[0])
	if !ok {
		return nil, &tengo.ErrInvalidArgumentType{
			Name:     "b",
			Expected: "bool",
			Found:    args[0].TypeName(),
		}
	}

	tf.b.WriteBool(b)
	return tengo.UndefinedValue, nil
}

func (tf *TengoField) WriteBinaryFile(args ...tengo.Object) (tengo.Object, error) {
	if len(args) < 1 {
		return nil, tengo.ErrWrongNumArguments
	}

	b, ok := tengo.ToString(args[0])
	if !ok {
		return nil, &tengo.ErrInvalidArgumentType{
			Name:     "b",
			Expected: "string",
			Found:    args[0].TypeName(),
		}
	}

	tf.b.WriteBinaryFile(b)
	return tengo.UndefinedValue, nil
}

func (tf *TengoField) WriteEntityReference(args ...tengo.Object) (tengo.Object, error) {
	if len(args) < 1 {
		return nil, tengo.ErrWrongNumArguments
	}

	e, ok := tengo.ToString(args[0])
	if !ok {
		return nil, &tengo.ErrInvalidArgumentType{
			Name:     "e",
			Expected: "string",
			Found:    args[0].TypeName(),
		}
	}

	tf.b.WriteEntityReference(e)
	return tengo.UndefinedValue, nil
}

func (tf *TengoField) WriteTimestamp(args ...tengo.Object) (tengo.Object, error) {
	if len(args) < 1 {
		return nil, tengo.ErrWrongNumArguments
	}

	t, ok := tengo.ToTime(args[0])
	if !ok {
		return nil, &tengo.ErrInvalidArgumentType{
			Name:     "t",
			Expected: "time",
			Found:    args[0].TypeName(),
		}
	}

	tf.b.WriteTimestamp(t)
	return tengo.UndefinedValue, nil
}
