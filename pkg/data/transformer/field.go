package transformer

import (
	"context"

	"github.com/d5/tengo/v2"
	"github.com/rqure/qlib/pkg/data"
)

type TengoField struct {
	b data.FieldBinding
}

func NewTengoField(b data.FieldBinding) *TengoField {
	return &TengoField{b: b}
}

func (tf *TengoField) ToTengoMap(ctx context.Context) tengo.Object {
	return &tengo.Map{
		Value: map[string]tengo.Object{
			"readInt": &tengo.UserFunction{
				Name: "readInt",
				Value: func(args ...tengo.Object) (tengo.Object, error) {
					return tf.ReadInt(ctx, args...)
				},
			},
			"readFloat": &tengo.UserFunction{
				Name: "readFloat",
				Value: func(args ...tengo.Object) (tengo.Object, error) {
					return tf.ReadFloat(ctx, args...)
				},
			},
			"readString": &tengo.UserFunction{
				Name: "readString",
				Value: func(args ...tengo.Object) (tengo.Object, error) {
					return tf.ReadString(ctx, args...)
				},
			},
			"readBool": &tengo.UserFunction{
				Name: "readBool",
				Value: func(args ...tengo.Object) (tengo.Object, error) {
					return tf.ReadBool(ctx, args...)
				},
			},
			"readBinaryFile": &tengo.UserFunction{
				Name: "readBinaryFile",
				Value: func(args ...tengo.Object) (tengo.Object, error) {
					return tf.ReadBinaryFile(ctx, args...)
				},
			},
			"readEntityReference": &tengo.UserFunction{
				Name: "readEntityReference",
				Value: func(args ...tengo.Object) (tengo.Object, error) {
					return tf.ReadEntityReference(ctx, args...)
				},
			},
			"readTimestamp": &tengo.UserFunction{
				Name: "readTimestamp",
				Value: func(args ...tengo.Object) (tengo.Object, error) {
					return tf.ReadTimestamp(ctx, args...)
				},
			},
			"getWriteTime": &tengo.UserFunction{
				Name: "getWriteTime",
				Value: func(args ...tengo.Object) (tengo.Object, error) {
					return tf.GetWriteTime(ctx, args...)
				},
			},
			"getWriter": &tengo.UserFunction{
				Name: "getWriter",
				Value: func(args ...tengo.Object) (tengo.Object, error) {
					return tf.GetWriter(ctx, args...)
				},
			},
			"getInt": &tengo.UserFunction{
				Name: "getInt",
				Value: func(args ...tengo.Object) (tengo.Object, error) {
					return tf.GetInt(ctx, args...)
				},
			},
			"getFloat": &tengo.UserFunction{
				Name: "getFloat",
				Value: func(args ...tengo.Object) (tengo.Object, error) {
					return tf.GetFloat(ctx, args...)
				},
			},
			"getString": &tengo.UserFunction{
				Name: "getString",
				Value: func(args ...tengo.Object) (tengo.Object, error) {
					return tf.GetString(ctx, args...)
				},
			},
			"getBool": &tengo.UserFunction{
				Name: "getBool",
				Value: func(args ...tengo.Object) (tengo.Object, error) {
					return tf.GetBool(ctx, args...)
				},
			},
			"getBinaryFile": &tengo.UserFunction{
				Name: "getBinaryFile",
				Value: func(args ...tengo.Object) (tengo.Object, error) {
					return tf.GetBinaryFile(ctx, args...)
				},
			},
			"getEntityReference": &tengo.UserFunction{
				Name: "getEntityReference",
				Value: func(args ...tengo.Object) (tengo.Object, error) {
					return tf.GetEntityReference(ctx, args...)
				},
			},
			"getTimestamp": &tengo.UserFunction{
				Name: "getTimestamp",
				Value: func(args ...tengo.Object) (tengo.Object, error) {
					return tf.GetTimestamp(ctx, args...)
				},
			},
			"getId": &tengo.UserFunction{
				Name: "getEntityId",
				Value: func(args ...tengo.Object) (tengo.Object, error) {
					return tf.GetEntityId(ctx, args...)
				},
			},
			"getName": &tengo.UserFunction{
				Name: "getFieldName",
				Value: func(args ...tengo.Object) (tengo.Object, error) {
					return tf.GetFieldName(ctx, args...)
				},
			},
			"writeInt": &tengo.UserFunction{
				Name: "writeInt",
				Value: func(args ...tengo.Object) (tengo.Object, error) {
					return tf.WriteInt(ctx, args...)
				},
			},
			"writeFloat": &tengo.UserFunction{
				Name: "writeFloat",
				Value: func(args ...tengo.Object) (tengo.Object, error) {
					return tf.WriteFloat(ctx, args...)
				},
			},
			"writeString": &tengo.UserFunction{
				Name: "writeString",
				Value: func(args ...tengo.Object) (tengo.Object, error) {
					return tf.WriteString(ctx, args...)
				},
			},
			"writeBool": &tengo.UserFunction{
				Name: "writeBool",
				Value: func(args ...tengo.Object) (tengo.Object, error) {
					return tf.WriteBool(ctx, args...)
				},
			},
			"writeBinaryFile": &tengo.UserFunction{
				Name: "writeBinaryFile",
				Value: func(args ...tengo.Object) (tengo.Object, error) {
					return tf.WriteBinaryFile(ctx, args...)
				},
			},
			"writeEntityReference": &tengo.UserFunction{
				Name: "writeEntityReference",
				Value: func(args ...tengo.Object) (tengo.Object, error) {
					return tf.WriteEntityReference(ctx, args...)
				},
			},
			"writeTimestamp": &tengo.UserFunction{
				Name: "writeTimestamp",
				Value: func(args ...tengo.Object) (tengo.Object, error) {
					return tf.WriteTimestamp(ctx, args...)
				},
			},
		},
	}
}

func (tf *TengoField) ReadInt(ctx context.Context, args ...tengo.Object) (tengo.Object, error) {
	return &tengo.Int{Value: tf.b.ReadInt(ctx)}, nil
}

func (tf *TengoField) ReadFloat(ctx context.Context, args ...tengo.Object) (tengo.Object, error) {
	return &tengo.Float{Value: tf.b.ReadFloat(ctx)}, nil
}

func (tf *TengoField) ReadString(ctx context.Context, args ...tengo.Object) (tengo.Object, error) {
	return &tengo.String{Value: tf.b.ReadString(ctx)}, nil
}

func (tf *TengoField) ReadBool(ctx context.Context, args ...tengo.Object) (tengo.Object, error) {
	if tf.b.ReadBool(ctx) {
		return tengo.TrueValue, nil
	}

	return tengo.FalseValue, nil
}

func (tf *TengoField) ReadBinaryFile(ctx context.Context, args ...tengo.Object) (tengo.Object, error) {
	return &tengo.String{Value: tf.b.ReadBinaryFile(ctx)}, nil
}

func (tf *TengoField) ReadEntityReference(ctx context.Context, args ...tengo.Object) (tengo.Object, error) {
	return &tengo.String{Value: tf.b.ReadEntityReference(ctx)}, nil
}

func (tf *TengoField) ReadTimestamp(ctx context.Context, args ...tengo.Object) (tengo.Object, error) {
	return &tengo.Time{Value: tf.b.ReadTimestamp(ctx)}, nil
}

func (tf *TengoField) GetInt(ctx context.Context, args ...tengo.Object) (tengo.Object, error) {
	return &tengo.Int{Value: tf.b.GetInt()}, nil
}

func (tf *TengoField) GetFloat(ctx context.Context, args ...tengo.Object) (tengo.Object, error) {
	return &tengo.Float{Value: tf.b.GetFloat()}, nil
}

func (tf *TengoField) GetString(ctx context.Context, args ...tengo.Object) (tengo.Object, error) {
	return &tengo.String{Value: tf.b.GetString()}, nil
}

func (tf *TengoField) GetBool(ctx context.Context, args ...tengo.Object) (tengo.Object, error) {
	if tf.b.GetBool() {
		return tengo.TrueValue, nil
	}

	return tengo.FalseValue, nil
}

func (tf *TengoField) GetBinaryFile(ctx context.Context, args ...tengo.Object) (tengo.Object, error) {
	return &tengo.String{Value: tf.b.GetBinaryFile()}, nil
}

func (tf *TengoField) GetEntityReference(ctx context.Context, args ...tengo.Object) (tengo.Object, error) {
	return &tengo.String{Value: tf.b.GetEntityReference()}, nil
}

func (tf *TengoField) GetTimestamp(ctx context.Context, args ...tengo.Object) (tengo.Object, error) {
	return &tengo.Time{Value: tf.b.GetTimestamp()}, nil
}

func (tf *TengoField) GetWriteTime(ctx context.Context, args ...tengo.Object) (tengo.Object, error) {
	return &tengo.Time{Value: tf.b.GetWriteTime()}, nil
}

func (tf *TengoField) GetWriter(ctx context.Context, args ...tengo.Object) (tengo.Object, error) {
	return &tengo.String{Value: tf.b.GetWriter()}, nil
}

func (tf *TengoField) GetEntityId(ctx context.Context, args ...tengo.Object) (tengo.Object, error) {
	return &tengo.String{Value: tf.b.GetEntityId()}, nil
}

func (tf *TengoField) GetFieldName(ctx context.Context, args ...tengo.Object) (tengo.Object, error) {
	return &tengo.String{Value: tf.b.GetFieldName()}, nil
}

func (tf *TengoField) WriteInt(ctx context.Context, args ...tengo.Object) (tengo.Object, error) {
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

	writeOpts := []interface{}{i}
	if len(args) > 1 {
		if opt, ok := args[1].(*tengo.Int); ok {
			writeOpts = append(writeOpts, data.WriteOpt(opt.Value))
		}
	}

	tf.b.WriteInt(ctx, writeOpts...)
	return tengo.UndefinedValue, nil
}

func (tf *TengoField) WriteFloat(ctx context.Context, args ...tengo.Object) (tengo.Object, error) {
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

	writeOpts := []interface{}{f}
	if len(args) > 1 {
		if opt, ok := args[1].(*tengo.Int); ok {
			writeOpts = append(writeOpts, data.WriteOpt(opt.Value))
		}
	}

	tf.b.WriteFloat(ctx, writeOpts...)
	return tengo.UndefinedValue, nil
}

func (tf *TengoField) WriteString(ctx context.Context, args ...tengo.Object) (tengo.Object, error) {
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

	writeOpts := []interface{}{s}
	if len(args) > 1 {
		if opt, ok := args[1].(*tengo.Int); ok {
			writeOpts = append(writeOpts, data.WriteOpt(opt.Value))
		}
	}

	tf.b.WriteString(ctx, writeOpts...)
	return tengo.UndefinedValue, nil
}

func (tf *TengoField) WriteBool(ctx context.Context, args ...tengo.Object) (tengo.Object, error) {
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

	writeOpts := []interface{}{b}
	if len(args) > 1 {
		if opt, ok := args[1].(*tengo.Int); ok {
			writeOpts = append(writeOpts, data.WriteOpt(opt.Value))
		}
	}

	tf.b.WriteBool(ctx, writeOpts...)
	return tengo.UndefinedValue, nil
}

func (tf *TengoField) WriteBinaryFile(ctx context.Context, args ...tengo.Object) (tengo.Object, error) {
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

	writeOpts := []interface{}{b}
	if len(args) > 1 {
		if opt, ok := args[1].(*tengo.Int); ok {
			writeOpts = append(writeOpts, data.WriteOpt(opt.Value))
		}
	}

	tf.b.WriteBinaryFile(ctx, writeOpts...)
	return tengo.UndefinedValue, nil
}

func (tf *TengoField) WriteEntityReference(ctx context.Context, args ...tengo.Object) (tengo.Object, error) {
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

	writeOpts := []interface{}{e}
	if len(args) > 1 {
		if opt, ok := args[1].(*tengo.Int); ok {
			writeOpts = append(writeOpts, data.WriteOpt(opt.Value))
		}
	}

	tf.b.WriteEntityReference(ctx, writeOpts...)
	return tengo.UndefinedValue, nil
}

func (tf *TengoField) WriteTimestamp(ctx context.Context, args ...tengo.Object) (tengo.Object, error) {
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

	writeOpts := []interface{}{t}
	if len(args) > 1 {
		if opt, ok := args[1].(*tengo.Int); ok {
			writeOpts = append(writeOpts, data.WriteOpt(opt.Value))
		}
	}

	tf.b.WriteTimestamp(ctx, writeOpts...)
	return tengo.UndefinedValue, nil
}
