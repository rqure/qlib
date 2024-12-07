package field

import (
	"strconv"
	"time"

	"github.com/rqure/qlib/pkg/data"
	"github.com/rqure/qlib/pkg/log"
	"github.com/rqure/qlib/pkg/protobufs"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type Value struct {
	impl *anypb.Any
}

func NewValue() data.Value {
	return &Value{
		impl: nil,
	}
}

func FromAnyPb(impl *anypb.Any) data.Value {
	return &Value{
		impl: impl,
	}
}

func (v *Value) IsInt() bool {
	return v.impl != nil && v.impl.MessageIs(&protobufs.Int{})
}

func (v *Value) IsFloat() bool {
	return v.impl != nil && v.impl.MessageIs(&protobufs.Float{})
}

func (v *Value) IsString() bool {
	return v.impl != nil && v.impl.MessageIs(&protobufs.String{})
}

func (v *Value) IsBool() bool {
	return v.impl != nil && v.impl.MessageIs(&protobufs.Bool{})
}

func (v *Value) IsBinaryFile() bool {
	return v.impl != nil && v.impl.MessageIs(&protobufs.BinaryFile{})
}

func (v *Value) IsEntityReference() bool {
	return v.impl != nil && v.impl.MessageIs(&protobufs.EntityReference{})
}

func (v *Value) IsTimestamp() bool {
	return v.impl != nil && v.impl.MessageIs(&protobufs.Timestamp{})
}

func (v *Value) IsTransformation() bool {
	return v.impl != nil && v.impl.MessageIs(&protobufs.Transformation{})
}

func (v *Value) GetInt() int64 {
	m := new(protobufs.Int)

	if v.impl != nil {
		if err := v.impl.UnmarshalTo(m); err != nil {
			return 0
		}
	}

	return m.Raw
}

func (v *Value) GetFloat() float64 {
	m := new(protobufs.Float)

	if v.impl != nil {
		if err := v.impl.UnmarshalTo(m); err != nil {
			return 0
		}
	}

	return m.Raw
}

func (v *Value) GetString() string {
	m := new(protobufs.String)

	if v.impl != nil {
		if err := v.impl.UnmarshalTo(m); err != nil {
			return ""
		}
	}

	return m.Raw
}

func (v *Value) GetBool() bool {
	m := new(protobufs.Bool)

	if v.impl != nil {
		if err := v.impl.UnmarshalTo(m); err != nil {
			return false
		}
	}

	return m.Raw
}

func (v *Value) GetBinaryFile() string {
	m := new(protobufs.BinaryFile)

	if v.impl != nil {
		if err := v.impl.UnmarshalTo(m); err != nil {
			return ""
		}
	}

	return m.Raw
}

func (v *Value) GetEntityReference() string {
	m := new(protobufs.EntityReference)

	if v.impl != nil {
		if err := v.impl.UnmarshalTo(m); err != nil {
			return ""
		}
	}

	return m.Raw
}

func (v *Value) GetTimestamp() time.Time {
	m := new(protobufs.Timestamp)

	if v.impl != nil {
		if err := v.impl.UnmarshalTo(m); err != nil {
			return time.Unix(0, 0)
		}
	}

	return m.Raw.AsTime()
}

func (v *Value) GetTransformation() string {
	m := new(protobufs.Transformation)

	if err := v.impl.UnmarshalTo(m); err != nil {
		return ""
	}

	return m.Raw
}

func (v *Value) SetInt(i interface{}) data.Value {
	value := int64(0)

	switch c := i.(type) {
	case int:
		value = int64(c)
	case int8:
		value = int64(c)
	case int16:
		value = int64(c)
	case int32:
		value = int64(c)
	case int64:
		value = c
	case uint:
		value = int64(c)
	case uint8:
		value = int64(c)
	case uint16:
		value = int64(c)
	case uint32:
		value = int64(c)
	case uint64:
		value = int64(c)
	case float32:
		value = int64(c)
	case float64:
		value = int64(c)
	case bool:
		if c {
			value = 1
		}
	case string:
		if i, err := strconv.ParseInt(c, 10, 64); err == nil {
			value = i
		} else {
			log.Error("[Value::SetInt] Error parsing int: %s", err)
		}
	default:
		log.Error("[Value::SetInt] Unsupported type: %T", v)
	}

	a, err := anypb.New(&protobufs.Int{
		Raw: value,
	})

	if err != nil {
		log.Error("[Value::SetInt] Error creating Any: %s", err)
	} else {
		v.impl = a
	}

	return v
}

func (v *Value) SetFloat(f interface{}) data.Value {
	value := float64(0)

	switch c := f.(type) {
	case int:
		value = float64(c)
	case int8:
		value = float64(c)
	case int16:
		value = float64(c)
	case int32:
		value = float64(c)
	case int64:
		value = float64(c)
	case uint:
		value = float64(c)
	case uint8:
		value = float64(c)
	case uint16:
		value = float64(c)
	case uint32:
		value = float64(c)
	case uint64:
		value = float64(c)
	case float32:
		value = float64(c)
	case float64:
		value = c
	case bool:
		if c {
			value = 1
		}
	case string:
		if f, err := strconv.ParseFloat(c, 64); err == nil {
			value = f
		} else {
			log.Error("[Value::SetFloat] Error parsing float: %s", err)
		}
	default:
		log.Error("[Value::SetFloat] Unsupported type: %T", v)
	}

	a, err := anypb.New(&protobufs.Float{
		Raw: value,
	})

	if err != nil {
		log.Error("[Value::SetFloat] Error creating Any: %s", err)
	} else {
		v.impl = a
	}

	return v
}

func (v *Value) SetString(s interface{}) data.Value {
	value := ""

	switch c := s.(type) {
	case int:
		value = strconv.Itoa(c)
	case int8:
		value = strconv.Itoa(int(c))
	case int16:
		value = strconv.Itoa(int(c))
	case int32:
		value = strconv.Itoa(int(c))
	case int64:
		value = strconv.Itoa(int(c))
	case uint:
		value = strconv.Itoa(int(c))
	case uint8:
		value = strconv.Itoa(int(c))
	case uint16:
		value = strconv.Itoa(int(c))
	case uint32:
		value = strconv.Itoa(int(c))
	case uint64:
		value = strconv.Itoa(int(c))
	case float32:
		value = strconv.FormatFloat(float64(c), 'f', -1, 32)
	case float64:
		value = strconv.FormatFloat(c, 'f', -1, 64)
	case bool:
		value = strconv.FormatBool(c)
	case string:
		value = c
	default:
		log.Error("[Value::SetString] Unsupported type: %T", v)
	}

	a, err := anypb.New(&protobufs.String{
		Raw: value,
	})

	if err != nil {
		log.Error("[Value::SetString] Error creating Any: %s", err)
	} else {
		v.impl = a
	}

	return v
}

func (v *Value) SetBool(b interface{}) data.Value {
	value := false

	switch c := b.(type) {
	case int:
		value = c != 0
	case int8:
		value = c != 0
	case int16:
		value = c != 0
	case int32:
		value = c != 0
	case int64:
		value = c != 0
	case uint:
		value = c != 0
	case uint8:
		value = c != 0
	case uint16:
		value = c != 0
	case uint32:
		value = c != 0
	case uint64:
		value = c != 0
	case float32:
		value = c != 0
	case float64:
		value = c != 0
	case bool:
		value = c
	case string:
		if b, err := strconv.ParseBool(c); err == nil {
			value = b
		} else if i, err := strconv.ParseInt(c, 10, 64); err == nil {
			value = i != 0
		} else if f, err := strconv.ParseFloat(c, 64); err == nil {
			value = f != 0
		} else {
			log.Error("[Value::SetBool] Error parsing bool: %s", err)
		}
	default:
		log.Error("[Value::SetBool] Unsupported type: %T", v)
	}

	a, err := anypb.New(&protobufs.Bool{
		Raw: value,
	})

	if err != nil {
		log.Error("[Value::SetBool] Error creating Any: %s", err)
	} else {
		v.impl = a
	}

	return v
}

func (v *Value) SetBinaryFile(b interface{}) data.Value {
	value := ""

	switch c := b.(type) {
	case string:
		value = c
	default:
		log.Error("[Value::SetBinaryFile] Unsupported type: %T", v)
	}

	a, err := anypb.New(&protobufs.BinaryFile{
		Raw: value,
	})

	if err != nil {
		log.Error("[Value::SetBinaryFile] Error creating Any: %s", err)
	} else {
		v.impl = a
	}

	return v
}

func (v *Value) SetEntityReference(e interface{}) data.Value {
	value := ""

	switch c := e.(type) {
	case string:
		value = c
	default:
		log.Error("[Value::SetEntityReference] Unsupported type: %T", v)
	}

	a, err := anypb.New(&protobufs.EntityReference{
		Raw: value,
	})

	if err != nil {
		log.Error("[Value::SetEntityReference] Error creating Any: %s", err)
	} else {
		v.impl = a
	}

	return v
}

func (v *Value) SetTimestamp(t interface{}) data.Value {
	value := time.Now()

	switch c := t.(type) {
	case time.Time:
		value = c
	case string:
		if ts, err := time.Parse(time.RFC3339, c); err == nil {
			value = ts
		} else {
			log.Error("[Value::SetTimestamp] Error parsing time: %s", err)
		}
	case int:
		value = time.Unix(int64(c), 0)
	case int8:
		value = time.Unix(int64(c), 0)
	case int16:
		value = time.Unix(int64(c), 0)
	case int32:
		value = time.Unix(int64(c), 0)
	case int64:
		value = time.Unix(c, 0)
	case uint:
		value = time.Unix(int64(c), 0)
	case uint8:
		value = time.Unix(int64(c), 0)
	case uint16:
		value = time.Unix(int64(c), 0)
	case uint32:
		value = time.Unix(int64(c), 0)
	case uint64:
		value = time.Unix(int64(c), 0)
	default:
		log.Error("[Value::SetTimestamp] Unsupported type: %T", v)
	}

	a, err := anypb.New(&protobufs.Timestamp{
		Raw: timestamppb.New(value),
	})

	if err != nil {
		log.Error("[Value::SetTimestamp] Error creating Any: %s", err)
	} else {
		v.impl = a
	}

	return v
}

func (v *Value) SetTransformation(t interface{}) data.Value {
	value := ""

	switch c := t.(type) {
	case string:
		value = c
	default:
		log.Error("[Value::SetTransformation] Unsupported type: %T", v)
	}

	a, err := anypb.New(&protobufs.Transformation{
		Raw: value,
	})

	if err != nil {
		log.Error("[Value::SetTransformation] Error creating Any: %s", err)
	} else {
		v.impl = a
	}

	return v
}