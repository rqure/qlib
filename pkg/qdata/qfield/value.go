package qfield

import (
	"strconv"
	"time"

	"github.com/rqure/qlib/pkg/qdata"
	"github.com/rqure/qlib/pkg/qlog"
	"github.com/rqure/qlib/pkg/qprotobufs"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type Value struct {
	impl **anypb.Any
}

func NewValue() qdata.ValueTypeProvider {
	return &qdata.Value{
		impl: new(*anypb.Any),
	}
}

func FromAnyPb(impl **anypb.Any) qdata.ValueTypeProvider {
	if impl == nil {
		impl = new(*anypb.Any)
		*impl = nil
	}

	return &qdata.Value{
		impl: impl,
	}
}

func ToAnyPb(v qdata.ValueTypeProvider) *anypb.Any {
	if v == nil {
		return nil
	}

	switch c := v.(type) {
	case *Value:
		return *c.impl
	default:
		qlog.Error("Unsupported type: %T", v)
		return nil
	}
}

func (me *Value) IsNil() bool {
	return me.getImpl() == nil
}

func (me *Value) IsInt() bool {
	return me.getImpl() != nil && me.getImpl().MessageIs(&qprotobufs.Int{})
}

func (me *Value) IsFloat() bool {
	return me.getImpl() != nil && me.getImpl().MessageIs(&qprotobufs.Float{})
}

func (me *Value) IsString() bool {
	return me.getImpl() != nil && me.getImpl().MessageIs(&qprotobufs.String{})
}

func (me *Value) IsBool() bool {
	return me.getImpl() != nil && me.getImpl().MessageIs(&qprotobufs.Bool{})
}

func (me *Value) IsBinaryFile() bool {
	return me.getImpl() != nil && me.getImpl().MessageIs(&qprotobufs.BinaryFile{})
}

func (me *Value) IsEntityReference() bool {
	return me.getImpl() != nil && me.getImpl().MessageIs(&qprotobufs.EntityReference{})
}

func (me *Value) IsTimestamp() bool {
	return me.getImpl() != nil && me.getImpl().MessageIs(&qprotobufs.Timestamp{})
}

func (me *Value) IsChoice() bool {
	return me.getImpl() != nil && me.getImpl().MessageIs(&qprotobufs.Choice{})
}

func (me *Value) IsEntityList() bool {
	return me.getImpl() != nil && me.getImpl().MessageIs(&qprotobufs.EntityList{})
}

func (me *Value) GetType() string {
	if me.getImpl() == nil {
		return ""
	}

	return me.getImpl().TypeUrl
}

func (me *Value) GetInt() int64 {
	m := new(qprotobufs.Int)

	if me.getImpl() != nil {
		if err := me.getImpl().UnmarshalTo(m); err != nil {
			qlog.Error("Error unmarshalling int: %s", err)
			return 0
		}
	}

	return m.Raw
}

func (me *Value) GetFloat() float64 {
	m := new(qprotobufs.Float)

	if me.getImpl() != nil {
		if err := me.getImpl().UnmarshalTo(m); err != nil {
			qlog.Error("Error unmarshalling float: %s", err)
			return 0
		}
	}

	return m.Raw
}

func (me *Value) GetString() string {
	m := new(qprotobufs.String)

	if me.getImpl() != nil {
		if err := me.getImpl().UnmarshalTo(m); err != nil {
			qlog.Error("Error unmarshalling string: %s", err)
			return ""
		}
	}

	return m.Raw
}

func (me *Value) GetBool() bool {
	m := new(qprotobufs.Bool)

	if me.getImpl() != nil {
		if err := me.getImpl().UnmarshalTo(m); err != nil {
			qlog.Error("Error unmarshalling bool: %s", err)
			return false
		}
	}

	return m.Raw
}

func (me *Value) GetBinaryFile() string {
	m := new(qprotobufs.BinaryFile)

	if me.getImpl() != nil {
		if err := me.getImpl().UnmarshalTo(m); err != nil {
			qlog.Error("Error unmarshalling binary file: %s", err)
			return ""
		}
	}

	return m.Raw
}

func (me *Value) GetEntityReference() string {
	m := new(qprotobufs.EntityReference)

	if me.getImpl() != nil {
		if err := me.getImpl().UnmarshalTo(m); err != nil {
			qlog.Error("Error unmarshalling entity reference: %s", err)
			return ""
		}
	}

	return m.Raw
}

func (me *Value) GetTimestamp() time.Time {
	m := new(qprotobufs.Timestamp)

	if me.getImpl() != nil {
		if err := me.getImpl().UnmarshalTo(m); err != nil {
			qlog.Error("Error unmarshalling timestamp: %s", err)
			return time.Unix(0, 0)
		}
	}

	return m.Raw.AsTime()
}

func (me *Value) GetChoice() qdata.Choice {
	m := new(qprotobufs.Choice)

	if me.getImpl() != nil {
		if err := me.getImpl().UnmarshalTo(m); err != nil {
			qlog.Error("Error unmarshalling choice: %s", err)
			return NewChoice(0)
		}
	}

	return NewChoice(m.Raw)
}

func (me *Value) GetEntityList() qdata.EntityList {
	m := new(qprotobufs.EntityList)

	if me.getImpl() != nil {
		if err := me.getImpl().UnmarshalTo(m); err != nil {
			qlog.Error("Error unmarshalling entity list: %s", err)
			return NewEntityList()
		}
	}

	return NewEntityList(m.Raw...)
}

func (me *Value) SetInt(i interface{}) qdata.ValueTypeProvider {
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
			qlog.Error("Error parsing int: %s", err)
		}
	default:
		qlog.Error("Unsupported type: %T", me)
	}

	a, err := anypb.New(&qprotobufs.Int{
		Raw: value,
	})

	if err != nil {
		qlog.Error("Error creating Any: %s", err)
	} else {
		me.setImpl(a)
	}

	return me
}

func (me *Value) SetFloat(f interface{}) qdata.ValueTypeProvider {
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
			qlog.Error("Error parsing float: %s", err)
		}
	default:
		qlog.Error("Unsupported type: %T", me)
	}

	a, err := anypb.New(&qprotobufs.Float{
		Raw: value,
	})

	if err != nil {
		qlog.Error("Error creating Any: %s", err)
	} else {
		me.setImpl(a)
	}

	return me
}

func (me *Value) SetString(s interface{}) qdata.ValueTypeProvider {
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
		qlog.Error("Unsupported type: %T", me)
	}

	a, err := anypb.New(&qprotobufs.String{
		Raw: value,
	})

	if err != nil {
		qlog.Error("Error creating Any: %s", err)
	} else {
		me.setImpl(a)
	}

	return me
}

func (me *Value) SetBool(b interface{}) qdata.ValueTypeProvider {
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
			qlog.Error("Error parsing bool: %s", err)
		}
	default:
		qlog.Error("Unsupported type: %T", me)
	}

	a, err := anypb.New(&qprotobufs.Bool{
		Raw: value,
	})

	if err != nil {
		qlog.Error("Error creating Any: %s", err)
	} else {
		me.setImpl(a)
	}

	return me
}

func (me *Value) SetBinaryFile(b interface{}) qdata.ValueTypeProvider {
	value := ""

	switch c := b.(type) {
	case string:
		value = c
	default:
		qlog.Error("Unsupported type: %T", me)
	}

	a, err := anypb.New(&qprotobufs.BinaryFile{
		Raw: value,
	})

	if err != nil {
		qlog.Error("Error creating Any: %s", err)
	} else {
		me.setImpl(a)
	}

	return me
}

func (me *Value) SetEntityReference(e interface{}) qdata.ValueTypeProvider {
	value := ""

	switch c := e.(type) {
	case string:
		value = c
	default:
		qlog.Error("Unsupported type: %T", me)
	}

	a, err := anypb.New(&qprotobufs.EntityReference{
		Raw: value,
	})

	if err != nil {
		qlog.Error("Error creating Any: %s", err)
	} else {
		me.setImpl(a)
	}

	return me
}

func (me *Value) SetTimestamp(t interface{}) qdata.ValueTypeProvider {
	value := time.Now()

	switch c := t.(type) {
	case time.Time:
		value = c
	case string:
		if ts, err := time.Parse(time.RFC3339, c); err == nil {
			value = ts
		} else {
			qlog.Error("Error parsing time: %s", err)
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
		qlog.Error("Unsupported type: %T", me)
	}

	a, err := anypb.New(&qprotobufs.Timestamp{
		Raw: timestamppb.New(value),
	})

	if err != nil {
		qlog.Error("Error creating Any: %s", err)
	} else {
		me.setImpl(a)
	}

	return me
}

func (me *Value) SetChoice(selected interface{}) qdata.ValueTypeProvider {
	value := int64(0)

	switch c := selected.(type) {
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
	case string:
		if i, err := strconv.ParseInt(c, 10, 64); err == nil {
			value = i
		} else {
			qlog.Error("Error parsing choice selection from '%s': %s", c, err)
		}
	case qdata.Choice:
		// If passed a Choice interface, use its values
		value = c.Index()
	default:
		qlog.Error("Unsupported type for choice selection: %T", selected)
	}

	a, err := anypb.New(&qprotobufs.Choice{
		Raw: value,
	})

	if err != nil {
		qlog.Error("Error creating Any: %s", err)
	} else {
		me.setImpl(a)
	}

	return me
}

func (me *Value) SetEntityList(entities interface{}) qdata.ValueTypeProvider {
	var entityList []string

	switch list := entities.(type) {
	case []string:
		entityList = list
	case string:
		entityList = []string{list} // Single entity as string
	case []interface{}:
		entityList = make([]string, 0, len(list))
		for _, item := range list {
			if str, ok := item.(string); ok {
				entityList = append(entityList, str)
			} else {
				qlog.Error("Non-string item in entity list: %T", item)
			}
		}
	case qdata.EntityList:
		// If passed an EntityList interface, use its values
		entityList = list.GetEntities()
	default:
		qlog.Error("Unsupported type for entity list: %T", entities)
		entityList = []string{}
	}

	a, err := anypb.New(&qprotobufs.EntityList{
		Raw: entityList,
	})

	if err != nil {
		qlog.Error("Error creating Any: %s", err)
	} else {
		me.setImpl(a)
	}

	return me
}

func (me *Value) getImpl() *anypb.Any {
	return *me.impl
}

func (me *Value) setImpl(a *anypb.Any) {
	*me.impl = a
}
