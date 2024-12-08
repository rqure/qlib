package binding

import (
	"time"

	"github.com/rqure/qlib/pkg/data"
	"github.com/rqure/qlib/pkg/data/field"
	"github.com/rqure/qlib/pkg/data/request"
	"github.com/rqure/qlib/pkg/log"
)

type Binding struct {
	s data.Store
	r data.Request
}

func New(s data.Store, e, f string) data.Binding {
	r := request.New().SetEntityId(e).SetFieldName(f)
	return &Binding{
		s: s,
		r: r,
	}
}

func (b *Binding) GetEntityId() string {
	return b.r.GetEntityId()
}

func (b *Binding) GetFieldName() string {
	return b.r.GetFieldName()
}

func (b *Binding) GetWriteTime() time.Time {
	return field.FromRequest(b.r).GetWriteTime()
}

func (b *Binding) GetWriter() string {
	return field.FromRequest(b.r).GetWriter()
}

func (b *Binding) GetValue() data.Value {
	return b.r.GetValue()
}

func (b *Binding) WriteValue(v data.Value) Binding {
	b.r.SetValue(v)
	b.s.Write(b.r)

	return b
}

func (b *Binding) WriteInt(args ...interface{}) Binding {
	v := args[0]

	b.r.GetValue().SetInt(args[0])

	if len(args) > 1 {
		writeChanges := args[1].(data.WriteOpt) == data.WriteChanges

		if writeChanges && b.ReadInt() == b.GetValue().GetInt() {
			return b
		}
	}

	if len(args) > 2 {
		wt, ok := args[2].(time.Time)
		if ok {
			b.r.SetWriteTime(&wt)
		} else {
			log.Error("[Binding::WriteInt] Invalid write time: %v", args[2])
		}
	}

	if len(args) > 3 {
		writer, ok := args[3].(string)
		if ok {
			b.r.SetWriter(&writer)
		} else {
			log.Error("[Binding::WriteInt] Invalid writer: %v", args[3])
		}
	}

	b.s.Write(b.r)

	// Clear write time and writer for future use
	b.r.SetWriteTime(nil).SetWriter(nil)

	return b
}

func (b *Binding) WriteFloat(args ...interface{}) Binding {
	v := args[0]

	b.r.GetValue().SetFloat(args[0])

	if len(args) > 1 {
		writeChanges := args[1].(data.WriteOpt) == data.WriteChanges

		if writeChanges && b.ReadFloat() == b.GetValue().GetFloat() {
			return b
		}
	}

	if len(args) > 2 {
		wt, ok := args[2].(time.Time)
		if ok {
			b.r.SetWriteTime(&wt)
		} else {
			log.Error("[Binding::WriteFloat] Invalid write time: %v", args[2])
		}
	}

	if len(args) > 3 {
		writer, ok := args[3].(string)
		if ok {
			b.r.SetWriter(&writer)
		} else {
			log.Error("[Binding::WriteFloat] Invalid writer: %v", args[3])
		}
	}

	b.s.Write(b.r)

	// Clear write time and writer for future use
	b.r.SetWriteTime(nil).SetWriter(nil)

	return b
}

func (b *Binding) WriteString(args ...interface{}) Binding {
	v := args[0]

	b.r.GetValue().SetString(args[0])

	if len(args) > 1 {
		writeChanges := args[1].(data.WriteOpt) == data.WriteChanges

		if writeChanges && b.ReadString() == b.GetValue().GetString() {
			return b
		}
	}

	if len(args) > 2 {
		wt, ok := args[2].(time.Time)
		if ok {
			b.r.SetWriteTime(&wt)
		} else {
			log.Error("[Binding::WriteString] Invalid write time: %v", args[2])
		}
	}

	if len(args) > 3 {
		writer, ok := args[3].(string)
		if ok {
			b.r.SetWriter(&writer)
		} else {
			log.Error("[Binding::WriteString] Invalid writer: %v", args[3])
		}
	}

	b.s.Write(b.r)

	// Clear write time and writer for future use
	b.r.SetWriteTime(nil).SetWriter(nil)

	return b
}

func (b *Binding) WriteBool(args ...interface{}) Binding {
	v := args[0]

	b.r.GetValue().SetBool(args[0])

	if len(args) > 1 {
		writeChanges := args[1].(data.WriteOpt) == data.WriteChanges

		if writeChanges && b.ReadBool() == b.GetValue().GetBool() {
			return b
		}
	}

	if len(args) > 2 {
		wt, ok := args[2].(time.Time)
		if ok {
			b.r.SetWriteTime(&wt)
		} else {
			log.Error("[Binding::WriteBool] Invalid write time: %v", args[2])
		}
	}

	if len(args) > 3 {
		writer, ok := args[3].(string)
		if ok {
			b.r.SetWriter(&writer)
		} else {
			log.Error("[Binding::WriteBool] Invalid writer: %v", args[3])
		}
	}

	b.s.Write(b.r)

	// Clear write time and writer for future use
	b.r.SetWriteTime(nil).SetWriter(nil)

	return b
}

func (b *Binding) WriteBinaryFile(args ...interface{}) Binding {
	v := args[0]

	b.r.GetValue().SetBinaryFile(args[0])

	if len(args) > 1 {
		writeChanges := args[1].(data.WriteOpt) == data.WriteChanges

		if writeChanges && b.ReadBinaryFile() == b.GetValue().GetBinaryFile() {
			return b
		}
	}

	if len(args) > 2 {
		wt, ok := args[2].(time.Time)
		if ok {
			b.r.SetWriteTime(&wt)
		} else {
			log.Error("[Binding::WriteBinaryFile] Invalid write time: %v", args[2])
		}
	}

	if len(args) > 3 {
		writer, ok := args[3].(string)
		if ok {
			b.r.SetWriter(&writer)
		} else {
			log.Error("[Binding::WriteBinaryFile] Invalid writer: %v", args[3])
		}
	}

	b.s.Write(b.r)

	// Clear write time and writer for future use
	b.r.SetWriteTime(nil).SetWriter(nil)

	return b
}

func (b *Binding) WriteEntityReference(args ...interface{}) Binding {
	v := args[0]

	b.r.GetValue().SetEntityReference(args[0])

	if len(args) > 1 {
		writeChanges := args[1].(data.WriteOpt) == data.WriteChanges

		if writeChanges && b.ReadEntityReference() == b.GetValue().GetEntityReference() {
			return b
		}
	}

	if len(args) > 2 {
		wt, ok := args[2].(time.Time)
		if ok {
			b.r.SetWriteTime(&wt)
		} else {
			log.Error("[Binding::WriteEntityReference] Invalid write time: %v", args[2])
		}
	}

	if len(args) > 3 {
		writer, ok := args[3].(string)
		if ok {
			b.r.SetWriter(&writer)
		} else {
			log.Error("[Binding::WriteEntityReference] Invalid writer: %v", args[3])
		}
	}

	b.s.Write(b.r)

	// Clear write time and writer for future use
	b.r.SetWriteTime(nil).SetWriter(nil)

	return b
}

func (b *Binding) WriteTimestamp(args ...interface{}) Binding {
	v := args[0]

	b.r.GetValue().SetTimestamp(args[0])

	if len(args) > 1 {
		writeChanges := args[1].(data.WriteOpt) == data.WriteChanges

		if writeChanges && b.ReadTimestamp() == b.GetValue().GetTimestamp() {
			return b
		}
	}

	if len(args) > 2 {
		wt, ok := args[2].(time.Time)
		if ok {
			b.r.SetWriteTime(&wt)
		} else {
			log.Error("[Binding::WriteTimestamp] Invalid write time: %v", args[2])
		}
	}

	if len(args) > 3 {
		writer, ok := args[3].(string)
		if ok {
			b.r.SetWriter(&writer)
		} else {
			log.Error("[Binding::WriteTimestamp] Invalid writer: %v", args[3])
		}
	}

	b.s.Write(b.r)

	// Clear write time and writer for future use
	b.r.SetWriteTime(nil).SetWriter(nil)

	return b
}

func (b *Binding) WriteTransformation(args ...interface{}) Binding {
	v := args[0]

	b.r.GetValue().SetTransformation(args[0])

	if len(args) > 1 {
		writeChanges := args[1].(data.WriteOpt) == data.WriteChanges

		if writeChanges && b.ReadTransformation() == b.GetValue().GetTransformation() {
			return b
		}
	}

	if len(args) > 2 {
		wt, ok := args[2].(time.Time)
		if ok {
			b.r.SetWriteTime(&wt)
		} else {
			log.Error("[Binding::WriteTransformation] Invalid write time: %v", args[2])
		}
	}

	if len(args) > 3 {
		writer, ok := args[3].(string)
		if ok {
			b.r.SetWriter(&writer)
		} else {
			log.Error("[Binding::WriteTransformation] Invalid writer: %v", args[3])
		}
	}

	b.s.Write(b.r)

	// Clear write time and writer for future use
	b.r.SetWriteTime(nil).SetWriter(nil)

	return b
}

func (b *Binding) ReadValue() data.Value {
	b.s.Read(b.r)
	return b.r.GetValue()
}

func (b *Binding) ReadInt() int64 {
	b.s.Read(b.r)
	return b.r.GetValue().GetInt()
}

func (b *Binding) ReadFloat() float64 {
	b.s.Read(b.r)
	return b.r.GetValue().GetFloat()
}

func (b *Binding) ReadString() string {
	b.s.Read(b.r)
	return b.r.GetValue().GetString()
}

func (b *Binding) ReadBool() bool {
	b.s.Read(b.r)
	return b.r.GetValue().GetBool()
}

func (b *Binding) ReadBinaryFile() string {
	b.s.Read(b.r)
	return b.r.GetValue().GetBinaryFile()
}

func (b *Binding) ReadEntityReference() string {
	b.s.Read(b.r)
	return b.r.GetValue().GetEntityReference()
}

func (b *Binding) ReadTimestamp() time.Time {
	b.s.Read(b.r)
	return b.r.GetValue().GetTimestamp()
}

func (b *Binding) ReadTransformation() string {
	b.s.Read(b.r)
	return b.r.GetValue().GetTransformation()
}
