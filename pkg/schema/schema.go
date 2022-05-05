package schema

import (
	"encoding/json"
)

type TypeEnum string

var (
	NullAvroType   TypeEnum = "null"
	BoolAvroType   TypeEnum = "boolean"
	IntAvroType    TypeEnum = "int"
	LongAvroType   TypeEnum = "long"
	FloatAvroType  TypeEnum = "float"
	DoubleAvroType TypeEnum = "double"
	BytesAvroType  TypeEnum = "bytes"
	StringAvroType TypeEnum = "string"

	RecordAvroType TypeEnum = "record"
	// These complex types are not used by Daft at the moment
	//
	// EnumAvroType TypeEnum = "enum"
	// ArrayAvroType TypeEnum = "array"
	// MapAvroType TypeEnum = "map"
	// FixedAvroType TypeEnum = "fixed"
	// UnionAvroType  TypeEnum = "union"
)

type AvroType interface {
	// AvroTypes marshal and unmarshal into Avro-compatible JSONs
	// These are usually just strings, except for Union types which are arrays
	json.Marshaler
	json.Unmarshaler

	Name() TypeEnum
}

type SchemaField interface {
	Name() string
	Type() AvroType
	Doc() string

	// Populated if RecordType
	Fields() []SchemaField
}

// Primitive Fields

type primitiveField struct {
	NameAttr string   `json:"name"`
	DocAttr  string   `json:"doc"`
	AvroType AvroType `json:"type"`
}

func (field *primitiveField) MarshalJSON() ([]byte, error) {
	return json.Marshal(field)
}

func (field *primitiveField) UnmarshalJSON(data []byte) error {
	err := json.Unmarshal(data, field)
	if err != nil {
		return err
	}
	return nil
}

func (field *primitiveField) Name() string {
	return field.NameAttr
}

func (field *primitiveField) Type() AvroType {
	return field.AvroType
}

func (field *primitiveField) Doc() string {
	return field.DocAttr
}

func (field *primitiveField) Fields() []SchemaField {
	return nil
}

type primitiveAvroType struct {
	name string
}

func (avroType *primitiveAvroType) MarshalJSON() ([]byte, error) {
	return []byte(avroType.name), nil
}

func (avroType *primitiveAvroType) UnmarshalJSON(data []byte) error {
	avroType.name = string(data)
	return nil
}

func (avroType *primitiveAvroType) Name() TypeEnum {
	return TypeEnum(avroType.name)
}

func NewStringField(name string, doc string) primitiveField {
	avroType := &primitiveAvroType{name: string(StringAvroType)}
	return primitiveField{
		NameAttr: name,
		DocAttr:  doc,
		AvroType: avroType,
	}
}

func NewNullField(name string, doc string) primitiveField {
	avroType := &primitiveAvroType{name: string(NullAvroType)}
	return primitiveField{
		NameAttr: name,
		DocAttr:  doc,
		AvroType: avroType,
	}
}

func NewBoolField(name string, doc string) primitiveField {
	avroType := &primitiveAvroType{name: string(BoolAvroType)}
	return primitiveField{
		NameAttr: name,
		DocAttr:  doc,
		AvroType: avroType,
	}
}

func NewIntField(name string, doc string) primitiveField {
	avroType := &primitiveAvroType{name: string(IntAvroType)}
	return primitiveField{
		NameAttr: name,
		DocAttr:  doc,
		AvroType: avroType,
	}
}

func NewLongField(name string, doc string) primitiveField {
	avroType := &primitiveAvroType{name: string(LongAvroType)}
	return primitiveField{
		NameAttr: name,
		DocAttr:  doc,
		AvroType: avroType,
	}
}

func NewFloatField(name string, doc string) primitiveField {
	avroType := &primitiveAvroType{name: string(FloatAvroType)}
	return primitiveField{
		NameAttr: name,
		DocAttr:  doc,
		AvroType: avroType,
	}
}

func NewDoubleField(name string, doc string) primitiveField {
	avroType := &primitiveAvroType{name: string(DoubleAvroType)}
	return primitiveField{
		NameAttr: name,
		DocAttr:  doc,
		AvroType: avroType,
	}
}

func NewBytesField(name string, doc string) primitiveField {
	avroType := &primitiveAvroType{name: string(BytesAvroType)}
	return primitiveField{
		NameAttr: name,
		DocAttr:  doc,
		AvroType: avroType,
	}
}

// Record Fields
type recordField struct {
	NameAttr   string        `json:"name"`
	DocAttr    string        `json:"doc"`
	FieldsAttr []SchemaField `json:"fields"`
}

func (field *recordField) Name() string {
	return field.NameAttr
}

func (field *recordField) Type() TypeEnum {
	return RecordAvroType
}

func (field *recordField) Doc() string {
	return field.DocAttr
}

func (field *recordField) Fields() []SchemaField {
	return field.FieldsAttr
}

func NewRecordField(name string, doc string, fields []SchemaField) recordField {
	f := recordField{
		NameAttr:   name,
		DocAttr:    doc,
		FieldsAttr: fields,
	}
	return f
}
