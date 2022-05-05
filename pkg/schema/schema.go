package schema

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

type SchemaField struct {
	Name   string        `json:"name"`
	Doc    string        `json:"doc"`
	Type   string        `json:"type"`
	Fields []SchemaField `json:"fields,omitempty"`
}

func NewStringField(name string, doc string) SchemaField {
	return SchemaField{
		Name: name,
		Doc:  doc,
		Type: string(StringAvroType),
	}
}

func NewNullField(name string, doc string) SchemaField {
	return SchemaField{
		Name: name,
		Doc:  doc,
		Type: string(NullAvroType),
	}
}

func NewBoolField(name string, doc string) SchemaField {
	return SchemaField{
		Name: name,
		Doc:  doc,
		Type: string(BoolAvroType),
	}
}

func NewIntField(name string, doc string) SchemaField {
	return SchemaField{
		Name: name,
		Doc:  doc,
		Type: string(IntAvroType),
	}
}

func NewLongField(name string, doc string) SchemaField {
	return SchemaField{
		Name: name,
		Doc:  doc,
		Type: string(LongAvroType),
	}
}

func NewFloatField(name string, doc string) SchemaField {
	return SchemaField{
		Name: name,
		Doc:  doc,
		Type: string(FloatAvroType),
	}
}

func NewDoubleField(name string, doc string) SchemaField {
	return SchemaField{
		Name: name,
		Doc:  doc,
		Type: string(DoubleAvroType),
	}
}

func NewBytesField(name string, doc string) SchemaField {
	return SchemaField{
		Name: name,
		Doc:  doc,
		Type: string(BytesAvroType),
	}
}

func NewRecordField(name string, doc string, fields []SchemaField) SchemaField {
	return SchemaField{
		Name:   name,
		Doc:    doc,
		Type:   string(RecordAvroType),
		Fields: fields,
	}
}
