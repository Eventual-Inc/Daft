package schema

type AvroTypeEnum string
type DaftTypeEnum string

var (
	// Avro types that are used to write avro-compatible schemas
	// EnumAvroType AvroTypeEnum = "enum"
	// ArrayAvroType AvroTypeEnum = "array"
	// MapAvroType AvroTypeEnum = "map"
	// FixedAvroType AvroTypeEnum = "fixed"
	// UnionAvroType  AvroTypeEnum = "union"
	NullAvroType   AvroTypeEnum = "null"
	BoolAvroType   AvroTypeEnum = "boolean"
	IntAvroType    AvroTypeEnum = "int"
	LongAvroType   AvroTypeEnum = "long"
	FloatAvroType  AvroTypeEnum = "float"
	DoubleAvroType AvroTypeEnum = "double"
	BytesAvroType  AvroTypeEnum = "bytes"
	StringAvroType AvroTypeEnum = "string"
	RecordAvroType AvroTypeEnum = "record"

	// Daft types that provide Daft applications an extra layer of
	// semantic information about the type of data in each column
	// e.g. string->date, string->url etc
	URLDaftType DaftTypeEnum = "string/url"
)

type SchemaField struct {
	Name     string        `json:"name"`
	Doc      string        `json:"doc"`
	Type     string        `json:"type"`
	DaftType string        `json:"daft_type"`
	Fields   []SchemaField `json:"fields,omitempty"`
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
