package schema

import "github.com/apache/arrow/go/arrow"

type TypeEnum = string

var (
	StringType TypeEnum = "string"
)

type SchemaField struct {
	Name string `yaml:"name"`
	Doc  string `yaml:"doc"`
	Type string `yaml:"type"`
}

type Schema struct {
	Fields []SchemaField `yaml:"fields,omitempty"`
}

var DaftFieldTypeToArrowType = map[string]arrow.DataType{
	StringType: arrow.BinaryTypes.String,
}

// Daft schemas are currently being translated into Arrow schemas for storage and usage by our Functions
// We should unify this when we are more certain of our data schemas in Daft. This is temporary.
// TODO(jaychia):
func (schema *Schema) ArrowSchema() *arrow.Schema {
	var arrowFields []arrow.Field
	for _, field := range schema.Fields {
		arrowFields = append(arrowFields, arrow.Field{Name: field.Name, Type: DaftFieldTypeToArrowType[field.Type]})
	}
	return arrow.NewSchema(arrowFields, nil)
}

func NewPrimitiveField(name string, doc string, t TypeEnum) SchemaField {
	return SchemaField{
		Name: name,
		Doc:  doc,
		Type: StringType,
	}
}
