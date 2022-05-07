package schema

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

func NewPrimitiveField(name string, doc string, t TypeEnum) SchemaField {
	return SchemaField{
		Name: name,
		Doc:  doc,
		Type: StringType,
	}
}
