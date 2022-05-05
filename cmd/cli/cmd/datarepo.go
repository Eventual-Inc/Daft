package cmd

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/manifoldco/promptui"
	"github.com/spf13/cobra"
	"gopkg.in/yaml.v3"

	"github.com/Eventual-Inc/Daft/pkg/schema"
)

func init() {
	rootCmd.AddCommand(datarepoCmd)
	datarepoCmd.AddCommand(ingestCmd)
}

var (
	IndividualBinaryFilesSelector = selectPromptData{
		Name:        "Files (WIP)",
		Value:       "individual_binary_files",
		Description: "Individual files on disk or in object storage.",
	}
	CommaSeparatedValuesFilesSelector = selectPromptData{
		Name:        "CSV Files",
		Value:       "csv_files",
		Description: "Comma-separated value files on disk or in object storage. Other delimiters such as tabs are also supported.",
	}
	DatabaseTableSelector = selectPromptData{
		Name:        "Database Table (WIP)",
		Value:       "database_table",
		Description: "A database table from databases such as PostgreSQL, Snowflake or BigQuery.",
	}

	LocalDirectorySelector = selectPromptData{
		Name:        "Local Directory (WIP)",
		Value:       "local_dir",
		Description: "A directory on your current machine's local filesystem.",
	}
	AWSS3Selector = selectPromptData{
		Name:        "AWS S3",
		Value:       "aws_s3",
		Description: "An AWS S3 Bucket and prefix, indicating a collection of AWS S3 objects.",
	}

	CommasSelector = selectPromptData{
		Name:        "Commas: ,",
		Value:       ",",
		Description: "The most common type of delimiter in CSV files.",
	}
	TabsSelector = selectPromptData{
		Name:        "Tabs: \\t",
		Value:       "\t",
		Description: "Values in each column are separated by a tab.",
	}
)

type ManifestConfig interface {
	yaml.Marshaler
	Kind() string
}

type DatasourceTypeConfiguration interface {
	ManifestConfig
	SchemaHints() []schema.SchemaField
}

type CSVFilesTypeConfig struct {
	Delimiter rune
	Headers   []string
}

func NewCSVFilesTypeConfigFromPrompts() (*CSVFilesTypeConfig, error) {
	config := CSVFilesTypeConfig{}
	result, err := SelectPrompt(
		"Delimiter",
		"Columns in each file are delimited by this character",
		[]*selectPromptData{&CommasSelector, &TabsSelector},
	)
	if err != nil {
		return nil, err
	}
	config.Delimiter = []rune(result.Value)[0]

	headerPrompt := promptui.Prompt{
		Label: "Headers as comma-separated values (optional - inferred if not provided)",
	}
	headerResult, err := headerPrompt.Run()
	if err != nil {
		return nil, err
	}
	headers := strings.Split(headerResult, ",")
	config.Headers = headers
	return &config, nil
}

func (config *CSVFilesTypeConfig) MarshalYAML() (interface{}, error) {
	type s struct {
		Kind      string
		Delimiter rune
		Headers   []string
	}
	return s{
		Kind:      CommaSeparatedValuesFilesSelector.Value,
		Delimiter: config.Delimiter,
		Headers:   config.Headers,
	}, nil
}

func (config *CSVFilesTypeConfig) Kind() string {
	return CommaSeparatedValuesFilesSelector.Value
}

func (config *CSVFilesTypeConfig) SchemaHints() []schema.SchemaField {
	var fields []schema.SchemaField
	for _, header := range config.Headers {
		field := schema.NewStringField(header, "CSV header provided by user")
		fields = append(fields, field)
	}
	return fields
}

type AWSS3LocationConfig struct {
	Bucket string
	Prefix string
}

func (config *AWSS3LocationConfig) MarshalYAML() (interface{}, error) {
	type s struct {
		Kind   string
		Bucket string
		Prefix string
	}
	return s{
		Kind:   CommaSeparatedValuesFilesSelector.Value,
		Bucket: config.Bucket,
		Prefix: config.Prefix,
	}, nil
}

func (config *AWSS3LocationConfig) Kind() string {
	return AWSS3Selector.Value
}

func NewAWSS3LocationConfigFromPrompts() (*AWSS3LocationConfig, error) {
	config := AWSS3LocationConfig{}
	{
		prompt := promptui.Prompt{
			Label: "AWS S3 Bucket",
		}
		result, err := prompt.Run()
		if err != nil {
			return nil, err
		}
		config.Bucket = result
	}
	{
		prompt := promptui.Prompt{
			Label: "AWS S3 Prefix",
		}
		result, err := prompt.Run()
		if err != nil {
			return nil, err
		}
		config.Prefix = result
	}
	return &config, nil
}

type IngestManifest struct {
	selectedDatasourceType *selectPromptData
	DatasourceTypeConfig   DatasourceTypeConfiguration `yaml:"datasourceType"`

	selectedDatasourceLocation *selectPromptData
	DatasourceLocationConfig   ManifestConfig `yaml:"datasourceLocation"`
}

// Builds the configuration for the DatasourceType
func (manifest *IngestManifest) buildDatasourceTypeConfig() error {
	result, err := SelectPrompt(
		"Data format",
		"Choose how your data is laid out.",
		[]*selectPromptData{&CommaSeparatedValuesFilesSelector, &IndividualBinaryFilesSelector, &DatabaseTableSelector},
	)

	if err != nil {
		return err
	}
	switch result.Value {
	case IndividualBinaryFilesSelector.Value:
		return errors.New("individual binary files not yet supported")
	case CommaSeparatedValuesFilesSelector.Value:
		config, err := NewCSVFilesTypeConfigFromPrompts()
		if err != nil {
			return err
		}
		manifest.selectedDatasourceType = result
		manifest.DatasourceTypeConfig = config
		return nil
	case DatabaseTableSelector.Value:
		return errors.New("database tables not yet supported")
	default:
		return fmt.Errorf("datasource type %s not supported", result)
	}
}

// Builds the configuration for the DatasourceLocation
func (manifest *IngestManifest) buildDatasourceLocationConfig() error {
	switch manifest.selectedDatasourceType.Value {
	case IndividualBinaryFilesSelector.Value:
		return errors.New("individual binary files not yet supported")
	case CommaSeparatedValuesFilesSelector.Value:
		result, err := SelectPrompt(
			"CSV Files Location",
			"Specify where to find your files, and the appropriate credentials to access them.",
			[]*selectPromptData{&AWSS3Selector, &LocalDirectorySelector},
		)
		if err != nil {
			return err
		}
		config, err := buildDatasourceLocationConfigForSelectedLocation(result)
		if err != nil {
			return err
		}
		manifest.selectedDatasourceLocation = result
		manifest.DatasourceLocationConfig = config
		return nil
	case DatabaseTableSelector.Value:
		return errors.New("database tables not yet supported")
	default:
		return fmt.Errorf("datasource type %s not supported", manifest.selectedDatasourceType.Value)
	}
}

func buildDatasourceLocationConfigForSelectedLocation(location *selectPromptData) (ManifestConfig, error) {
	switch location {
	case &LocalDirectorySelector:
		return nil, errors.New("local directories not yet supported")
	case &AWSS3Selector:
		config, err := NewAWSS3LocationConfigFromPrompts()
		if err != nil {
			return nil, err
		}
		return config, nil
	default:
		return nil, fmt.Errorf("datasource location %s not supported", location)
	}
}

func (manifest *IngestManifest) confirmDatasourceConfigs() error {
	y, err := yaml.Marshal(manifest)
	if err != nil {
		return err
	}
	fmt.Println("Data Source Configurations:")
	fmt.Println("")
	fmt.Println(string(y))
	prompt := promptui.Prompt{
		Label:     "Confirm and detect schema?",
		IsConfirm: true,
	}
	result, err := prompt.Run()
	if err != nil {
		return err
	}
	if result != "y" {
		return errors.New("user cancelled data source configurations")
	}
	return nil
}

func (manifest *IngestManifest) buildDatarepoSchema() error {
	sampler, err := SamplerFactory(manifest.DatasourceTypeConfig, manifest.DatasourceLocationConfig)
	if err != nil {
		return err
	}
	samples, err := sampler.Sample()
	if err != nil {
		return err
	}

	// TODO(jchia): Go through samples and come up with best effort schema detected
	var schemaFields []schema.SchemaField
	for _, sampleResult := range samples {
		schemaFields = append(schemaFields, sampleResult.InferredSchema)
	}
	detectedSchema := schema.NewRecordField("schema", "", schemaFields)
	jsonSchema, err := json.MarshalIndent(detectedSchema, "", "    ")
	if err != nil {
		return err
	}
	finalizedSchemaStr, err := EditorPrompt(string(jsonSchema), "json")
	if err != nil {
		return err
	}
	recordField := schema.SchemaField{}
	err = json.Unmarshal([]byte(finalizedSchemaStr), &recordField)
	if err != nil {
		return err
	}

	fmt.Println("Final Schema:")
	fmt.Println(finalizedSchemaStr)
	prompt := promptui.Prompt{
		Label:     "Confirm finalized schema",
		IsConfirm: true,
	}
	confirmSchema, err := prompt.Run()
	if err != nil {
		return err
	}
	if confirmSchema != "y" {
		return errors.New("aborted finalizing schema")
	}

	return nil
}

var datarepoCmd = &cobra.Command{
	Use:   "datarepo",
	Short: "Commands related to Data Repositories",
	Long: `
Data Repositories are tabular and unbounded collections of data (they have rows and columns,
and the number of rows can grow indefinitely). They can natively support storing binary column
types, with no limit to the size of data in rows or columns.`,
}

var ingestCmd = &cobra.Command{
	Use:   "ingest",
	Short: "Creates a Data Repo by ingesting an existing data source",
	Long: `
Interactive UI for ingesting data from an existing data source, creating a new Data Repo.
Daft does a best-effort detection and generation of a schema, but users will be able to
modify and confirm the schema manually before creating the repo and ingesting data.`,
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("")
		var manifest IngestManifest

		err := manifest.buildDatasourceTypeConfig()
		cobra.CheckErr(err)

		err = manifest.buildDatasourceLocationConfig()
		cobra.CheckErr(err)

		err = manifest.confirmDatasourceConfigs()
		cobra.CheckErr(err)

		err = manifest.buildDatarepoSchema()
		cobra.CheckErr(err)
	},
}
