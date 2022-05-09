package cmd

import (
	"errors"
	"fmt"

	"github.com/spf13/cobra"
	"gopkg.in/yaml.v3"

	"github.com/Eventual-Inc/Daft/pkg/datarepo"
	"github.com/Eventual-Inc/Daft/pkg/datarepo/ingest"
	"github.com/Eventual-Inc/Daft/pkg/datarepo/sample"
	"github.com/Eventual-Inc/Daft/pkg/datarepo/schema"
)

func init() {
	rootCmd.AddCommand(datarepoCmd)
	datarepoCmd.AddCommand(ingestCmd)
}

var SchemaEditorTutorialBlurb = `# This editor lets you make manual modifications to your field types to help Daft ingest your data.
#
# Daft types and what they mean:
#
#     "string": A simple string
#     "uri/s3": A URL to S3 that starts with s3://...
#     "uri/http": A URL to a HTTP resource that can be retrieved with a GET request
#


`

var (
	IndividualBinaryFilesSelector = selectPromptData{
		Name:        "Files (WIP)",
		Value:       datarepo.DataformatIDIndividualFiles,
		Description: "Individual files on disk or in object storage.",
	}
	CommaSeparatedValuesFilesSelector = selectPromptData{
		Name:        "CSV Files",
		Value:       datarepo.DataformatIDCSVFiles,
		Description: "Comma-separated value files on disk or in object storage. Other delimiters such as tabs are also supported.",
	}
	DatabaseTableSelector = selectPromptData{
		Name:        "Database Table (WIP)",
		Value:       datarepo.DataformatIDDatabaseTable,
		Description: "A database table from databases such as PostgreSQL, Snowflake or BigQuery.",
	}

	LocalDirectorySelector = selectPromptData{
		Name:        "Local Directory (WIP)",
		Value:       datarepo.DatasourceIDLocalDirectory,
		Description: "A directory on your current machine's local filesystem.",
	}
	AWSS3Selector = selectPromptData{
		Name:        "AWS S3",
		Value:       datarepo.DatasourceIDAWSS3,
		Description: "An AWS S3 Bucket and prefix, indicating a collection of AWS S3 objects.",
	}

	CommasSelector = selectPromptData{
		Name:        "Commas: ,",
		Value:       datarepo.CSVDelimiterCommas,
		Description: "The most common type of delimiter in CSV files.",
	}
	TabsSelector = selectPromptData{
		Name:        "Tabs: \\t",
		Value:       datarepo.CSVDelimiterTabs,
		Description: "Values in each column are separated by a tab.",
	}
)

var locationSelectors = []selectPromptData{
	AWSS3Selector,
	LocalDirectorySelector,
}

var allowedSelectors = map[string][]selectPromptData{
	AWSS3Selector.Value:          {CommaSeparatedValuesFilesSelector, IndividualBinaryFilesSelector},
	LocalDirectorySelector.Value: {CommaSeparatedValuesFilesSelector, IndividualBinaryFilesSelector},
}

var csvDelimiterSelectors = []selectPromptData{
	CommasSelector,
	TabsSelector,
}

type IngestManifest struct {
	selectedDatasourceType selectPromptData
	DatasourceFormatConfig datarepo.ManifestConfig `yaml:"datasourceType"`

	selectedDatasourceLocation selectPromptData
	DatasourceLocationConfig   datarepo.ManifestConfig `yaml:"datasourceLocation"`
}

func NewCSVFilesFormatConfigFromPrompts() (*datarepo.CSVFilesFormatConfig, error) {
	config := datarepo.CSVFilesFormatConfig{}
	result, err := SelectPrompt(
		"Delimiter",
		"Columns in each file are delimited by this character",
		csvDelimiterSelectors,
	)
	if err != nil {
		return nil, err
	}
	config.Delimiter = result.Value
	headerResult, err := BoolPrompt("CSV files contain header row")
	if err != nil {
		return nil, err
	}
	config.Header = headerResult
	return &config, nil
}

func NewAWSS3LocationConfigFromPrompts() (*datarepo.AWSS3LocationConfig, error) {
	config := datarepo.AWSS3LocationConfig{}
	{
		result, err := TextPrompt("AWS S3 Bucket")
		if err != nil {
			return nil, err
		}
		config.Bucket = result
	}
	{
		result, err := TextPrompt("AWS S3 Prefix")
		if err != nil {
			return nil, err
		}
		config.Prefix = result
	}
	return &config, nil
}

func getDatarepoIdentifiers() (string, string, error) {
	datarepoName, err := TextPrompt("Name of Datarepo")
	if err != nil {
		return "", "", err
	}
	datarepoVersion, err := TextPrompt("Version of Datarepo")
	return datarepoName, datarepoVersion, err
}

// Builds the configuration for the DatasourceType
func (manifest *IngestManifest) buildDatasourceFormatConfig() error {
	selectors := allowedSelectors[manifest.selectedDatasourceLocation.Value]
	result, err := SelectPrompt(
		"Data format",
		"Choose how your data is laid out.",
		selectors,
	)

	if err != nil {
		return err
	}
	switch result.Value {
	case IndividualBinaryFilesSelector.Value:
		return errors.New("individual binary files not yet supported")
	case CommaSeparatedValuesFilesSelector.Value:
		config, err := NewCSVFilesFormatConfigFromPrompts()
		if err != nil {
			return err
		}
		manifest.selectedDatasourceType = result
		manifest.DatasourceFormatConfig = config
		return nil
	case DatabaseTableSelector.Value:
		return errors.New("database tables not yet supported")
	default:
		return fmt.Errorf("datasource type %s not supported", result)
	}
}

// Builds the configuration for the DatasourceLocation
func (manifest *IngestManifest) buildDatasourceLocationConfig() error {
	result, err := SelectPrompt(
		"Data Source",
		"Specify the source for importing data from.",
		locationSelectors,
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
}

func buildDatasourceLocationConfigForSelectedLocation(location selectPromptData) (datarepo.ManifestConfig, error) {
	switch location {
	case LocalDirectorySelector:
		return nil, errors.New("local directories not yet supported")
	case AWSS3Selector:
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
	result, err := BoolPrompt("Confirm and detect schema")
	if err != nil {
		return err
	}
	if !result {
		return errors.New("user cancelled data source configurations")
	}
	return nil
}

func (manifest *IngestManifest) buildDatarepoSchema() error {
	sampler, err := sample.SamplerFactory(manifest.DatasourceFormatConfig, manifest.DatasourceLocationConfig)
	if err != nil {
		return err
	}
	sampledSchema, err := sampler.SampleSchema()
	if err != nil {
		return err
	}
	tablePreview, err := PreviewSamples(sampledSchema, sampler)
	if err != nil {
		return err
	}

	yamlSchema, err := yaml.Marshal(sampledSchema)
	if err != nil {
		return err
	}
	finalizedSchemaStr, err := EditorPrompt(SchemaEditorTutorialBlurb+tablePreview+string(yamlSchema), "yaml")
	if err != nil {
		return err
	}
	finalizedSchema := schema.Schema{}
	err = yaml.Unmarshal([]byte(finalizedSchemaStr), &finalizedSchema)
	if err != nil {
		return err
	}

	fmt.Println("Final Schema:")
	finalSchemaDisplay, err := yaml.Marshal(finalizedSchema)
	if err != nil {
		return err
	}
	fmt.Println(string(finalSchemaDisplay))
	confirmSchema, err := BoolPrompt("Confirm finalized schema")
	if err != nil {
		return err
	}
	if !confirmSchema {
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

		datarepoName, datarepoVersion, err := getDatarepoIdentifiers()
		cobra.CheckErr(err)

		err = manifest.buildDatasourceLocationConfig()
		cobra.CheckErr(err)

		err = manifest.buildDatasourceFormatConfig()
		cobra.CheckErr(err)

		err = manifest.confirmDatasourceConfigs()
		cobra.CheckErr(err)

		err = manifest.buildDatarepoSchema()
		cobra.CheckErr(err)

		ingestor, err := ingest.NewLocalIngestor(
			datarepoName,
			datarepoVersion,
			&datarepo.S3DatarepoConfig{
				S3bucket: config.DatarepoS3Bucket,
				S3prefix: config.DatarepoS3Prefix,
			},
			manifest.DatasourceFormatConfig,
			manifest.DatasourceLocationConfig,
		)
		cobra.CheckErr(err)
		jobId, err := ingestor.Ingest(cmd.Context())
		cobra.CheckErr(err)
		fmt.Printf("Ingest job started: %s\n", jobId)
	},
}
