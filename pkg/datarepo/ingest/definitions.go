package ingest

import "gopkg.in/yaml.v2"

// Data Sources
const DatasourceIDLocalDirectory = "local_dir"
const DatasourceIDAWSS3 = "aws_s3"

// Data Formats
const DataformatIDIndividualFiles = "individual_files"
const DataformatIDCSVFiles = "csv_files"
const DataformatIDDatabaseTable = "database_table"

// CSV Delimiters
const CSVDelimiterCommas = "commas"
const CSVDelimiterTabs = "tabs"

var DelimiterMap = map[string]rune{
	CSVDelimiterCommas: ',',
	CSVDelimiterTabs:   []rune("\t")[0],
}

// Config for an object in the manifest
type ManifestConfig interface {
	yaml.Marshaler
	Kind() string
}

// ------------------------
// Configs for Data Formats
// ------------------------

type CSVFilesFormatConfig struct {
	Delimiter string
	Header    bool
}

func (config *CSVFilesFormatConfig) MarshalYAML() (interface{}, error) {
	type s struct {
		Kind      string
		Delimiter string
	}
	return s{
		Kind:      DataformatIDCSVFiles,
		Delimiter: config.Delimiter,
	}, nil
}

func (config *CSVFilesFormatConfig) Kind() string {
	return DataformatIDCSVFiles
}

// ------------------------
// Configs for Data Sources
// ------------------------

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
		Kind:   DatasourceIDAWSS3,
		Bucket: config.Bucket,
		Prefix: config.Prefix,
	}, nil
}

func (config *AWSS3LocationConfig) Kind() string {
	return DatasourceIDAWSS3
}
