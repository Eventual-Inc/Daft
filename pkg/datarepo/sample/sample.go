package sample

import (
	"context"
	"encoding/csv"
	"fmt"
	"strings"

	"github.com/Eventual-Inc/Daft/pkg/datarepo"
	"github.com/Eventual-Inc/Daft/pkg/datarepo/schema"
	"github.com/Eventual-Inc/Daft/pkg/objectstorage"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/sirupsen/logrus"
)

// A Sampler retrieves data, when provided with a Datasource and Data format
type Sampler interface {
	SampleSchema() (schema.Schema, error)
	SampleRows(outputChannel chan map[string][]byte, opts ...SamplingOpt) error
}

type CSVSampler struct {
	objectStore objectstorage.ObjectStore
	delimiter   rune
	fullDirPath string
	hasHeaders  bool
}

type SampleResult struct {
	InferredSchema schema.Schema
	Rows           chan map[string][]byte
}

type SamplingOptions struct {
	// Number of rows to sample, or 0 to sample all data
	numRows int

	// Schema to use, or nil if not provided and need to detect
	schemaFields []schema.SchemaField
}

type SamplingOpt = func(*SamplingOptions)

func WithSampleAll() SamplingOpt {
	return func(opt *SamplingOptions) {
		opt.numRows = 0
	}
}

func WithSchema(usingSchema schema.Schema) SamplingOpt {
	return func(opt *SamplingOptions) {
		opt.schemaFields = usingSchema.Fields
	}
}

func (sampler *CSVSampler) SampleSchema() (schema.Schema, error) {
	ctx := context.TODO()
	sampledSchema := schema.Schema{}
	objectPaths, err := sampler.objectStore.ListObjects(ctx, sampler.fullDirPath)
	if err != nil {
		return sampledSchema, err
	}

	for _, objPath := range objectPaths {
		// Skip files that are not CSV or TSV
		if !strings.HasSuffix(objPath, ".csv") && !strings.HasSuffix(objPath, ".tsv") {
			logrus.Debug(fmt.Sprintf("Skipping non-CSV file: %s", objPath))
			continue
		}

		// TODO(jaychia): Download up to 100KB, assumes that header wont exceed that size
		objBody, err := sampler.objectStore.DownloadObject(ctx, objPath, objectstorage.WithDownloadRange(0, 100000))
		if err != nil {
			return sampledSchema, fmt.Errorf("unable to download object from AWS S3: %w", err)
		}
		reader := csv.NewReader(objBody)
		reader.Comma = sampler.delimiter

		// Parse or generate headers using first file found
		record, err := reader.Read()
		if err != nil {
			return sampledSchema, fmt.Errorf("unable to read header from CSV file: %w", err)
		}
		for i, cell := range record {
			fieldName := fmt.Sprintf("col_%d", i)
			if sampler.hasHeaders {
				fieldName = cell
			}
			sampledSchema.Fields = append(sampledSchema.Fields, schema.NewPrimitiveField(
				fieldName,
				"",
				schema.StringType,
			))
		}
		break
	}
	return sampledSchema, nil
}

func (sampler *CSVSampler) SampleRows(outputChannel chan map[string][]byte, opts ...SamplingOpt) error {
	// Default to sampling 10 rows of data
	samplingOptions := SamplingOptions{numRows: 10}
	for _, opt := range opts {
		opt(&samplingOptions)
	}

	ctx := context.TODO()
	objectPaths, err := sampler.objectStore.ListObjects(ctx, sampler.fullDirPath)
	if err != nil {
		return err
	}

	// Use schema if provided as an opt, otherwise detect it first
	detectedSchema := schema.Schema{Fields: samplingOptions.schemaFields}
	if len(detectedSchema.Fields) == 0 {
		detectedSchema, err = sampler.SampleSchema()
		if err != nil {
			return err
		}
	}

	// If sampling N number of rows, we limit the downloads to just the top 100KB * (N/num_files) amount of bytes
	var downloadOptions []objectstorage.DownloadObjectOption
	if samplingOptions.numRows != 0 {
		numRowsPerFile := samplingOptions.numRows / len(objectPaths)
		sizePerRow := 100000
		sizePerFile := sizePerRow * numRowsPerFile
		downloadOptions = append(downloadOptions, objectstorage.WithDownloadRange(0, sizePerFile))
	}

	for _, objPath := range objectPaths {
		// Skip files that are not CSV or TSV
		if !strings.HasSuffix(objPath, ".csv") && !strings.HasSuffix(objPath, ".tsv") {
			logrus.Debug(fmt.Sprintf("Skipping non-CSV file: %s", objPath))
			continue
		}
		objBody, err := sampler.objectStore.DownloadObject(ctx, objPath, downloadOptions...)
		if err != nil {
			return fmt.Errorf("unable to download object from AWS S3: %w", err)
		}
		reader := csv.NewReader(objBody)
		reader.Comma = sampler.delimiter

		// Parse or generate headers using first file found
		record, err := reader.Read()
		if sampler.hasHeaders {
			record, err = reader.Read()
		}
		if err != nil {
			return fmt.Errorf("unable to read row from CSV file: %w", err)
		}

		// If number of columns don't match the schema, throw an error
		if len(record) != len(detectedSchema.Fields) {
			return fmt.Errorf("received %d number of columns but expected: %d", len(record), len(detectedSchema.Fields))
		}

		row := make(map[string][]byte)
		for i, field := range detectedSchema.Fields {
			row[field.Name] = []byte(record[i])
		}
		outputChannel <- row
	}
	return nil
}

func objectStoreFactory(locationConfig datarepo.ManifestConfig) (objectstorage.ObjectStore, error) {
	switch locationConfig.Kind() {
	case datarepo.DatasourceIDAWSS3:
		ctx := context.TODO()
		awsConfig, err := config.LoadDefaultConfig(ctx, config.WithRegion("us-west-2"))
		if err != nil {
			return nil, err
		}
		return objectstorage.NewAwsS3ObjectStore(ctx, awsConfig), nil
	default:
		return nil, fmt.Errorf("object store for %s not implemented", locationConfig.Kind())
	}
}

func getFullDirPath(locationConfig datarepo.ManifestConfig) (string, error) {
	switch locationConfig.Kind() {
	case datarepo.DatasourceIDAWSS3:
		config := locationConfig.(*datarepo.AWSS3LocationConfig)
		return fmt.Sprintf("s3://%s/%s", config.Bucket, config.Prefix), nil
	default:
		return "", fmt.Errorf("object store for %s not implemented", locationConfig.Kind())
	}
}

func SamplerFactory(formatConfig datarepo.ManifestConfig, locationConfig datarepo.ManifestConfig) (Sampler, error) {
	switch formatConfig.Kind() {
	case datarepo.DataformatIDCSVFiles:
		config := formatConfig.(*datarepo.CSVFilesFormatConfig)
		objectStore, err := objectStoreFactory(locationConfig)
		if err != nil {
			return nil, err
		}
		fullDirPath, err := getFullDirPath(locationConfig)
		if err != nil {
			return nil, err
		}
		sampler := &CSVSampler{
			objectStore: objectStore,
			fullDirPath: fullDirPath,
			delimiter:   datarepo.DelimiterMap[config.Delimiter],
			hasHeaders:  config.Header,
		}
		return sampler, nil
	default:
		return nil, fmt.Errorf("sampler for %s not implemented", formatConfig.Kind())
	}
}
