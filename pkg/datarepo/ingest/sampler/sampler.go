package sampler

import (
	"context"
	"encoding/csv"
	"fmt"
	"strings"

	"github.com/Eventual-Inc/Daft/pkg/datarepo/ingest"
	"github.com/Eventual-Inc/Daft/pkg/datarepo/schema"
	"github.com/Eventual-Inc/Daft/pkg/objectstorage"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/sirupsen/logrus"
)

// A Sampler retrieves data, when provided with a Datasource and Data format
type Sampler interface {
	Sample(opts ...SamplingOpt) (map[string]SampleResult, error)
}

type CSVSampler struct {
	objectStore objectstorage.ObjectStore
	delimiter   rune
	fullDirPath string
	hasHeaders  bool
}

type SampleResult struct {
	InferredSchema  schema.SchemaField
	SampledDataRows [][]byte
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

func WithSchema(schemaFields []schema.SchemaField) SamplingOpt {
	return func(opt *SamplingOptions) {
		opt.schemaFields = schemaFields
	}
}

func (sampler *CSVSampler) Sample(opts ...SamplingOpt) (map[string]SampleResult, error) {
	// Default to sampling 10 rows of data
	samplingOptions := SamplingOptions{numRows: 10}
	for _, opt := range opts {
		opt(&samplingOptions)
	}

	ctx := context.TODO()
	objectPaths, err := sampler.objectStore.ListObjects(ctx, sampler.fullDirPath)
	if err != nil {
		return nil, err
	}
	samples := map[string]SampleResult{}

	// Use schema if provided as an opt, otherwise default to detecting it from the 0th CSV file
	var detectedSchema []schema.SchemaField
	if len(samplingOptions.schemaFields) > 0 {
		detectedSchema = samplingOptions.schemaFields
	}

	// If sampling N number of rows, we limit the downloads to just the top 100KB * (N/num_files) amount of bytes
	var downloadOptions []objectstorage.DownloadObjectOption
	if samplingOptions.numRows != 0 {
		numRowsPerFile := samplingOptions.numRows / len(objectPaths)
		sizePerRow := 100000
		sizePerFile := sizePerRow * numRowsPerFile
		downloadOptions = append(downloadOptions, objectstorage.WithDownloadRange(0, sizePerFile))
	}

	for i, objPath := range objectPaths {
		// Skip files that are not CSV or TSV
		if !strings.HasSuffix(objPath, ".csv") && !strings.HasSuffix(objPath, ".tsv") {
			logrus.Debug(fmt.Sprintf("Skipping non-CSV file: %s", objPath))
			continue
		}
		objBody, err := sampler.objectStore.DownloadObject(ctx, objPath, downloadOptions...)
		if err != nil {
			return samples, fmt.Errorf("unable to download object from AWS S3: %w", err)
		}
		reader := csv.NewReader(objBody)
		reader.Comma = sampler.delimiter

		// Parse or generate headers using first file found
		record, err := reader.Read()
		if err != nil {
			return samples, fmt.Errorf("unable to read header from CSV file: %w", err)
		}
		if i == 0 && len(detectedSchema) == 0 {
			for i, cell := range record {
				fieldName := fmt.Sprintf("col_%d", i)
				if sampler.hasHeaders {
					fieldName = cell
				}
				detectedSchema = append(detectedSchema, schema.NewPrimitiveField(
					fieldName,
					"",
					schema.StringType,
				))
			}
		}

		// If headers specified, get next row which is the first data row
		if sampler.hasHeaders {
			record, err = reader.Read()
			if err != nil {
				return nil, fmt.Errorf("unable to read first data row from CSV file: %w", err)
			}
		}

		// If number of columns don't match the schema, throw an error
		if len(record) != len(detectedSchema) {
			return samples, fmt.Errorf("received %d number of columns but expected: %d", len(record), len(detectedSchema))
		}

		for i, field := range detectedSchema {
			currentSample := samples[field.Name]
			currentSample.InferredSchema = field
			currentSample.SampledDataRows = append(currentSample.SampledDataRows, []byte(record[i]))
			samples[field.Name] = currentSample
		}
	}
	return samples, nil
}

func objectStoreFactory(locationConfig ingest.ManifestConfig) (objectstorage.ObjectStore, error) {
	switch locationConfig.Kind() {
	case ingest.DatasourceIDAWSS3:
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

func getFullDirPath(locationConfig ingest.ManifestConfig) (string, error) {
	switch locationConfig.Kind() {
	case ingest.DatasourceIDAWSS3:
		config := locationConfig.(*ingest.AWSS3LocationConfig)
		return fmt.Sprintf("s3://%s/%s", config.Bucket, config.Prefix), nil
	default:
		return "", fmt.Errorf("object store for %s not implemented", locationConfig.Kind())
	}
}

func SamplerFactory(FormatConfig ingest.ManifestConfig, locationConfig ingest.ManifestConfig) (Sampler, error) {
	switch FormatConfig.Kind() {
	case ingest.DataformatIDCSVFiles:
		config := FormatConfig.(*ingest.CSVFilesFormatConfig)
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
			delimiter:   ingest.DelimiterMap[config.Delimiter],
			hasHeaders:  config.Header,
		}
		return sampler, nil
	default:
		return nil, fmt.Errorf("sampler for %s not implemented", FormatConfig.Kind())
	}
}
