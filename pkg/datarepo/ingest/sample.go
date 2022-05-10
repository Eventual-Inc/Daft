package ingest

import (
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/Eventual-Inc/Daft/pkg/datarepo"
	"github.com/Eventual-Inc/Daft/pkg/datarepo/schema"
	"github.com/Eventual-Inc/Daft/pkg/objectstorage"
	"github.com/sirupsen/logrus"
)

// A Sampler retrieves data from a given Datasource
type Sampler interface {
	SampleSchema(ctx context.Context) (schema.Schema, error)
	SampleRows(ctx context.Context, outputChannel chan [][]byte, opts ...SamplingOpt) error
}

// Sampler that retrieves data from CSV file(s)
type CSVSampler struct {
	objectStore objectstorage.ObjectStore
	delimiter   rune
	fullDirPath string
	hasHeaders  bool
}

type samplingOptions struct {
	// Number of rows to sample, or 0 to sample all data
	numRows int

	// Schema to use, or nil if not provided and need to detect
	schemaFields []schema.SchemaField
}

type SamplingOpt = func(*samplingOptions)

// Use this option to sample all rows in the specified datasources
func WithSampleAll() SamplingOpt {
	return func(opt *samplingOptions) {
		opt.numRows = 0
	}
}

// Use this option to provide a schema to the Sampler, instead of autodetecting it
func WithSchema(usingSchema schema.Schema) SamplingOpt {
	return func(opt *samplingOptions) {
		opt.schemaFields = usingSchema.Fields
	}
}

func (sampler *CSVSampler) SampleRows(ctx context.Context, outputChannel chan [][]byte, opts ...SamplingOpt) error {
	// Default to sampling 10 rows of data
	samplingOptions := samplingOptions{numRows: 10}
	for _, opt := range opts {
		opt(&samplingOptions)
	}

	objectPaths, err := sampler.objectStore.ListObjects(ctx, sampler.fullDirPath)
	if err != nil {
		return err
	}
	if len(objectPaths) == 0 {
		return errors.New("no objects found at specified location")
	}

	// Use schema if provided as an opt, otherwise detect it first
	detectedSchema := schema.Schema{Fields: samplingOptions.schemaFields}
	if len(detectedSchema.Fields) == 0 {
		detectedSchema, err = sampler.SampleSchema(ctx)
		if err != nil {
			return err
		}
	}

	// If sampling N number of rows, we limit the downloads to just the top 100KB * (N/num_files) amount of bytes
	var downloadOptions []objectstorage.DownloadObjectOption
	numRowsPerFile := 0
	if samplingOptions.numRows != 0 {
		numRowsPerFile = samplingOptions.numRows / len(objectPaths)
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

		// TODO(jaychia): We can stream this instead for better memory usage of downloading it one chunk
		// Download object and start reading with a CSV Reader
		objBody, err := sampler.objectStore.DownloadObject(ctx, objPath, downloadOptions...)
		if err != nil {
			return fmt.Errorf("unable to download object from AWS S3: %w", err)
		}
		reader := csv.NewReader(objBody)
		reader.Comma = sampler.delimiter

		// Skip first row if hasHeaders
		if sampler.hasHeaders {
			_, err := reader.Read()
			if err != nil {
				return err
			}
		}

		for i := 0; i < numRowsPerFile || numRowsPerFile == 0; i++ {
			record, err := reader.Read()
			if err == io.EOF {
				break
			}
			if err != nil {
				return err
			}
			var row [][]byte
			for i, _ := range detectedSchema.Fields {
				row = append(row, []byte(record[i]))
			}
			outputChannel <- row
		}

	}
	return nil
}

func (sampler *CSVSampler) SampleSchema(ctx context.Context) (schema.Schema, error) {
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

		// Download 100KB worth of data, assumes that header wont exceed that size
		// TODO(jaychia): We can download until retrieving a \n instead
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

func getFullDirPath(locationConfig datarepo.ManifestConfig) (string, error) {
	switch locationConfig.Kind() {
	case datarepo.DatasourceIDAWSS3:
		config := locationConfig.(*datarepo.AWSS3LocationConfig)
		return fmt.Sprintf("s3://%s/%s", config.Bucket, config.Prefix), nil
	default:
		return "", fmt.Errorf("getting directory path for %s not implemented", locationConfig.Kind())
	}
}

// Creates the appropriate Sampler when provided with format and location configs
func SamplerFactory(formatConfig datarepo.ManifestConfig, locationConfig datarepo.ManifestConfig) (Sampler, error) {
	switch formatConfig.Kind() {
	case datarepo.DataformatIDCSVFiles:
		config := formatConfig.(*datarepo.CSVFilesFormatConfig)
		objectStore, err := datarepo.ObjectStoreFactory(locationConfig)
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
