package sampler

import (
	"bytes"
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

type Sampler interface {
	Sample() (map[string]SampleResult, error)
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
}

type SamplingOption = func(*SamplingOptions)

func (sampler *CSVSampler) Sample() (map[string]SampleResult, error) {
	ctx := context.TODO()
	objectPaths, err := sampler.objectStore.ListObjects(ctx, sampler.fullDirPath)
	if err != nil {
		return nil, err
	}
	samples := map[string]SampleResult{}

	// Track headers from the first file we find. If all files have the same set of headers,
	// we assume that these are the right set of headers. Otherwise, we pre-populate the schema
	// with N_COLs generically named StringSchemas.
	var detectedSchema []schema.SchemaField

	for i, objPath := range objectPaths {
		// Skip files that are not CSV or TSV
		if !strings.HasSuffix(objPath, ".csv") && !strings.HasSuffix(objPath, ".tsv") {
			logrus.Debug(fmt.Sprintf("Skipping non-CSV file: %s", objPath))
			continue
		}
		buf := bytes.Buffer{}
		// TODO(jaychia): Hardcoded to retrieve 100kb of data at the moment, but could be better
		sampler.objectStore.DownloadObject(ctx, objPath, &buf, objectstorage.WithDownloadRange(0, 100000))
		reader := csv.NewReader(bytes.NewReader(buf.Bytes()))
		reader.Comma = sampler.delimiter

		// Parse or generate headers using first file found
		record, err := reader.Read()
		if err != nil {
			return samples, fmt.Errorf("unable to read header from CSV file: %w", err)
		}
		if i == 0 && sampler.hasHeaders {
			for _, fieldName := range record {
				detectedSchema = append(detectedSchema, schema.NewPrimitiveField(
					fieldName,
					"",
					schema.StringType,
				))
			}
		} else if i == 0 {
			for i := 0; i < len(record); i++ {
				detectedSchema = append(
					detectedSchema,
					schema.NewPrimitiveField(
						fmt.Sprintf("col_%d", i),
						"",
						schema.StringType,
					),
				)
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
