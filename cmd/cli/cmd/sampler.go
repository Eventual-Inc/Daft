package cmd

import (
	"bytes"
	"context"
	"encoding/csv"
	"fmt"
	"strings"

	"github.com/Eventual-Inc/Daft/pkg/objectstorage"
	"github.com/Eventual-Inc/Daft/pkg/schema"
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
	schemaHints []schema.SchemaField
}

type SampleResult struct {
	InferredSchema  schema.SchemaField
	sampledDataRows [][]byte
}

func (sampler *CSVSampler) Sample() (map[string]SampleResult, error) {
	ctx := context.TODO()
	objectPaths, err := sampler.objectStore.ListObjects(ctx, sampler.fullDirPath)
	if err != nil {
		return nil, err
	}
	samples := map[string]SampleResult{}
	for _, objPath := range objectPaths {
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

		// Use schema hints (if provided) to check if first row is a header row, or should be treated as a data row
		isHeaderRow := true
		record, err := reader.Read()
		if err != nil {
			return samples, fmt.Errorf("unable to read header from CSV file: %w", err)
		}
		if len(sampler.schemaHints) > 0 {
			if len(record) != len(sampler.schemaHints) {
				return samples, fmt.Errorf("found CSV file with %d columns, expecting: %d", len(record), len(sampler.schemaHints))
			}
			for i, schemaHint := range sampler.schemaHints {
				if foundHeader := record[i]; foundHeader != schemaHint.Name {
					isHeaderRow = false
				}
			}
		}

		// If no schema hint is provided, we detect all header fields as strings
		// TODO(jchia): We can be smarter about detection here with regexes
		detectedSchema := sampler.schemaHints
		if isHeaderRow && len(detectedSchema) == 0 {
			for _, columnName := range record {
				newField := schema.NewStringField(columnName, "Detected column from CSV")
				detectedSchema = append(detectedSchema, newField)
			}
		}

		// Grab the next row as the data row if the first row is a header row
		if isHeaderRow {
			record, err = reader.Read()
			if err != nil {
				return samples, fmt.Errorf("unable to read first record from CSV file: %w", err)
			}
		}

		for i, field := range detectedSchema {
			currentSample := samples[field.Name]
			currentSample.InferredSchema = field
			currentSample.sampledDataRows = append(currentSample.sampledDataRows, []byte(record[i]))
			samples[field.Name] = currentSample
		}
	}
	return samples, nil
}

func objectStoreFactory(locationConfig ManifestConfig) (objectstorage.ObjectStore, error) {
	switch locationConfig.Kind() {
	case AWSS3Selector.Value:
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

func getFullDirPath(locationConfig ManifestConfig) (string, error) {
	switch locationConfig.Kind() {
	case AWSS3Selector.Value:
		config := locationConfig.(*AWSS3LocationConfig)
		return fmt.Sprintf("s3://%s/%s", config.Bucket, config.Prefix), nil
	default:
		return "", fmt.Errorf("object store for %s not implemented", locationConfig.Kind())
	}
}

func SamplerFactory(typeConfig ManifestConfig, locationConfig ManifestConfig) (Sampler, error) {
	switch typeConfig.Kind() {
	case CommaSeparatedValuesFilesSelector.Value:
		config := typeConfig.(*CSVFilesTypeConfig)
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
			delimiter:   config.Delimiter,
			schemaHints: config.SchemaHints(),
		}
		return sampler, nil
	default:
		return nil, fmt.Errorf("sampler for %s not implemented", typeConfig.Kind())
	}
}
