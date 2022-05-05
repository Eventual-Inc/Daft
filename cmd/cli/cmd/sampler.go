package cmd

import (
	"bytes"
	"context"
	"encoding/csv"
	"fmt"
	"strings"

	"github.com/Eventual-Inc/Daft/pkg/objectstorage"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/sirupsen/logrus"
)

type Sampler interface {
	Sample() ([]map[string]string, error)
}

type CSVSampler struct {
	objectStore objectstorage.ObjectStore
	delimiter   rune
	fullDirPath string
}

func zipMap(a, b []string) map[string]string {
	r := make(map[string]string, len(a))
	for i, aval := range a {
		r[aval] = b[i]
	}
	return r
}

func (sampler *CSVSampler) Sample() ([]map[string]string, error) {
	ctx := context.TODO()
	objectPaths, err := sampler.objectStore.ListObjects(ctx, sampler.fullDirPath)
	if err != nil {
		return nil, err
	}
	sampledRows := []map[string]string{}
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
		header, err := reader.Read()
		if err != nil {
			return sampledRows, fmt.Errorf("unable to read header from CSV file: %w", err)
		}
		record, err := reader.Read()
		if err != nil {
			return sampledRows, fmt.Errorf("unable to read first record from CSV file: %w", err)
		}

		sampledRows = append(sampledRows, zipMap(header, record))
	}
	return sampledRows, nil
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
		}
		return sampler, nil
	default:
		return nil, fmt.Errorf("sampler for %s not implemented", typeConfig.Kind())
	}
}
