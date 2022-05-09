package ingest

import (
	"fmt"

	"github.com/Eventual-Inc/Daft/pkg/datarepo"
	"github.com/Eventual-Inc/Daft/pkg/datarepo/sample"
	"github.com/sirupsen/logrus"
)

type IngestJobID = string

type DatarepoIngestor interface {
	// Ingests the configured Datasource as a Datarepo, returning a Job ID and errors
	Ingest() (IngestJobID, error)
}

// A Datarepo Ingestor that runs locally using local resources and permissions
type LocalIngestor struct {
	sampler sample.Sampler
}

func (ingestor *LocalIngestor) Ingest() (IngestJobID, error) {
	sampledSchema, err := ingestor.sampler.SampleSchema()
	if err != nil {
		return "", err
	}

	rowChannel := make(chan map[string][]byte)
	go func() {
		err := ingestor.sampler.SampleRows(
			rowChannel,
			// TODO(jaychia): Re-enable when we want to actually sample the entire dataset
			// sample.WithSampleAll(),
			sample.WithSchema(sampledSchema),
		)
		if err != nil {
			logrus.Error(fmt.Errorf("error while sampling rows: %w", err))
		}
		close(rowChannel)
	}()

	for row := range rowChannel {
		fmt.Println(row)
	}
	return "FOO", nil
}

func NewLocalIngestor(formatConfig datarepo.ManifestConfig, locationConfig datarepo.ManifestConfig) (DatarepoIngestor, error) {
	dataSampler, err := sample.SamplerFactory(formatConfig, locationConfig)
	if err != nil {
		return nil, err
	}
	return &LocalIngestor{sampler: dataSampler}, nil
}
