package ingest

import (
	"context"
	"fmt"

	"github.com/Eventual-Inc/Daft/pkg/datarepo"
	"github.com/Eventual-Inc/Daft/pkg/datarepo/schema"
	"github.com/Eventual-Inc/Daft/pkg/objectstorage"
	"github.com/apache/arrow/go/arrow/array"
	"github.com/apache/arrow/go/arrow/memory"
	"github.com/sirupsen/logrus"
)

// TODO(jaychia): This is currently hardcoded, but could instead be dynamically sampled
const RowsPerPartfile = 1000000

type IngestJobID = string

// Interface for sampling data from a configured Datasource and ingesting it as a new Datarepo
type DatarepoIngestor interface {
	// Ingests the configured Datasource as a Datarepo, returning a Job ID and errors
	Ingest(ctx context.Context) (IngestJobID, error)
}

// A Datarepo Ingestor that runs locally using local resources and permissions
type LocalIngestor struct {
	datarepoName    string
	datarepoVersion string
	datarepoSchema  schema.Schema
	datarepoClient  datarepo.StorageClient
	sampler         Sampler
	store           objectstorage.ObjectStore
}

func (ingestor *LocalIngestor) Ingest(ctx context.Context) (IngestJobID, error) {
	// Write schema
	ingestor.datarepoClient.WriteSchema(ctx, ingestor.datarepoName, ingestor.datarepoVersion, ingestor.datarepoSchema)

	// Construct a channel of Rows and start sampling all data into this channel
	rowChannel := make(chan [][]byte)
	go func() {
		err := ingestor.sampler.SampleRows(
			ctx,
			rowChannel,
			WithSampleAll(),
			WithSchema(ingestor.datarepoSchema),
		)
		if err != nil {
			logrus.Error(fmt.Errorf("error while sampling rows: %w", err))
		}
		close(rowChannel)
	}()

	// Create a new arrow batch every 1,000,000 records
	// TODO(jaychia): We could instead do a new batch every 100MB
	arrowSchema := ingestor.datarepoSchema.ArrowSchema()
	pool := memory.NewGoAllocator()
	arrowBuilder := array.NewRecordBuilder(pool, arrowSchema)
	rowCount := 0
	for row := range rowChannel {
		for i, cell := range row {
			fieldType := ingestor.datarepoSchema.Fields[i].Type
			switch fieldType {
			case schema.StringType:
				arrowBuilder.Field(i).(*array.StringBuilder).Append(string(cell))
			default:
				return "", fmt.Errorf("unable to map type %s to an Arrow builder", fieldType)
			}
		}
		// Upload data if RowsPerPartfile threshold reached
		if rowCount%RowsPerPartfile == (RowsPerPartfile - 1) {
			rec := arrowBuilder.NewRecord()
			_, err := ingestor.datarepoClient.WritePartfile(ctx, ingestor.datarepoName, ingestor.datarepoVersion, &rec)
			rec.Release()
			arrowBuilder.Release()
			if err != nil {
				return "", err
			}
			arrowBuilder = array.NewRecordBuilder(pool, arrowSchema)
		}
		rowCount++
	}

	// Upload remainder of data
	if rowCount%RowsPerPartfile != (RowsPerPartfile - 1) {
		rec := arrowBuilder.NewRecord()
		_, err := ingestor.datarepoClient.WritePartfile(ctx, ingestor.datarepoName, ingestor.datarepoVersion, &rec)
		rec.Release()
		arrowBuilder.Release()
		if err != nil {
			return "", err
		}
	}

	return "datarepo ingestion was performed locally", nil
}

func NewLocalIngestor(
	datarepoName string,
	datarepoVersion string,
	datarepoClient datarepo.StorageClient,
	formatConfig datarepo.ManifestConfig,
	locationConfig datarepo.ManifestConfig,
	datarepoSchema schema.Schema,
) (DatarepoIngestor, error) {
	dataSampler, err := SamplerFactory(formatConfig, locationConfig)
	if err != nil {
		return nil, err
	}
	objectStore, err := datarepo.ObjectStoreFactory(locationConfig)
	if err != nil {
		return nil, err
	}
	return &LocalIngestor{
		datarepoName:    datarepoName,
		datarepoVersion: datarepoVersion,
		datarepoClient:  datarepoClient,
		datarepoSchema:  datarepoSchema,
		sampler:         dataSampler,
		store:           objectStore,
	}, nil
}
