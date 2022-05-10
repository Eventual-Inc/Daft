package ingest

import (
	"bytes"
	"context"
	"fmt"
	"io"

	"github.com/Eventual-Inc/Daft/pkg/datarepo"
	"github.com/Eventual-Inc/Daft/pkg/datarepo/schema"
	"github.com/Eventual-Inc/Daft/pkg/datarepo/serialization"
	"github.com/Eventual-Inc/Daft/pkg/objectstorage"
	"github.com/sirupsen/logrus"
)

// Size in bytes of the threshold for each partfile
const PartfileSizeThreshold = 100 * 1000 * 1000

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

func (ingestor *LocalIngestor) flushSerializerToPartfile(ctx context.Context, serializer serialization.Serializer) error {
	reader, writer := io.Pipe()
	errChan := make(chan error)
	go func() {
		err := serializer.Flush(writer)
		writer.CloseWithError(err)
		errChan <- err
	}()

	// HACK(jaychia): The AWS S3 APIs seem to be failing weirdly with io.PipeReader and reading 0 bytes of data. It seems that
	// the API expects all the data to be ready before it is called, instead of properly terminating at an EOF...
	// Hence, we buffer in memory here by reading all the data into an in-memory []byte buffer before sending it to the S3 client.
	// This could be optimized if we do uploading by parts instead.
	b, err := io.ReadAll(reader)
	if err != nil {
		return err
	}
	bytesBufferReader := bytes.NewReader(b)

	partId, err := ingestor.datarepoClient.WritePartfile(ctx, ingestor.datarepoName, ingestor.datarepoVersion, bytesBufferReader)
	if err != nil {
		return fmt.Errorf("error occurred with writing partfile %s : %w", partId, err)
	}
	err = <-errChan
	if err != nil {
		return err
	}
	if err != nil {
		return err
	}
	return nil
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

	serializer := serialization.NewArrowSerializer(ingestor.datarepoSchema)
	totalBytesBuffered := 0
	for row := range rowChannel {
		bytesBuffered, err := serializer.AddRow(row)
		if err != nil {
			return "", err
		}
		// Upload data if PartfileSizeThreshold threshold reached
		totalBytesBuffered += bytesBuffered
		if totalBytesBuffered > PartfileSizeThreshold {
			totalBytesBuffered = 0
			err = ingestor.flushSerializerToPartfile(ctx, &serializer)
			if err != nil {
				return "", err
			}
		}
	}

	// Upload remainder of data
	if totalBytesBuffered != 0 {
		err := ingestor.flushSerializerToPartfile(ctx, &serializer)
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
