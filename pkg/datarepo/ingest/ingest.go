package ingest

import (
	"bytes"
	"context"
	"fmt"

	"github.com/Eventual-Inc/Daft/pkg/datarepo"
	"github.com/Eventual-Inc/Daft/pkg/datarepo/schema"
	"github.com/Eventual-Inc/Daft/pkg/objectstorage"
	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
	"github.com/apache/arrow/go/arrow/ipc"
	"github.com/apache/arrow/go/arrow/memory"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
)

var DaftFieldTypeToArrowType = map[string]arrow.DataType{
	schema.StringType: arrow.BinaryTypes.String,
}

func AppendToArrowBuilder(arrowBuilder *array.RecordBuilder, fieldIdx int, daftType schema.TypeEnum, val []byte) error {
	switch daftType {
	case schema.StringType:
		arrowBuilder.Field(fieldIdx).(*array.StringBuilder).Append(string(val))
	default:
		return fmt.Errorf("unable to map type %s to an Arrow builder", daftType)
	}
	return nil
}

type IngestJobID = string

type DatarepoIngestor interface {
	// Ingests the configured Datasource as a Datarepo, returning a Job ID and errors
	Ingest(ctx context.Context) (IngestJobID, error)
}

// A Datarepo Ingestor that runs locally using local resources and permissions
type LocalIngestor struct {
	datarepoName    string
	datarepoVersion string
	datarepoConfig  datarepo.DatarepoConfig
	datarepoSchema  schema.Schema
	sampler         Sampler
	store           objectstorage.ObjectStore
}

func (ingestor *LocalIngestor) uploadData(ctx context.Context, arrowBuilder *array.RecordBuilder) error {
	rec := arrowBuilder.NewRecord()
	defer rec.Release()
	defer arrowBuilder.Release()

	partId := uuid.New().String()
	path := ingestor.datarepoConfig.GetPartfilePath(ingestor.datarepoName, ingestor.datarepoVersion, partId)

	buf := bytes.Buffer{}
	writer := ipc.NewWriter(&buf, ipc.WithSchema(rec.Schema()))
	err := writer.Write(rec)
	if err != nil {
		return err
	}

	err = ingestor.store.UploadObject(ctx, path, &buf)
	return err
}

func (ingestor *LocalIngestor) Ingest(ctx context.Context) (IngestJobID, error) {
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

	var arrowFields []arrow.Field
	for _, field := range ingestor.datarepoSchema.Fields {
		arrowFields = append(arrowFields, arrow.Field{Name: field.Name, Type: DaftFieldTypeToArrowType[field.Type]})
	}
	arrowSchema := arrow.NewSchema(arrowFields, nil)
	pool := memory.NewGoAllocator()

	// TODO(jaychia): We could instead do a new batch every 100MB
	// Create a new arrow batch every 100,000 records
	rowCount := 0
	arrowBuilder := array.NewRecordBuilder(pool, arrowSchema)
	for row := range rowChannel {
		for i, cell := range row {
			AppendToArrowBuilder(arrowBuilder, i, ingestor.datarepoSchema.Fields[i].Type, cell)
		}
		if rowCount%100000 == 99999 {
			ingestor.uploadData(ctx, arrowBuilder)
			pool = memory.NewGoAllocator()
			arrowBuilder = array.NewRecordBuilder(pool, arrowSchema)
		}
		rowCount++
	}
	if rowCount%100000 != 99999 {
		ingestor.uploadData(ctx, arrowBuilder)
	}
	return "nil - datarepo ingestion complete", nil
}

func NewLocalIngestor(
	datarepoName string,
	datarepoVersion string,
	datarepoConfig datarepo.DatarepoConfig,
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
		datarepoConfig:  datarepoConfig,
		datarepoSchema:  datarepoSchema,
		sampler:         dataSampler,
		store:           objectStore,
	}, nil
}
