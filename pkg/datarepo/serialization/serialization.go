package serialization

import (
	"fmt"
	"io"

	"github.com/Eventual-Inc/Daft/pkg/datarepo/schema"
	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
	"github.com/apache/arrow/go/arrow/ipc"
	"github.com/apache/arrow/go/arrow/memory"
)

// Serializes rows into files
type Serializer interface {
	// Adds a row to the serializer, returning the number of bytes added
	AddRow(row [][]byte) (n int, err error)
	// Flushes all added rows as serialized bytes into the provided Writer
	Flush(out io.Writer) error
}

type ArrowSerializer struct {
	pool         *memory.GoAllocator
	arrowBuilder *array.RecordBuilder
	arrowSchema  arrow.Schema
}

func NewArrowSerializer(daftSchema schema.Schema) ArrowSerializer {
	arrowSchema := daftSchema.ArrowSchema()
	pool := memory.NewGoAllocator()
	arrowBuilder := array.NewRecordBuilder(pool, arrowSchema)
	return ArrowSerializer{
		pool:         pool,
		arrowBuilder: arrowBuilder,
		arrowSchema:  *arrowSchema,
	}
}

func (ser *ArrowSerializer) AddRow(row [][]byte) (int, error) {
	n := 0
	for i, val := range row {
		field := ser.arrowSchema.Field(i)
		switch field.Type {
		case arrow.BinaryTypes.String:
			ser.arrowBuilder.Field(i).(*array.StringBuilder).Append(string(val))
		default:
			return n, fmt.Errorf("unimplemented method to write data of type %s to Arrow", arrow.BinaryTypes.String)
		}
		n += len(val)
	}
	return n, nil
}

func (ser *ArrowSerializer) Flush(out io.Writer) error {
	rec := ser.arrowBuilder.NewRecord()
	defer rec.Release()
	defer ser.arrowBuilder.Release()
	defer func() {
		ser.arrowBuilder = array.NewRecordBuilder(ser.pool, &ser.arrowSchema)
	}()

	writer := ipc.NewWriter(out, ipc.WithSchema(&ser.arrowSchema))
	err := writer.Write(rec)
	if err != nil {
		return err
	}
	return nil
}
