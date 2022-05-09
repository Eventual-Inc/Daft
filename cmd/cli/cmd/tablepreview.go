package cmd

import (
	"fmt"
	"strings"

	"github.com/Eventual-Inc/Daft/pkg/datarepo/sample"
	"github.com/Eventual-Inc/Daft/pkg/datarepo/schema"
	"github.com/sirupsen/logrus"
)

const MaxCharPerCol = 16

func PreviewSamples(sampledSchema schema.Schema, sampler sample.Sampler) (string, error) {
	var headers []string
	var rows [][]string
	for _, header := range sampledSchema.Fields {
		headers = append(headers, header.Name)
	}

	rowChannel := make(chan map[string][]byte)

	go func() {
		err := sampler.SampleRows(rowChannel, sample.WithSchema(sampledSchema))
		if err != nil {
			logrus.Error(fmt.Errorf("error while sampling rows: %w", err))
		}
		close(rowChannel)
	}()

	for row := range rowChannel {
		var parsedRow []string
		for _, schemaField := range sampledSchema.Fields {
			parsedRow = append(parsedRow, string(row[schemaField.Name]))
		}
		rows = append(rows, parsedRow)
	}
	rows = append([][]string{headers}, rows...)

	var stringRows []string
	for _, row := range rows {
		stringRow := "# "
		for _, cell := range row {
			truncatedCell := fmt.Sprintf("%*.*s", MaxCharPerCol, MaxCharPerCol, cell)
			stringRow += " | "
			stringRow += truncatedCell
		}
		stringRows = append(stringRows, stringRow)
	}
	return strings.Join(stringRows, "\n") + "\n\n", nil
}
