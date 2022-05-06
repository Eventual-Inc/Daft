package cmd

import (
	"fmt"
	"strings"

	"github.com/Eventual-Inc/Daft/pkg/schema"
)

const MaxCharPerCol = 16

func transpose(slice [][]string) [][]string {
	xl := len(slice[0])
	yl := len(slice)
	result := make([][]string, xl)
	for i := range result {
		result[i] = make([]string, yl)
	}
	for i := 0; i < xl; i++ {
		for j := 0; j < yl; j++ {
			result[i][j] = slice[j][i]
		}
	}
	return result
}

func PreviewSamples(samples map[string]SampleResult) (string, error) {
	var headers []string
	columns := make([][]string, len(samples))
	colIdx := 0
	for header, sampleResult := range samples {
		columns[colIdx] = make([]string, len(sampleResult.sampledDataRows))
		headers = append(headers, header)
		schemaType := sampleResult.InferredSchema.Type
		for rowIdx, cell := range sampleResult.sampledDataRows {
			var val string
			switch schemaType {
			case string(schema.StringAvroType):
				val = string(cell)
			default:
				return "", fmt.Errorf("previewing %s not implemented", schemaType)
			}
			columns[colIdx][rowIdx] = val
		}
		colIdx++
	}
	rows := transpose(columns)
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
