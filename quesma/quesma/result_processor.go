package quesma

import (
	"mitmproxy/quesma/model"
	"strings"
)

// API

type ResultProcessor interface {
	Process(query model.Query, rows []model.QueryResultRow) ([]model.QueryResultRow, error)
}

// Factory

func FindResultProcessor(name string) ResultProcessor {
	switch name {
	case "to_elasticsearch_field_names":
		fn := func(input string) string {
			return strings.ReplaceAll(input, "::", ".")
		}
		return &translateFieldNameResultProcessor{translate: fn}
	default:
		return nil
	}
}

// implementations

type translateFieldNameResultProcessor struct {
	translate func(input string) string
}

func (processor translateFieldNameResultProcessor) Process(query model.Query, rows []model.QueryResultRow) ([]model.QueryResultRow, error) {
	for i, row := range rows {
		for j, col := range row.Cols {
			rows[i].Cols[j].ColName = processor.translate(col.ColName)
		}
	}
	return rows, nil
}
