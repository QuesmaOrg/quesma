package plugins

import (
	"mitmproxy/quesma/model"
	"mitmproxy/quesma/quesma/types"
)

type NopResultTransformer struct {
}

func (*NopResultTransformer) Transform(rows []model.QueryResultRow) ([]model.QueryResultRow, error) {
	return rows, nil
}

type NopFieldCapsTransformer struct {
}

func (*NopFieldCapsTransformer) Transform(fieldCaps model.FieldCapsResponse) (model.FieldCapsResponse, error) {
	return fieldCaps, nil
}

type NopQueryTransformer struct {
}

func (*NopQueryTransformer) Transform(query []*model.Query) ([]*model.Query, error) {
	return query, nil
}

type NopIngestTransformer struct {
}

func (*NopIngestTransformer) Transform(document types.JSON) (types.JSON, error) {
	return document, nil
}
