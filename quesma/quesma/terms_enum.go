package quesma

import (
	"context"
	"encoding/json"
	"fmt"
	"mitmproxy/quesma/clickhouse"
	"mitmproxy/quesma/logger"
	"mitmproxy/quesma/model"
	"mitmproxy/quesma/queryparser"
)

func handleTermsEnum(ctx context.Context, index string, reqBody []byte, lm *clickhouse.LogManager) ([]byte, error) {
	if resolvedTableName := lm.ResolveTableName(index); resolvedTableName == "" {
		errorMsg := fmt.Sprintf("terms enum failed - could not resolve table name for index: %s", index)
		logger.Error().Msg(errorMsg)
		return nil, fmt.Errorf(errorMsg)
	} else {
		return handleTermsEnumRequest(ctx, reqBody, &queryparser.ClickhouseQueryTranslator{ClickhouseLM: lm, Table: lm.GetTable(resolvedTableName)})
	}
}

func handleTermsEnumRequest(_ context.Context, reqBody []byte, qt *queryparser.ClickhouseQueryTranslator) ([]byte, error) {
	var jsonReq map[string]interface{}
	if err := json.Unmarshal(reqBody, &jsonReq); err != nil {
		logger.Error().Msgf("error unmarshalling terms enum API request: %s", err)
		return json.Marshal(emptyTermsEnumResponse())
	}
	fieldName := jsonReq["field"].(string)
	simpleQ := qt.ParseQueryMap(jsonReq)
	selectQuery := qt.BuildSelectQuery([]string{fieldName}, simpleQ.Sql.Stmt)
	if rows, err := qt.ClickhouseLM.ProcessAutocompleteSuggestionsQuery(selectQuery); err != nil {
		logger.Error().Msgf("terms enum failed - error processing SQL query [%s]", err)
		return json.Marshal(emptyTermsEnumResponse())
	} else {
		return json.Marshal(makeTermsEnumResponse(rows))
	}
}

func makeTermsEnumResponse(rows []model.QueryResultRow) *model.TermsEnumResponse {
	terms := make([]string, 0)
	for _, row := range rows {
		terms = append(terms, row.Cols[0].Value.(string))
	}
	return &model.TermsEnumResponse{
		Complete: true,
		Terms:    terms,
	}
}

func emptyTermsEnumResponse() *model.TermsEnumResponse {
	return &model.TermsEnumResponse{
		Complete: false,
		Terms:    nil,
	}
}
