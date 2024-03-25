package termsenum

import (
	"context"
	"encoding/json"
	"fmt"
	"mitmproxy/quesma/clickhouse"
	"mitmproxy/quesma/logger"
	"mitmproxy/quesma/model"
	"mitmproxy/quesma/queryparser"
	"mitmproxy/quesma/quesma/ui"
	"mitmproxy/quesma/tracing"
	"time"
)

func HandleTermsEnum(ctx context.Context, index string, reqBody []byte, lm *clickhouse.LogManager,
	qmc *ui.QuesmaManagementConsole) ([]byte, error) {
	if resolvedTableName := lm.ResolveTableName(index); resolvedTableName == "" {
		errorMsg := fmt.Sprintf("terms enum failed - could not resolve table name for index: %s", index)
		logger.Error().Msg(errorMsg)
		return nil, fmt.Errorf(errorMsg)
	} else {
		return handleTermsEnumRequest(ctx, reqBody, &queryparser.ClickhouseQueryTranslator{ClickhouseLM: lm, Table: lm.GetTable(resolvedTableName), Ctx: context.Background()}, qmc)
	}
}

func handleTermsEnumRequest(ctx context.Context, reqBody []byte, qt *queryparser.ClickhouseQueryTranslator, qmc *ui.QuesmaManagementConsole) (result []byte, err error) {
	request := NewRequest()
	startTime := time.Now()
	if err := request.UnmarshalJSON(reqBody); err != nil {
		logger.Error().Msgf("error unmarshalling terms enum API request: %s", err)
		return json.Marshal(emptyTermsEnumResponse())
	}

	where := qt.ParseAutocomplete(request.IndexFilter, request.Field, request.String, request.CaseInsensitive)
	selectQuery := qt.BuildAutocompleteQuery(request.Field, where.Sql.Stmt, request.Size)
	dbQueryCtx, cancel := context.WithCancel(ctx)
	// TODO this will be used to cancel goroutine that is executing the query
	_ = cancel
	if rows, err2 := qt.ClickhouseLM.ProcessAutocompleteSuggestionsQuery(dbQueryCtx, selectQuery); err2 != nil {
		logger.Error().Msgf("terms enum failed - error processing SQL query [%s]", err2)
		result, err = json.Marshal(emptyTermsEnumResponse())
	} else {
		result, err = json.Marshal(makeTermsEnumResponse(rows))
	}

	qmc.PushSecondaryInfo(&ui.QueryDebugSecondarySource{
		Id:                     ctx.Value(tracing.RequestIdCtxKey).(string),
		IncomingQueryBody:      reqBody,
		QueryBodyTranslated:    []byte(selectQuery.String()),
		QueryRawResults:        nil,
		QueryTranslatedResults: result,
		SecondaryTook:          time.Since(startTime),
	})
	return
}

func makeTermsEnumResponse(rows []model.QueryResultRow) *model.TermsEnumResponse {
	terms := make([]string, 0)
	for _, row := range rows {
		value := row.Cols[0].Value
		if value != nil {
			if tmp, ok := value.(*string); ok {
				terms = append(terms, *tmp)
			} else {
				terms = append(terms, value.(string)) // needed only for tests
			}
		}
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
