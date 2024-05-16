package main

import (
	"database/sql"
	"fmt"
	"github.com/ClickHouse/clickhouse-go/v2"
	"mitmproxy/quesma/eql"
	"mitmproxy/quesma/eql/transform"
	"os"
	"strings"
)

// ------------------- solution

var logger DatabaseLet
var db *sql.DB

func currentQuesmaLike() {

	httpConnector := &RestServer{}

	panicBarrier := &PanicBarrier{}
	dispatcher := &Dispatcher{Sources: make(map[string]DatabaseLet), DispatchField: "path"}

	httpConnector.Source = panicBarrier
	panicBarrier.Source = dispatcher

	httpConnector.ListenAndServe(":6666")

	dispatcher.Sources["/logger"] = logger

	dispatcher.Sources["/sql"] = sqlPipeline()

	dispatcher.Sources["/windows_logs/_eql"] = eqlPipeline()
	dispatcher.Sources["/device_logs/_search"] = quesmaDeviceLogsPipeline()

	dispatcher.Sources["/panic"] = &Panic{}

}

func main() {

	sig := make(chan os.Signal, 1)

	options := clickhouse.Options{Addr: []string{"localhost:9000"}}
	db = clickhouse.OpenDB(&options)

	logger = makeLogger()

	Print("starting...")

	currentQuesmaLike()

	Print("waiting for signal...")

	<-sig
}

func sqlPipeline() DatabaseLet {

	restToSQL := &QueryTransformer{Transformer: TransformerFunc(func(doc Document) Document {
		body := doc["body"].(Document)
		doc["query"] = body["query"]
		return doc
	})}

	documentsToHits := &DocumentReducer{Reducer: ReducerFunc(func(docs []Document) Document {
		return Document{"hits": len(docs), "docs": docs}
	})}

	redactFields := &DocumentsTransformer{Transformer: TransformerFunc(func(doc Document) Document {
		delete(doc, "process::executable")
		doc["create_table_query"] = "XXX REDACTED XXX"
		return doc
	})}

	sqlDatabase := &SQLDatabase{db: db}

	restToSQL.Source = documentsToHits
	documentsToHits.Source = redactFields
	redactFields.Source = sqlDatabase

	return restToSQL
}

func quesmaDeviceLogsPipeline() DatabaseLet {

	toHttpRequest := &QueryTransformer{Transformer: TransformerFunc(func(doc Document) Document {

		doc["url"] = "http://localhost:8080/device_logs/_search"

		return doc
	})}

	restClient := &RestClient{}

	toHttpRequest.Source = restClient

	return toHttpRequest
}

func eqlPipeline() DatabaseLet {

	restToSQL := &QueryTransformer{Transformer: TransformerFunc(func(doc Document) Document {
		body := doc["body"].(Document)
		doc["query"] = body["query"]
		return doc
	})}

	eqlToSql := &QueryTransformer{Transformer: TransformerFunc(func(doc Document) Document {

		eqlQuery := doc["query"].(string)

		translateName := func(name *transform.Symbol) (*transform.Symbol, error) {
			res := strings.ReplaceAll(name.Name, ".", "::")
			res = "\"" + res + "\"" // TODO proper escaping
			return transform.NewSymbol(res), nil
		}

		trans := eql.NewTransformer()
		trans.FieldNameTranslator = translateName
		trans.ExtractParameters = false
		where, _, err := trans.TransformQuery(eqlQuery)

		if err != nil {
			fmt.Println("tranform errors:")
			fmt.Println(err)
		}

		fmt.Printf("where clause: '%s'\n", where)

		sqlQuery := `select "@timestamp", "event::category", "process::name", "process::pid", "process::executable" from windows_logs where ` + where

		doc["query"] = sqlQuery
		return doc
	})}

	documentsToHits := &DocumentReducer{Reducer: ReducerFunc(func(docs []Document) Document {
		return Document{"hits": len(docs), "docs": docs}
	})}

	sqlDatabase := &SQLDatabase{db: db}

	restToSQL.Source = eqlToSql
	eqlToSql.Source = documentsToHits
	documentsToHits.Source = sqlDatabase

	return restToSQL
}
