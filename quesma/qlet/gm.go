package main

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/ClickHouse/clickhouse-go/v2"
	"io/ioutil"
	"log"
	"mitmproxy/quesma/eql"
	"mitmproxy/quesma/eql/transform"
	"net/http"
	"os"
	"strings"
	"time"
)

// Every process is a database.

type DatabaseLet interface {
	Query(query Document) ([]Document, error)
}

// Every data is a document

type Document map[string]interface{}

//

func NewDocument() Document {
	return make(Document)
}

func (d Document) String() string {
	out, err := json.Marshal(d)
	if err != nil {
		return fmt.Sprintf("error: %v", err)
	}
	return string(out)
}

// --------------------------

// ---------------------
//

// Aux  implementation

type DocumentLetFunc func(Document) ([]Document, error)

func (f DocumentLetFunc) Query(query Document) ([]Document, error) {
	return f(query)
}

// ---

var emptyDocuments = make([]Document, 0)

type EmptyDatabaseLet struct{}

func (e *EmptyDatabaseLet) Query(query Document) ([]Document, error) {
	return emptyDocuments, nil
}

type ErrorDatabaseLet struct {
	Err error
}

func (e *ErrorDatabaseLet) Query(query Document) ([]Document, error) {
	return nil, e.Err
}

// -----

type StaticDocuments struct {
	Documents []Document
}

func (s *StaticDocuments) Query(query Document) ([]Document, error) {
	return s.Documents, nil
}

//

// Document transformer

type Transformer interface {
	Transform(document Document) Document
}

type TransformerFunc func(Document) Document

func (f TransformerFunc) Transform(document Document) Document {
	return f(document)
}

// Transforms query document before passing it to the source

type QueryTransformer struct {
	Transformer Transformer
	Source      DatabaseLet
}

func (i *QueryTransformer) Query(query Document) ([]Document, error) {
	query = i.Transformer.Transform(query)
	return i.Source.Query(query)
}

// Transforms documents after they are returned from the source

type DocumentsTransformer struct {
	Transformer Transformer
	Source      DatabaseLet
}

func (t *DocumentsTransformer) Query(query Document) ([]Document, error) {
	query = t.Transformer.Transform(query)
	docs, err := t.Source.Query(query)
	if err != nil {
		return nil, err
	}

	for i, _ := range docs {
		docs[i] = t.Transformer.Transform(docs[i])
	}

	return docs, nil
}

// ------------------- []Document -> Document

type Reducer interface {
	Reduce([]Document) Document
}

type ReducerFunc func([]Document) Document

func (r ReducerFunc) Reduce(docs []Document) Document {
	return r(docs)
}

type DocumentReducer struct {
	Reducer Reducer
	Source  DatabaseLet
}

func (r *DocumentReducer) Query(query Document) ([]Document, error) {
	docs, err := r.Source.Query(query)
	if err != nil {
		return nil, err
	}
	return []Document{r.Reducer.Reduce(docs)}, nil
}

// Exploder Document -> []Document

type Exploder interface {
	Explode(Document) []Document
}

type ExploderFunc func(Document) []Document

func (e ExploderFunc) Explode(doc Document) []Document {
	return e(doc)
}

type DocumentExploder struct {
	Exploder Exploder
	Source   DatabaseLet
}

func (e *DocumentExploder) Query(query Document) ([]Document, error) {
	docs, err := e.Source.Query(query)
	if err != nil {
		return nil, err
	}

	var out []Document

	for _, doc := range docs {
		out = append(out, e.Exploder.Explode(doc)...)
	}

	return out, nil
}

/// ---------

// actual implementations

type StdOutIngest struct{}

func (c *StdOutIngest) Query(document Document) ([]Document, error) {
	fmt.Println(document.String())
	return emptyDocuments, nil
}

// ---

func makeLogger() DatabaseLet {

	console := &StdOutIngest{}

	trans := &QueryTransformer{Transformer: TransformerFunc(func(in Document) Document {
		in["@timestamp"] = time.Now().Format(time.RFC3339)
		return in

	})}

	trans.Source = console

	return trans
}

//

func Print(m string, a ...any) {

	if logger == nil {
		return
	}

	doc := make(Document)

	doc["message"] = fmt.Sprintf(m, a...)

	_, err := logger.Query(doc)
	if err != nil {
		log.Println(err)
	}
}

// ----------

/// -------------------------------

//

type PanicBarrier struct {
	Source DatabaseLet
}

func (p *PanicBarrier) Query(query Document) ([]Document, error) {
	defer func() {
		if r := recover(); r != nil {
			fmt.Println("panic:", r)
		}
	}()

	return p.Source.Query(query)
}

type Panic struct {
}

func (p *Panic) Query(query Document) ([]Document, error) {
	panic("panic")
	return nil, nil
}

type Tracer struct {
	Source DatabaseLet
}

func (t *Tracer) Query(query Document) ([]Document, error) {

	trace := make(Document)

	trace["query"] = query

	docs, err := t.Source.Query(query)
	trace["docs"] = docs
	trace["error"] = err

	Print("TRACE %s", trace.String())
	if err != nil {
		return nil, err
	}

	return docs, err
}

// --

type SQLDatabase struct {
	db *sql.DB
}

func (d *SQLDatabase) Query(query Document) ([]Document, error) {

	sqlQuery := query["query"].(string)

	rows, err := d.db.Query(sqlQuery)
	if err != nil {
		return nil, err
	}

	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	var docs []Document

	for rows.Next() {
		doc := make(Document)

		row := make([]any, len(cols))
		for i := range row {
			row[i] = new(interface{})
		}
		err = rows.Scan(row...)
		if err != nil {
			return nil, err
		}

		for i, col := range cols {
			doc[col] = *row[i].(*interface{})
		}

		docs = append(docs, doc)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return docs, nil
}

//

type RestClient struct {
}

func (h *RestClient) Query(query Document) ([]Document, error) {

	url := query["url"].(string)

	body := query["body"]

	b, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}

	resp, err := http.Post(url, "application/json", bytes.NewReader(b))
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()

	b, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var doc Document

	err = json.Unmarshal(b, &doc)
	if err != nil {
		return nil, err
	}

	return []Document{doc}, nil
}

//

type RestServer struct {
	mux    *http.ServeMux
	Source DatabaseLet
}

func (h *RestServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {

	defer r.Body.Close()
	b, err := ioutil.ReadAll(r.Body)

	internalError := func(err error) {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(err.Error()))
	}

	if err != nil {
		internalError(err)
		return
	}

	query := make(Document)

	query["method"] = r.Method
	query["path"] = r.URL.Path

	body := make(Document)
	err = json.Unmarshal(b, &body)

	if err != nil {
		internalError(err)
		return
	}
	query["body"] = body

	if h.Source == nil {
		internalError(fmt.Errorf("no source"))
		return
	}

	docs, err := h.Source.Query(query)
	if err != nil {
		internalError(err)
		return
	}

	for _, doc := range docs {

		out, err := json.MarshalIndent(doc, "", " ")
		if err != nil {
			internalError(err)
			return
		}

		w.Write([]byte(out))
	}
}

func (h *RestServer) ListenAndServe(addr string) error {

	h.mux = http.NewServeMux()
	h.mux.Handle("/", h)
	go http.ListenAndServe(addr, h.mux)
	return nil
}

//

type Dispatcher struct {
	Sources       map[string]DatabaseLet
	DispatchField string
}

func (d *Dispatcher) Query(query Document) ([]Document, error) {

	field, ok := query[d.DispatchField]
	if !ok {
		return nil, fmt.Errorf("missing dispatch field: %s", d.DispatchField)
	}

	source, ok := d.Sources[field.(string)]
	if !ok {
		return nil, fmt.Errorf("no source for field: %s", field)
	}

	return source.Query(query)

}

type If struct {
	condition func() bool
	True      DatabaseLet
	False     DatabaseLet
}

func (i *If) Query(query Document) ([]Document, error) {
	if i.condition() {
		return i.True.Query(query)
	}
	return i.False.Query(query)
}

//

var logger DatabaseLet
var db *sql.DB

func main() {

	sig := make(chan os.Signal, 1)

	options := clickhouse.Options{Addr: []string{"localhost:9000"}}
	db = clickhouse.OpenDB(&options)

	logger = makeLogger()

	Print("starting...")

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
