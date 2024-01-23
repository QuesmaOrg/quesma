package quesma

import (
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"sync"

	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/sem/tree"
	"github.com/gorilla/mux"
	"github.com/mjibson/sqlfmt"
)

const (
	UI_TCP_PORT           = "9999"
	DISPLAY_LAST_MESSAGES = 100
	MAX_LAST_MESSAGES     = 10000
)

const (
	managementInternalPath = "/_quesma"
	healthPath             = managementInternalPath + "/health"
	bypassPath             = managementInternalPath + "/bypass"
)

//go:embed ui/*
var uiFs embed.FS

type QueryDebugPrimarySource struct {
	id        string
	queryResp []byte
}

type QueryDebugSecondarySource struct {
	id string

	incomingQueryBody []byte

	queryBodyTranslated    []byte
	queryRawResults        []byte
	queryTranslatedResults []byte
}

type QueryDebugInfo struct {
	QueryDebugPrimarySource
	QueryDebugSecondarySource
}

type QueryDebugger struct {
	queryDebugPrimarySource   chan *QueryDebugPrimarySource
	queryDebugSecondarySource chan *QueryDebugSecondarySource
	ui                        *http.Server
	mutex                     sync.Mutex
	debugInfoMessages         map[string]QueryDebugInfo
	debugLastMessages         []string
}

func NewQueryDebugger() *QueryDebugger {
	return &QueryDebugger{
		queryDebugPrimarySource:   make(chan *QueryDebugPrimarySource, 5),
		queryDebugSecondarySource: make(chan *QueryDebugSecondarySource, 5),
		debugInfoMessages:         make(map[string]QueryDebugInfo),
		debugLastMessages:         make([]string, 0),
	}
}

func (qd *QueryDebugger) PushPrimaryInfo(qdebugInfo *QueryDebugPrimarySource) {
	qd.queryDebugPrimarySource <- qdebugInfo
}

func (qd *QueryDebugger) PushSecondaryInfo(qdebugInfo *QueryDebugSecondarySource) {
	qd.queryDebugSecondarySource <- qdebugInfo
}

func copyMap(originalMap map[string]QueryDebugInfo) map[string]QueryDebugInfo {
	copiedMap := make(map[string]QueryDebugInfo)

	for key, value := range originalMap {
		copiedMap[key] = value
	}

	return copiedMap
}

func (qdi *QueryDebugInfo) contains(queryStr string) bool {
	potentialPlaces := [][]byte{qdi.QueryDebugPrimarySource.queryResp, qdi.QueryDebugSecondarySource.incomingQueryBody,
		qdi.QueryDebugSecondarySource.queryBodyTranslated, qdi.QueryDebugSecondarySource.queryTranslatedResults}
	for _, potentialPlace := range potentialPlaces {
		if potentialPlace != nil && strings.Contains(string(potentialPlace), queryStr) {
			return true
		}
	}
	return false
}

func (qd *QueryDebugger) newHTTPServer() *http.Server {
	return &http.Server{
		Addr:    ":" + UI_TCP_PORT,
		Handler: qd.createRouting(),
	}
}

func (qd *QueryDebugger) createRouting() *mux.Router {
	router := mux.NewRouter()

	router.HandleFunc(healthPath, ok)

	router.HandleFunc(bypassPath, bypassSwitch).Methods("POST")

	router.HandleFunc("/", func(writer http.ResponseWriter, req *http.Request) {
		buf := qd.generateLiveTail()
		_, _ = writer.Write(buf)
	})
	router.HandleFunc("/queries", func(writer http.ResponseWriter, req *http.Request) {
		buf := qd.generateQueries()
		_, _ = writer.Write(buf)
	})
	router.PathPrefix("/request-id/{requestId}").HandlerFunc(func(writer http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		buf := qd.generateReportForRequestId(vars["requestId"])
		_, _ = writer.Write(buf)
	})
	router.PathPrefix("/requests-by-str/{queryString}").HandlerFunc(func(writer http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		buf := qd.generateReportForRequests(vars["queryString"])
		_, _ = writer.Write(buf)
	})
	router.PathPrefix("/request-id").HandlerFunc(func(writer http.ResponseWriter, r *http.Request) {
		// redirect to /
		http.Redirect(writer, r, "/", http.StatusSeeOther)
	})
	router.PathPrefix("/requests-by-str").HandlerFunc(func(writer http.ResponseWriter, r *http.Request) {
		// redirect to /
		http.Redirect(writer, r, "/", http.StatusSeeOther)
	})
	router.HandleFunc("/queries", func(writer http.ResponseWriter, req *http.Request) {
		buf := qd.generateQueries()
		_, _ = writer.Write(buf)
	})
	router.PathPrefix("/static/").Handler(http.StripPrefix("/static/", http.FileServer(http.FS(uiFs))))
	return router
}

func (qd *QueryDebugger) listenAndServe() {
	if err := qd.ui.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatal("Error starting server:", err)
	}
}

type DebugKeyValue struct {
	Key   string
	Value QueryDebugInfo
}

func prettyPrintJson(jsonData []byte) []byte {
	// Unmarshal the JSON string into a map
	var data map[string]interface{}
	err := json.Unmarshal(jsonData, &data)
	if err != nil {
		log.Println("Warning while prettyPrintJson:", err)
		return jsonData
	}

	// Marshal the data with an indent of two spaces
	prettyJsonData, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		log.Println("Warning while prettyPrintJson:", err)
		return jsonData
	}
	return prettyJsonData
}

func sqlPrettyPrint(sqlData []byte) string {
	formattingConfig := tree.PrettyCfg{
		LineWidth:                120,
		DoNotNewLineAfterColName: true,
		Simplify:                 true,
		TabWidth:                 2,
		UseTabs:                  false,
		Align:                    tree.PrettyNoAlign,
	}
	stmts := []string{string(sqlData)}
	sqlFormatted, err := sqlfmt.FmtSQL(formattingConfig, stmts)
	if err != nil {
		log.Printf("Error while formatting sql: %s\n", err)
		sqlFormatted = string(sqlData)
	}
	return sqlFormatted
}

func generateQueries(debugKeyValueSlice []DebugKeyValue, withLinks bool) []byte {
	var buf []byte
	buf = make([]byte, 0)

	buf = append(buf, []byte("\n<div class=\"left\" id=\"left\">")...)
	buf = append(buf, []byte("\n<div class=\"title-bar\">Query")...)
	buf = append(buf, []byte("\n</div>\n")...)
	buf = append(buf, []byte(`<div class="debug-body">`)...)
	for _, v := range debugKeyValueSlice {
		if withLinks {
			buf = append(buf, []byte(`<a href="/request-id/`+v.Key+`">`)...)
		}
		buf = append(buf, []byte("<p>RequestID:"+v.Key+"</p>")...)
		buf = append(buf, []byte("\n<pre id=\"query"+v.Key+"\">")...)
		buf = append(buf, []byte(prettyPrintJson(v.Value.incomingQueryBody))...)
		buf = append(buf, []byte("\n</pre>")...)
		if withLinks {
			buf = append(buf, []byte("\n</a>")...)
		}
	}
	buf = append(buf, []byte("\n</div>")...)
	buf = append(buf, []byte("\n</div>")...)

	buf = append(buf, []byte("\n<div class=\"right\" id=\"right\">")...)
	buf = append(buf, []byte("\n<div class=\"title-bar\">Elasticsearch response")...)
	buf = append(buf, []byte("\n</div>")...)
	buf = append(buf, []byte(`<div class="debug-body">`)...)
	for _, v := range debugKeyValueSlice {
		if withLinks {
			buf = append(buf, []byte(`<a href="/request-id/`+v.Key+`">`)...)
		}
		buf = append(buf, []byte("<p>ResponseID:"+v.Key+"</p>")...)
		buf = append(buf, []byte("\n<pre id=\"response"+v.Key+"\">")...)
		buf = append(buf, []byte(v.Value.queryResp)...)
		buf = append(buf, []byte("\n</pre>")...)
		if withLinks {
			buf = append(buf, []byte("\n</a>")...)
		}
	}
	buf = append(buf, []byte("\n</div>")...)
	buf = append(buf, []byte("\n</div>")...)

	buf = append(buf, []byte("\n<div class=\"bottom_left\" id=\"bottom_left\">")...)
	buf = append(buf, []byte("\n<div class=\"title-bar\">Clickhouse translated query")...)
	buf = append(buf, []byte("\n</div>")...)
	buf = append(buf, []byte(`<div class="debug-body">`)...)
	for _, v := range debugKeyValueSlice {
		if withLinks {
			buf = append(buf, []byte(`<a href="/request-id/`+v.Key+`">`)...)
		}
		buf = append(buf, []byte("<p>RequestID:"+v.Key+"</p>")...)
		buf = append(buf, []byte("\n<pre id=\"second_query"+v.Key+"\">")...)
		buf = append(buf, []byte(sqlPrettyPrint(v.Value.queryBodyTranslated))...)
		buf = append(buf, []byte("\n</pre>")...)
		if withLinks {
			buf = append(buf, []byte("\n</a>")...)
		}
	}
	buf = append(buf, []byte("\n</div>")...)
	buf = append(buf, []byte("\n</div>")...)

	buf = append(buf, []byte("\n<div class=\"bottom_right\" id=\"bottom_right\">")...)
	buf = append(buf, []byte("\n<div class=\"title-bar\">Clickhouse response")...)
	buf = append(buf, []byte("\n</div>")...)
	buf = append(buf, []byte(`<div class="debug-body">`)...)
	for _, v := range debugKeyValueSlice {
		if withLinks {
			buf = append(buf, []byte(`<a href="/request-id/`+v.Key+`">`)...)
		}
		buf = append(buf, []byte("<p>ResponseID:"+v.Key+"</p>")...)
		buf = append(buf, []byte("\n<pre id=\"second_response"+v.Key+"\">")...)
		buf = append(buf, []byte(v.Value.queryTranslatedResults)...)
		buf = append(buf, []byte("\n</pre>")...)
		if withLinks {
			buf = append(buf, []byte("\n</a>")...)
		}
	}
	buf = append(buf, []byte("\n</div>")...)
	buf = append(buf, []byte("\n</div>")...)

	return buf
}

func (qd *QueryDebugger) generateQueries() []byte {
	// Take last MAX_LAST_MESSAGES to display, e.g. 100 out of potentially 10m000
	qd.mutex.Lock()
	lastMessages := qd.debugLastMessages
	debugKeyValueSlice := []DebugKeyValue{}
	count := 0
	for i := len(lastMessages) - 1; i >= 0 && count < MAX_LAST_MESSAGES; i-- {
		debugKeyValueSlice = append(debugKeyValueSlice, DebugKeyValue{lastMessages[i], qd.debugInfoMessages[lastMessages[i]]})
		count++
	}
	qd.mutex.Unlock()

	return generateQueries(debugKeyValueSlice, true)
}

func (qd *QueryDebugger) generateLiveTail() []byte {
	var buf []byte

	head, err := uiFs.ReadFile("ui/head.html")
	buf = append(buf, head...)
	if err != nil {
		buf = append(buf, []byte(err.Error())...)
	}
	buf = append(buf, []byte("\n<div class=\"topnav\">")...)
	buf = append(buf, []byte("\n<h3>Quesma Live Debugging Interface</h3>")...)

	buf = append(buf, []byte(`<div class="autorefresh-box">`)...)
	buf = append(buf, []byte(`<input type="checkbox" id="autorefresh" name="autorefresh" hx-target="#queries" hx-get="/queries" hx-trigger="every 1s [htmx.find('#autorefresh').checked]" checked />`)...)
	buf = append(buf, []byte(`<label for="autorefresh">Autorefresh every 1s</label>`)...)
	buf = append(buf, []byte("\n</div>")...)

	buf = append(buf, []byte("\n</div>")...)

	buf = append(buf, []byte("\n<div id=\"queries\">")...)
	buf = append(buf, qd.generateQueries()...)
	buf = append(buf, []byte("\n</div>")...)

	buf = append(buf, []byte("\n<div class=\"menu\">")...)
	buf = append(buf, []byte("\n<h2>Menu</h2>")...)

	buf = append(buf, []byte(`<form onsubmit="location.href = '/request-id/' + find_query_by_id_input.value; return false;">`)...)
	buf = append(buf, []byte(`&nbsp;<input id="find_query_by_id_button" type="submit" class="btn" value="Find query by id" /><br>`)...)
	buf = append(buf, []byte("&nbsp;<input type=\"text\" id=\"find_query_by_id_input\" class=\"input\" name=\"find_query_by_id_input\" value=\"\" required size=\"40\"><br><br>")...)
	buf = append(buf, []byte(`</form>`)...)

	buf = append(buf, []byte(`<form onsubmit="location.href = '/requests-by-str/' + find_query_by_str_input.value; return false;">`)...)
	buf = append(buf, []byte(`&nbsp;<input id="find_query_by_str_button" type="submit" class="btn" value="Find query by keyword" /><br>`)...)
	buf = append(buf, []byte("&nbsp;<input type=\"text\" id=\"find_query_by_str_input\" class=\"input\" name=\"find_query_by_str_input\" value=\"\" required size=\"40\"><br><br>")...)
	buf = append(buf, []byte(`</form>`)...)

	buf = append(buf, []byte(`<h3>Useful links</h3>`)...)
	buf = append(buf, []byte(`<ul>`)...)
	buf = append(buf, []byte(`<li><a href="http://localhost:5601">Kibana</a></li>`)...)
	buf = append(buf, []byte(`<li><a href="http://localhost:8081">mitmproxy</a></li>`)...)

	buf = append(buf, []byte(`<li><a href="http://localhost:8123/play">Clickhouse</a></li>`)...)

	buf = append(buf, []byte(`</ul>`)...)

	buf = append(buf, []byte("\n</div>")...)
	buf = append(buf, []byte("\n</body>")...)
	buf = append(buf, []byte("\n</html>")...)
	return buf
}

func (qd *QueryDebugger) generateReportForRequestId(requestId string) []byte {
	qd.mutex.Lock()
	request, requestFound := qd.debugInfoMessages[requestId]
	qd.mutex.Unlock()

	var buf []byte

	head, err := uiFs.ReadFile("ui/head.html")
	buf = append(buf, head...)
	if err != nil {
		buf = append(buf, []byte(err.Error())...)
	}
	buf = append(buf, []byte("\n<div class=\"topnav\">")...)
	if requestFound {
		buf = append(buf, []byte("\n<h3>Quesma Report for request id "+requestId+"</h3>")...)
	} else {
		buf = append(buf, []byte("\n<h3>Quesma Report not found for "+requestId+"</h3>")...)
	}

	buf = append(buf, []byte("\n</div>")...)

	buf = append(buf, []byte("\n<div id=\"queries\">")...)

	debugKeyValueSlice := []DebugKeyValue{}
	if requestFound {
		debugKeyValueSlice = append(debugKeyValueSlice, DebugKeyValue{requestId, request})
	}

	buf = append(buf, generateQueries(debugKeyValueSlice, false)...)

	buf = append(buf, []byte("\n</div>")...)

	buf = append(buf, []byte("\n<div class=\"menu\">")...)
	buf = append(buf, []byte("\n<h2>Menu</h2>")...)

	buf = append(buf, []byte(`<form action="/">&nbsp;<input class="btn" type="submit" value="Back to live tail" /></form>`)...)

	buf = append(buf, []byte("\n</div>")...)
	buf = append(buf, []byte("\n</body>")...)
	buf = append(buf, []byte("\n</html>")...)
	return buf
}

func (qd *QueryDebugger) generateReportForRequests(requestStr string) []byte {
	qd.mutex.Lock()
	localQueryDebugInfo := copyMap(qd.debugInfoMessages)
	lastMessages := qd.debugLastMessages
	qd.mutex.Unlock()

	var debugKeyValueSlice []DebugKeyValue
	for i := len(lastMessages) - 1; i >= 0; i-- {
		debugInfo := localQueryDebugInfo[lastMessages[i]]
		if debugInfo.contains(requestStr) {
			debugKeyValueSlice = append(debugKeyValueSlice, DebugKeyValue{lastMessages[i], localQueryDebugInfo[lastMessages[i]]})
		}
	}

	var buf []byte

	head, err := uiFs.ReadFile("ui/head.html")
	buf = append(buf, head...)
	if err != nil {
		buf = append(buf, []byte(err.Error())...)
	}
	buf = append(buf, []byte("\n<div class=\"topnav\">")...)
	title := fmt.Sprintf("Quesma Report for str '%s' with %d results", requestStr, len(debugKeyValueSlice))
	buf = append(buf, []byte("\n<h3>"+title+"</h3>")...)

	buf = append(buf, []byte("\n</div>")...)

	buf = append(buf, []byte("\n<div id=\"queries\">")...)

	buf = append(buf, generateQueries(debugKeyValueSlice, true)...)

	buf = append(buf, []byte("\n</div>")...)

	buf = append(buf, []byte("\n<div class=\"menu\">")...)
	buf = append(buf, []byte("\n<h2>Menu</h2>")...)

	buf = append(buf, []byte(`<form action="/">&nbsp;<input class="btn" type="submit" value="Back to live tail" /></form>`)...)

	buf = append(buf, []byte("\n</div>")...)
	buf = append(buf, []byte("\n</body>")...)
	buf = append(buf, []byte("\n</html>")...)
	return buf
}

func (gd *QueryDebugger) addNewMessageId(messageId string) {
	gd.debugLastMessages = append(gd.debugLastMessages, messageId)
	if len(gd.debugLastMessages) > MAX_LAST_MESSAGES {
		delete(gd.debugInfoMessages, gd.debugLastMessages[0])
		gd.debugLastMessages = gd.debugLastMessages[1:]
	}
}

func (qd *QueryDebugger) Run() {
	go func() {
		qd.ui = qd.newHTTPServer()
		qd.listenAndServe()
	}()
	for {
		select {
		case msg := <-qd.queryDebugPrimarySource:
			log.Println("Received debug info from primary source:", msg.id)
			debugPrimaryInfo := QueryDebugPrimarySource{msg.id, msg.queryResp}
			qd.mutex.Lock()
			if value, ok := qd.debugInfoMessages[msg.id]; !ok {
				qd.debugInfoMessages[msg.id] = QueryDebugInfo{
					QueryDebugPrimarySource: debugPrimaryInfo,
				}
				qd.addNewMessageId(msg.id)
			} else {
				value.QueryDebugPrimarySource = debugPrimaryInfo
				qd.debugInfoMessages[msg.id] = value
			}
			qd.mutex.Unlock()
		case msg := <-qd.queryDebugSecondarySource:
			log.Println("Received debug info from secondary source:", msg.id)
			secondaryDebugInfo := QueryDebugSecondarySource{
				msg.id,
				msg.incomingQueryBody,
				msg.queryBodyTranslated,
				msg.queryRawResults,
				msg.queryTranslatedResults,
			}
			qd.mutex.Lock()
			if value, ok := qd.debugInfoMessages[msg.id]; !ok {
				qd.debugInfoMessages[msg.id] = QueryDebugInfo{
					QueryDebugSecondarySource: secondaryDebugInfo,
				}
				qd.addNewMessageId(msg.id)
			} else {
				value.QueryDebugSecondarySource = secondaryDebugInfo
				qd.debugInfoMessages[msg.id] = value
			}
			qd.mutex.Unlock()

		}
	}
}

func ok(writer http.ResponseWriter, _ *http.Request) {
	writer.WriteHeader(200)
}

// curl -X POST localhost:9999/_quesma/bypass -d '{"bypass": true}'
func bypassSwitch(writer http.ResponseWriter, r *http.Request) {
	bodyString, err := io.ReadAll(r.Body)
	if err != nil {
		log.Println("Error reading body:", err)
		writer.WriteHeader(400)
		_, _ = writer.Write([]byte("Error reading body: " + err.Error()))
		return
	}
	body := make(map[string]interface{})
	err = json.Unmarshal(bodyString, &body)
	if err != nil {
		log.Fatal(err)
	}

	if body["bypass"] != nil {
		val := body["bypass"].(bool)
		globalBypass.Store(val)
		fmt.Printf("global bypass set to %t\n", val)
		writer.WriteHeader(200)
	} else {
		writer.WriteHeader(400)
	}
}
