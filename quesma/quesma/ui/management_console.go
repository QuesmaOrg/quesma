package ui

import (
	"bytes"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/k0kubun/pp"
	"github.com/mjibson/sqlfmt"
	"io"
	"log"
	"mitmproxy/quesma/quesma/config"
	"mitmproxy/quesma/stats"
	"mitmproxy/quesma/util"
	"net/http"
	"strings"
	"sync"

	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/sem/tree"
	"github.com/gorilla/mux"
)

const (
	uiTcpPort       = "9999"
	maxLastMessages = 10000
)

const (
	managementInternalPath = "/_quesma"
	healthPath             = managementInternalPath + "/health"
	bypassPath             = managementInternalPath + "/bypass"
)

//go:embed asset/*
var uiFs embed.FS

type QueryDebugPrimarySource struct {
	Id        string
	QueryResp []byte
}

type QueryDebugSecondarySource struct {
	Id string

	IncomingQueryBody []byte

	QueryBodyTranslated    []byte
	QueryRawResults        []byte
	QueryTranslatedResults []byte
}

type QueryDebugInfo struct {
	QueryDebugPrimarySource
	QueryDebugSecondarySource
}

type QuesmaManagementConsole struct {
	queryDebugPrimarySource   chan *QueryDebugPrimarySource
	queryDebugSecondarySource chan *QueryDebugSecondarySource
	ui                        *http.Server
	mutex                     sync.Mutex
	debugInfoMessages         map[string]QueryDebugInfo
	debugLastMessages         []string
	responseMatcherChannel    chan QueryDebugInfo
	config                    config.QuesmaConfiguration
}

func NewQuesmaManagementConsole(config config.QuesmaConfiguration) *QuesmaManagementConsole {
	return &QuesmaManagementConsole{
		queryDebugPrimarySource:   make(chan *QueryDebugPrimarySource, 5),
		queryDebugSecondarySource: make(chan *QueryDebugSecondarySource, 5),
		debugInfoMessages:         make(map[string]QueryDebugInfo),
		debugLastMessages:         make([]string, 0),
		responseMatcherChannel:    make(chan QueryDebugInfo, 5),
		config:                    config,
	}
}

func (qmc *QuesmaManagementConsole) PushPrimaryInfo(qdebugInfo *QueryDebugPrimarySource) {
	qmc.queryDebugPrimarySource <- qdebugInfo
}

func (qmc *QuesmaManagementConsole) PushSecondaryInfo(qdebugInfo *QueryDebugSecondarySource) {
	qmc.queryDebugSecondarySource <- qdebugInfo
}

func copyMap(originalMap map[string]QueryDebugInfo) map[string]QueryDebugInfo {
	copiedMap := make(map[string]QueryDebugInfo)

	for key, value := range originalMap {
		copiedMap[key] = value
	}

	return copiedMap
}

func (qdi *QueryDebugInfo) requestContains(queryStr string) bool {
	potentialPlaces := [][]byte{qdi.QueryDebugSecondarySource.IncomingQueryBody,
		qdi.QueryDebugSecondarySource.QueryBodyTranslated}
	for _, potentialPlace := range potentialPlaces {
		if potentialPlace != nil && strings.Contains(string(potentialPlace), queryStr) {
			return true
		}
	}
	return false
}

func (qmc *QuesmaManagementConsole) newHTTPServer() *http.Server {
	return &http.Server{
		Addr:    ":" + uiTcpPort,
		Handler: qmc.createRouting(),
	}
}

func (qmc *QuesmaManagementConsole) createRouting() *mux.Router {
	router := mux.NewRouter()

	router.HandleFunc(healthPath, ok)

	router.HandleFunc(bypassPath, bypassSwitch).Methods("POST")

	router.HandleFunc("/", func(writer http.ResponseWriter, req *http.Request) {
		buf := qmc.generateLiveTail()
		_, _ = writer.Write(buf)
	})

	router.HandleFunc("/statistics-json", func(writer http.ResponseWriter, req *http.Request) {
		jsonBody, err := json.Marshal(stats.GlobalStatistics)
		if err != nil {
			log.Println("Error marshalling statistics:", err)
			writer.WriteHeader(500)
			return
		}
		_, _ = writer.Write(jsonBody)
		writer.WriteHeader(200)
	})

	router.HandleFunc("/statistics", func(writer http.ResponseWriter, req *http.Request) {
		buf := qmc.generateStatistics()
		_, _ = writer.Write(buf)
	})

	router.HandleFunc("/ingest-statistics", func(writer http.ResponseWriter, req *http.Request) {
		buf := qmc.generateStatisticsLiveTail()
		_, _ = writer.Write(buf)
	})

	router.HandleFunc("/queries", func(writer http.ResponseWriter, req *http.Request) {
		buf := qmc.generateQueries()
		_, _ = writer.Write(buf)
	})
	router.PathPrefix("/request-Id/{requestId}").HandlerFunc(func(writer http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		buf := qmc.generateReportForRequestId(vars["requestId"])
		_, _ = writer.Write(buf)
	})
	router.PathPrefix("/requests-by-str/{queryString}").HandlerFunc(func(writer http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		buf := qmc.generateReportForRequests(vars["queryString"])
		_, _ = writer.Write(buf)
	})
	router.PathPrefix("/request-Id").HandlerFunc(func(writer http.ResponseWriter, r *http.Request) {
		// redirect to /
		http.Redirect(writer, r, "/", http.StatusSeeOther)
	})
	router.PathPrefix("/requests-by-str").HandlerFunc(func(writer http.ResponseWriter, r *http.Request) {
		// redirect to /
		http.Redirect(writer, r, "/", http.StatusSeeOther)
	})
	router.HandleFunc("/queries", func(writer http.ResponseWriter, req *http.Request) {
		buf := qmc.generateQueries()
		_, _ = writer.Write(buf)
	})
	router.PathPrefix("/static/").Handler(http.StripPrefix("/static/", http.FileServer(http.FS(uiFs))))
	return router
}

func (qmc *QuesmaManagementConsole) listenAndServe() {
	if err := qmc.ui.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatal("Error starting server:", err)
	}
}

type DebugKeyValue struct {
	Key   string
	Value QueryDebugInfo
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
	stmts := []string{strings.ReplaceAll(string(sqlData), "`", `"`)} // sqlfmt can't deal with backticks
	sqlFormatted, err := sqlfmt.FmtSQL(formattingConfig, stmts)
	if err != nil {
		log.Printf("Error while formatting sql: %s\n", err)
		sqlFormatted = string(sqlData)
	}
	return sqlFormatted
}

func generateQueries(debugKeyValueSlice []DebugKeyValue, withLinks bool) []byte {
	var buffer bytes.Buffer

	buffer.WriteString("\n" + `<div class="left" Id="left">` + "\n")
	buffer.WriteString(`<div class="title-bar">Query`)
	buffer.WriteString("\n</div>\n")
	buffer.WriteString(`<div class="debug-body">`)
	for _, v := range debugKeyValueSlice {
		if withLinks {
			buffer.WriteString(`<a href="/request-Id/` + v.Key + `">`)
		}
		buffer.WriteString("<p>RequestID:" + v.Key + "</p>\n")
		buffer.WriteString(`<pre Id="query` + v.Key + `">`)
		buffer.WriteString(util.JsonPrettify(string(v.Value.IncomingQueryBody), true))
		buffer.WriteString("\n</pre>")
		if withLinks {
			buffer.WriteString("\n</a>")
		}
	}
	buffer.WriteString("\n</div>")
	buffer.WriteString("\n</div>\n")

	buffer.WriteString(`<div class="right" Id="right">` + "\n")
	buffer.WriteString(`<div class="title-bar">Elasticsearch response` + "\n" + `</div>`)
	buffer.WriteString(`<div class="debug-body">`)
	for _, v := range debugKeyValueSlice {
		if withLinks {
			buffer.WriteString(`<a href="/request-Id/` + v.Key + `">`)
		}
		buffer.WriteString("<p>ResponseID:" + v.Key + "</p>\n")
		buffer.WriteString(`<pre Id="response` + v.Key + `">`)
		buffer.WriteString(util.JsonPrettify(string(v.Value.QueryResp), true))
		buffer.WriteString("\n</pre>")
		if withLinks {
			buffer.WriteString("\n</a>")
		}
	}
	buffer.WriteString("\n</div>")
	buffer.WriteString("\n</div>\n")

	buffer.WriteString(`<div class="bottom_left" Id="bottom_left">` + "\n")
	buffer.WriteString(`<div class="title-bar">Clickhouse translated query` + "\n" + `</div>`)
	buffer.WriteString(`<div class="debug-body">`)
	for _, v := range debugKeyValueSlice {
		if withLinks {
			buffer.WriteString(`<a href="/request-Id/` + v.Key + `">`)
		}
		buffer.WriteString("<p>RequestID:" + v.Key + "</p>\n")
		buffer.WriteString(`<pre Id="second_query` + v.Key + `">`)
		buffer.WriteString(sqlPrettyPrint(v.Value.QueryBodyTranslated))
		buffer.WriteString("\n</pre>")
		if withLinks {
			buffer.WriteString("\n</a>")
		}
	}
	buffer.WriteString("\n</div>")
	buffer.WriteString("\n</div>\n")

	buffer.WriteString(`<div class="bottom_right" Id="bottom_right">` + "\n")
	buffer.WriteString(`<div class="title-bar">Clickhouse response` + "\n" + `</div>`)
	buffer.WriteString(`<div class="debug-body">`)
	for _, v := range debugKeyValueSlice {
		if withLinks {
			buffer.WriteString(`<a href="/request-Id/` + v.Key + `">`)
		}
		buffer.WriteString("<p>ResponseID:" + v.Key + "</p>\n")
		buffer.WriteString(`<pre Id="second_response` + v.Key + `">`)
		buffer.WriteString(util.JsonPrettify(string(v.Value.QueryTranslatedResults), true))
		buffer.WriteString("\n\nThere are more results ...")
		buffer.WriteString("\n</pre>")
		if withLinks {
			buffer.WriteString("\n</a>")
		}
	}
	buffer.WriteString("\n</div>")
	buffer.WriteString("\n</div>\n")

	return buffer.Bytes()
}

func (qmc *QuesmaManagementConsole) generateQueries() []byte {
	// Take last MAX_LAST_MESSAGES to display, e.g. 100 out of potentially 10m000
	qmc.mutex.Lock()
	lastMessages := qmc.debugLastMessages
	debugKeyValueSlice := []DebugKeyValue{}
	count := 0
	for i := len(lastMessages) - 1; i >= 0 && count < maxLastMessages; i-- {
		debugInfoMessage := qmc.debugInfoMessages[lastMessages[i]]
		if len(debugInfoMessage.QueryDebugSecondarySource.IncomingQueryBody) > 0 {
			debugKeyValueSlice = append(debugKeyValueSlice, DebugKeyValue{lastMessages[i], debugInfoMessage})
			count++
		}
	}
	qmc.mutex.Unlock()

	return generateQueries(debugKeyValueSlice, true)
}

func newBufferWithHead() bytes.Buffer {
	const bufferSize = 4 * 1024 // size of ui/head.html
	var buffer bytes.Buffer
	buffer.Grow(bufferSize)
	head, err := uiFs.ReadFile("asset/head.html")
	buffer.Write(head)
	if err != nil {
		buffer.WriteString(err.Error())
	}
	buffer.WriteString("\n")
	return buffer
}

func (qmc *QuesmaManagementConsole) generateStatistics() []byte {
	var buffer bytes.Buffer
	const maxTopValues = 5

	statistics := stats.GlobalStatistics

	for _, index := range statistics.SortedIndexNames() {
		buffer.WriteString("\n" + fmt.Sprintf(`<h2>Stats for "%s" <small>from %d requests</small></h2>`, index.IndexName, index.Requests) + "\n")

		buffer.WriteString("<table>\n")

		buffer.WriteString("<thead>\n")
		buffer.WriteString("<tr>\n")
		buffer.WriteString(`<th class="key">Key</th>` + "\n")
		buffer.WriteString(`<th class="key-count">Count</th>` + "\n")
		buffer.WriteString(`<th class="value">Value</th>` + "\n")
		buffer.WriteString(`<th class="value-count">Count</th>` + "\n")
		buffer.WriteString(`<th class="types">Potential type</th>` + "\n")
		buffer.WriteString("</tr>\n")
		buffer.WriteString("</thead>\n")
		buffer.WriteString("<tbody>\n")

		for _, keyStats := range index.SortedKeyStatistics() {
			topValuesCount := maxTopValues
			if len(keyStats.Values) < maxTopValues {
				topValuesCount = len(keyStats.Values)
			}

			buffer.WriteString("<tr>\n")
			buffer.WriteString(fmt.Sprintf(`<td class="key" rowspan="%d">%s</td>`+"\n", topValuesCount, keyStats.KeyName))
			buffer.WriteString(fmt.Sprintf(`<td class="key-count" rowspan="%d">%d</td>`+"\n", topValuesCount, keyStats.Occurrences))

			for i, value := range keyStats.TopNValues(topValuesCount) {
				if i > 0 {
					buffer.WriteString("</tr>\n<tr>\n")
				}

				buffer.WriteString(fmt.Sprintf(`<td class="value">%s</td>`, value.ValueName))
				buffer.WriteString(fmt.Sprintf(`<td class="value-count">%d</td>`, value.Occurrences))
				buffer.WriteString(fmt.Sprintf(`<td class="types">%s</td>`, strings.Join(value.Types, ", ")))
			}
			buffer.WriteString("</tr>\n")
		}

		buffer.WriteString("</tbody>\n")

		buffer.WriteString("</table>\n")
	}

	return buffer.Bytes()
}

func (qmc *QuesmaManagementConsole) generateStatisticsLiveTail() []byte {
	buffer := newBufferWithHead()

	buffer.WriteString(`<div class="topnav">`)
	buffer.WriteString("\n<h3>Quesma Management Console - ingest statistics</h3>")
	buffer.WriteString(`<div class="autorefresh-box">` + "\n")
	buffer.WriteString(`<div class="autorefresh">`)
	buffer.WriteString(`<input type="checkbox" Id="autorefresh" name="autorefresh" hx-target="#statistics" hx-get="/statistics" hx-trigger="every 1s [htmx.find('#autorefresh').checked]" checked />`)
	buffer.WriteString(`<label for="autorefresh">Autorefresh every 1s</label>`)
	buffer.WriteString("\n</div>")
	buffer.WriteString("\n</div>\n")
	buffer.WriteString("\n</div>\n\n")

	buffer.WriteString(`<div Id="statistics">`)
	buffer.Write(qmc.generateStatistics())
	buffer.WriteString("\n</div>\n\n")

	buffer.WriteString(`<div class="menu">`)
	buffer.WriteString("\n<h2>Menu</h2>")

	buffer.WriteString(`<form action="/">&nbsp;<input class="btn" type="submit" value="Back to live tail" /></form>`)

	buffer.WriteString("\n</div>")

	buffer.WriteString("\n</body>")
	buffer.WriteString("\n</html>")
	return buffer.Bytes()
}

func (qmc *QuesmaManagementConsole) generateLiveTail() []byte {
	buffer := newBufferWithHead()

	buffer.WriteString(`<div class="topnav">`)
	buffer.WriteString("\n<h3>Quesma Management Console</h3>")

	buffer.WriteString(`<div class="autorefresh-box">` + "\n")
	buffer.WriteString(`<div class="autorefresh">`)
	buffer.WriteString(`<input type="checkbox" Id="autorefresh" name="autorefresh" hx-target="#queries" hx-get="/queries" hx-trigger="every 1s [htmx.find('#autorefresh').checked]" checked />`)
	buffer.WriteString(`<label for="autorefresh">Autorefresh every 1s</label>`)
	buffer.WriteString("\n</div>")
	buffer.WriteString("\n</div>\n")
	buffer.WriteString("\n</div>\n")

	buffer.WriteString(`<div Id="queries">`)
	buffer.Write(qmc.generateQueries())
	buffer.WriteString("\n</div>\n\n")

	buffer.WriteString(`<div class="menu">`)
	buffer.WriteString("\n<h2>Menu</h2>")
	buffer.WriteString("\n<h3>Find query</h3><br>\n")

	buffer.WriteString(`<form onsubmit="location.href = '/request-Id/' + find_query_by_id_input.value; return false;">`)
	buffer.WriteString("\n")
	buffer.WriteString(`&nbsp;<input Id="find_query_by_id_button" type="submit" class="btn" value="By Id" /><br>`)
	buffer.WriteString(`&nbsp;<input type="text" Id="find_query_by_id_input" class="input" name="find_query_by_id_input" value="" required size="32"><br><br>`)
	buffer.WriteString("</form>")

	buffer.WriteString(`<form onsubmit="location.href = '/requests-by-str/' + find_query_by_str_input.value; return false;">`)
	buffer.WriteString(`&nbsp;<input Id="find_query_by_str_button" type="submit" class="btn" value="By keyword in request" /><br>`)
	buffer.WriteString(`&nbsp;<input type="text" Id="find_query_by_str_input" class="input" name="find_query_by_str_input" value="" required size="32"><br><br>`)
	buffer.WriteString("</form>")

	buffer.WriteString(`<h3>Useful links</h3>`)
	buffer.WriteString(`<ul>`)
	buffer.WriteString(`<li><a href="http://localhost:5601/app/observability-log-explorer/">Kibana Log Explorer</a></li>`)
	buffer.WriteString(`<li><a href="http://localhost:8081">mitmproxy</a></li>`)
	buffer.WriteString(`<li><a href="http://localhost:8123/play">Clickhouse</a></li>`)
	buffer.WriteString(`<li><a href="/ingest-statistics">Ingest statistics</a></li>`)
	buffer.WriteString(`</ul>`)

	buffer.WriteString(`<h3>Details</h3>`)
	buffer.WriteString(`<ul>`)
	buffer.WriteString("<li><small>Mode: " + qmc.config.Mode.String() + "</small></li>")

	buffer.WriteString("\n</div>")
	buffer.WriteString("\n</body>")
	buffer.WriteString("\n</html>")
	return buffer.Bytes()
}

func (qmc *QuesmaManagementConsole) generateReportForRequestId(requestId string) []byte {
	qmc.mutex.Lock()
	request, requestFound := qmc.debugInfoMessages[requestId]
	qmc.mutex.Unlock()

	buffer := newBufferWithHead()
	buffer.WriteString(`<div class="topnav">`)
	if requestFound {
		buffer.WriteString("\n<h3>Quesma Report for request Id " + requestId + "</h3>")
	} else {
		buffer.WriteString("\n<h3>Quesma Report not found for " + requestId + "</h3>")
	}

	buffer.WriteString("\n</div>\n")
	buffer.WriteString(`<div Id="queries">`)

	debugKeyValueSlice := []DebugKeyValue{}
	if requestFound {
		debugKeyValueSlice = append(debugKeyValueSlice, DebugKeyValue{requestId, request})
	}

	buffer.Write(generateQueries(debugKeyValueSlice, false))

	buffer.WriteString("\n</div>\n")
	buffer.WriteString(`<div class="menu">`)
	buffer.WriteString("\n<h2>Menu</h2>")

	buffer.WriteString(`<form action="/">&nbsp;<input class="btn" type="submit" value="Back to live tail" /></form>`)

	buffer.WriteString("\n</div>")
	buffer.WriteString("\n</body>")
	buffer.WriteString("\n</html>")
	return buffer.Bytes()
}

func (qmc *QuesmaManagementConsole) generateReportForRequests(requestStr string) []byte {
	qmc.mutex.Lock()
	localQueryDebugInfo := copyMap(qmc.debugInfoMessages)
	lastMessages := qmc.debugLastMessages
	qmc.mutex.Unlock()

	var debugKeyValueSlice []DebugKeyValue
	for i := len(lastMessages) - 1; i >= 0; i-- {
		debugInfo := localQueryDebugInfo[lastMessages[i]]
		if debugInfo.requestContains(requestStr) {
			debugKeyValueSlice = append(debugKeyValueSlice, DebugKeyValue{lastMessages[i], localQueryDebugInfo[lastMessages[i]]})
		}
	}

	buffer := newBufferWithHead()
	buffer.WriteString(`<div class="topnav">`)
	title := fmt.Sprintf("Quesma Report for str '%s' with %d results", requestStr, len(debugKeyValueSlice))
	buffer.WriteString("\n<h3>" + title + "</h3>")

	buffer.WriteString("\n</div>\n\n")

	buffer.WriteString(`<div Id="queries">`)

	buffer.Write(generateQueries(debugKeyValueSlice, true))

	buffer.WriteString("\n</div>\n\n")

	buffer.WriteString(`<div class="menu">`)
	buffer.WriteString("\n<h2>Menu</h2>")

	buffer.WriteString(`<form action="/">&nbsp;<input class="btn" type="submit" value="Back to live tail" /></form>`)

	buffer.WriteString("\n</div>")
	buffer.WriteString("\n</body>")
	buffer.WriteString("\n</html>")

	return buffer.Bytes()
}

func (qmc *QuesmaManagementConsole) addNewMessageId(messageId string) {
	qmc.debugLastMessages = append(qmc.debugLastMessages, messageId)
	if len(qmc.debugLastMessages) > maxLastMessages {
		delete(qmc.debugInfoMessages, qmc.debugLastMessages[0])
		qmc.debugLastMessages = qmc.debugLastMessages[1:]
	}
}

func (qmc *QuesmaManagementConsole) Run() {
	go qmc.comparePipelines()
	go func() {
		qmc.ui = qmc.newHTTPServer()
		qmc.listenAndServe()
	}()
	for {
		select {
		case msg := <-qmc.queryDebugPrimarySource:
			log.Println("Received debug info from primary source:", msg.Id)
			debugPrimaryInfo := QueryDebugPrimarySource{msg.Id, msg.QueryResp}
			qmc.mutex.Lock()
			if value, ok := qmc.debugInfoMessages[msg.Id]; !ok {
				qmc.debugInfoMessages[msg.Id] = QueryDebugInfo{
					QueryDebugPrimarySource: debugPrimaryInfo,
				}
				qmc.addNewMessageId(msg.Id)
			} else {
				value.QueryDebugPrimarySource = debugPrimaryInfo
				qmc.debugInfoMessages[msg.Id] = value
				// That's the point where QueryDebugInfo is
				// complete and we can compare results
				qmc.responseMatcherChannel <- value
			}
			qmc.mutex.Unlock()
		case msg := <-qmc.queryDebugSecondarySource:
			log.Println("Received debug info from secondary source:", msg.Id)
			secondaryDebugInfo := QueryDebugSecondarySource{
				msg.Id,
				msg.IncomingQueryBody,
				msg.QueryBodyTranslated,
				msg.QueryRawResults,
				msg.QueryTranslatedResults,
			}
			qmc.mutex.Lock()
			if value, ok := qmc.debugInfoMessages[msg.Id]; !ok {
				qmc.debugInfoMessages[msg.Id] = QueryDebugInfo{
					QueryDebugSecondarySource: secondaryDebugInfo,
				}
				qmc.addNewMessageId(msg.Id)
			} else {
				value.QueryDebugSecondarySource = secondaryDebugInfo
				// That's the point where QueryDebugInfo is
				// complete and we can compare results
				qmc.debugInfoMessages[msg.Id] = value
				qmc.responseMatcherChannel <- value
			}
			qmc.mutex.Unlock()

		}
	}
}

func ok(writer http.ResponseWriter, _ *http.Request) {
	writer.WriteHeader(200)
	writer.Header().Set("Content-Type", "application/json")
	_, _ = writer.Write([]byte(`{"cluster_name": "quesma"}`))
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
		config.SetTrafficAnalysis(val)
		fmt.Printf("global bypass set to %t\n", val)
		writer.WriteHeader(200)
	} else {
		writer.WriteHeader(400)
	}
}

func (qmc *QuesmaManagementConsole) comparePipelines() {
	for {
		queryDebugInfo, ok := <-qmc.responseMatcherChannel
		if ok {
			if string(queryDebugInfo.QueryResp) != string(queryDebugInfo.QueryTranslatedResults) {
				log.Println("Responses are different:")
				elasticSurplusFields, ourSurplusFields, err := util.JsonDifference(string(queryDebugInfo.QueryResp), string(queryDebugInfo.QueryTranslatedResults))
				if err != nil {
					log.Println("Error while comparing responses:", err)
					continue
				}
				pp.Println(`Clickhouse response \ Elastic response: `, ourSurplusFields)
				pp.Println(`Elastic response \ Clickhouse response: `, elasticSurplusFields)
			}
		}
	}
}
