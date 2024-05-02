package ui

import (
	"fmt"
	"github.com/k0kubun/pp"
	"mitmproxy/quesma/logger"
	"mitmproxy/quesma/model"
	"mitmproxy/quesma/tracing"
	"regexp"
	"sync"
)

const maxSavedQueriesPerQueryType = 10
const UnrecognizedQueryType = "unrecognized"

var unsupportedSearchQueryRegex, _ = regexp.Compile(logger.Reason + `":"` + logger.ReasonPrefixUnsupportedQueryType + `([[:word:]]+)"`)

type errorMessageWithRequestId struct {
	requestId    string
	errorMessage string
}

type UnsupportedSearchQueries struct {
	mutex sync.Mutex // it's a rare situation to not support some query, let's do everything here under this mutex for simplicity
	// RequestId -> request body
	// It contains either empty string (if we don't have the body yet. It means that when it arrives, we need to save it here), or the body itself.
	// When body arrives and key is not present here, we don't save the body.
	requestBodies             map[string]string
	errorMessagesPerQueryType map[string][]errorMessageWithRequestId // queryType -> error messages
	savedUnsupportedQueries   int                                    // how many we saved (max 10 per type)
	totalUnsupportedQueries   int                                    // we many we've seen total
	unsupportedTypesSeenCount int
}

func newUnsupportedSearchQueries() *UnsupportedSearchQueries {
	errorMessagesPerQueryType := make(map[string][]errorMessageWithRequestId, len(model.AggregationQueryTypes)+1)
	for _, queryType := range model.AggregationQueryTypes {
		errorMessagesPerQueryType[queryType] = make([]errorMessageWithRequestId, 0, maxSavedQueriesPerQueryType)
	}
	errorMessagesPerQueryType[UnrecognizedQueryType] = make([]errorMessageWithRequestId, 0, maxSavedQueriesPerQueryType)
	return &UnsupportedSearchQueries{
		requestBodies:             make(map[string]string),
		errorMessagesPerQueryType: errorMessagesPerQueryType,
	}
}

func (u *UnsupportedSearchQueries) processLogMessage(requestId string, log tracing.LogWithLevel) {
	match := unsupportedSearchQueryRegex.FindStringSubmatch(log.Msg)
	pp.Println("match:", match, log)
	if len(match) < 2 {
		// there's no unsupported_search_query in the log message
		return
	}
	searchQueryType := match[1]

	u.mutex.Lock()
	defer u.mutex.Unlock()
	if _, recognizedQueryType := u.errorMessagesPerQueryType[searchQueryType]; !recognizedQueryType {
		searchQueryType = UnrecognizedQueryType
	}
	u.totalUnsupportedQueries++
	thisTypeCountSoFar := len(u.errorMessagesPerQueryType[searchQueryType])
	if thisTypeCountSoFar == 0 {
		u.unsupportedTypesSeenCount++
	}
	if thisTypeCountSoFar < maxSavedQueriesPerQueryType {
		u.errorMessagesPerQueryType[searchQueryType] = append(
			u.errorMessagesPerQueryType[searchQueryType],
			errorMessageWithRequestId{requestId, log.Msg},
		)
		u.savedUnsupportedQueries++
		if _, exists := u.requestBodies[requestId]; !exists {
			u.requestBodies[requestId] = "" // marking that it's needed
		}
	}
	pp.Println(u.errorMessagesPerQueryType)
}

func (u *UnsupportedSearchQueries) saveRequestBodyIfNeeded(requestId string, requestBody string) {
	u.mutex.Lock()
	defer u.mutex.Unlock()
	fmt.Println("SAVING REQUEST BODY ", requestId)
	if _, needed := u.requestBodies[requestId]; needed {
		u.requestBodies[requestId] = requestBody
	}
}

// generateMainPageHtml generates the HTML for a table with all unsupported search queries ("/unsupported-requests").
func (u *UnsupportedSearchQueries) generateMainPageHtml() []byte {
	u.mutex.Lock()
	allMessages := u.errorMessagesPerQueryType
	allMessagesCount := u.totalUnsupportedQueries
	u.mutex.Unlock()

	buffer := newBufferWithHead()
	buffer.Html(`<div class="topnav">`)
	buffer.Html("\n<h3>Quesma Received unsupported queries, per query type</h3>")
	buffer.Html("\n</div>\n")

	buffer.Html(`<main id="queries">`)
	buffer.Html(`<main class="center" id="request-log-messages">`)
	buffer.Html("\n\n")
	buffer.Html(`<div class="unsupported-requests">`)

	if allMessagesCount == 0 {
		buffer.Html("<p>No unsupported queries received yet</p>")
	} else {
		for queryType, messagesOneType := range allMessages {
			if len(messagesOneType) == 0 {
				continue
			}
			var messages = make([]string, 0, len(messagesOneType))
			var links = make([]string, 0, len(messagesOneType))
			for _, message := range messagesOneType {
				messages = append(messages, message.errorMessage)
				links = append(links, `/request-body/`+message.requestId)
			}
			logMessages, _ := generateLogMessages(messages, links)

			buffer.Html("<center>" + queryType + "</center>")
			_, err := buffer.Write(logMessages)
			if err != nil {
				logger.Error().Err(err).Msg("Error writing unsupported queries log")
			}
		}
	}

	buffer.Html("\n</div>\n")
	buffer.Html("\n</main>\n")

	buffer.Html(`<div class="menu">`)
	buffer.Html("\n<h2>Menu</h2>")

	buffer.Html(`<form action="/">&nbsp;<input class="btn" type="submit" value="Back to live tail" /></form>`)

	buffer.Html("\n</div>")
	buffer.Html("\n</body>")
	buffer.Html("\n</html>")

	return buffer.Bytes()
}

// generateQueryRequestHtml generates the HTML for a simple page which displays a saved query request with the given ID.
// TODO maybe improve this html a bit + add this view to the UI, it's not there yet.
func (u *UnsupportedSearchQueries) generateQueryBodyHtml(requestId string) []byte {
	u.mutex.Lock()
	queryBody, bodyFound := u.requestBodies[requestId]
	u.mutex.Unlock()

	buffer := newBufferWithHead()
	buffer.Html(`<div class="topnav">`)
	if bodyFound {
		buffer.Html("\n<h3>Query body for request id ").Text(requestId)
		buffer.Html("</h3>")
	} else {
		buffer.Html("\n<h3>Query body not found for ").Text(requestId).Html("</h3>")
	}
	buffer.Html("\n</div>\n")

	buffer.Html(`<main id="request-body">`)
	buffer.Html("\n\n")
	buffer.Html(`<div class="request-body">`)
	buffer.Html(`<pre Id="query`).Text(requestId).Html(`">`)
	fmt.Println("QUERY BODY: ", queryBody)
	buffer.Text(queryBody)
	buffer.Html("\n</pre>")

	buffer.Html("\n</div>\n")
	buffer.Html("\n</main>\n")
	buffer.Html(`<div class="menu">`)
	buffer.Html("\n<h2>Menu</h2>")

	buffer.Html(`<form action="/">&nbsp;<input class="btn" type="submit" value="Back to live tail" /></form>`)
	buffer.Html(`<br>`)
	buffer.Html(`<form action="/unsupported-requests/`).Text(requestId).Html(`">&nbsp;<input class="btn" type="submit" value="Back to list" /></form>`)

	buffer.Html("\n</div>")
	buffer.Html("\n</body>")
	buffer.Html("\n</html>")
	return buffer.Bytes()
}

func (u *UnsupportedSearchQueries) generateSidePanelHtml() []byte {
	u.mutex.Lock()
	savedErrorsCount := u.savedUnsupportedQueries
	totalErrorsCount := u.totalUnsupportedQueries
	typesSeenCount := u.unsupportedTypesSeenCount
	u.mutex.Unlock()

	var buffer HtmlBuffer
	linkToMainView := `<li><a href="/unsupported-requests/"`
	buffer.Html(`<ul id="unsupported-queries-stats" hx-swap-oob="true">`)
	if totalErrorsCount > 0 {
		buffer.Html(fmt.Sprintf(`%s class="debug-error-log"">%d total (%d saved)</a></li>`, linkToMainView, totalErrorsCount, savedErrorsCount))
		plural := "s"
		if typesSeenCount == 1 {
			plural = ""
		}
		buffer.Html(fmt.Sprintf(`%s class="debug-error-log"">%d different type%s</a></li>`, linkToMainView, typesSeenCount, plural))
	} else {
		buffer.Html(linkToMainView + `>None!</a></li>`)
	}
	buffer.Html(`</ul>`)

	return buffer.Bytes()
}

func (u *UnsupportedSearchQueries) GetTotalUnsupportedQueries() int {
	u.mutex.Lock()
	defer u.mutex.Unlock()
	return u.totalUnsupportedQueries
}

func (u *UnsupportedSearchQueries) GetSavedUnsupportedQueries() int {
	u.mutex.Lock()
	defer u.mutex.Unlock()
	return u.savedUnsupportedQueries
}

func (u *UnsupportedSearchQueries) GetUnsupportedTypesSeenCount() int {
	u.mutex.Lock()
	defer u.mutex.Unlock()
	return u.unsupportedTypesSeenCount
}

func (u *UnsupportedSearchQueries) GetErrorMessages(queryType string) []errorMessageWithRequestId {
	u.mutex.Lock()
	defer u.mutex.Unlock()
	return u.errorMessagesPerQueryType[queryType]
}
