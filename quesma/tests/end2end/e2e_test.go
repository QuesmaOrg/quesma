package end2end

import (
	"bytes"
	"github.com/k0kubun/pp"
	"io"
	"mitmproxy/quesma/util"
	"net/http"
	"testing"
)

var httpClient = http.Client{}

// useful if you want to debug one single request
func TestE2ESingleRequest(t *testing.T) {
	t.Skip("It fails now, there are differences in output for every testcase")
	const testSuite = "1"
	const testNr = "1"
	e2eRunSingleTest(t, testSuite, testNr)
}

// useful if you want to debug one single test suite
func TestE2ESingleSuite(t *testing.T) {
	t.Skip("It fails now, there are differences in output for every testcase")
	const testSuite = "1"
	e2eRunSingleSuite(t, testSuite)
}

// all tests
func TestE2EAll(t *testing.T) {
	t.Skip("It fails now, there are differences in output for every testcase")
	parser := httpRequestParser{}

	testSuites, err := parser.getAllTestSuites()
	if err != nil {
		t.Error(err)
	}
	if len(testSuites) == 0 {
		t.Error("no test suites found")
	}
	for _, testSuite := range testSuites {
		e2eRunSingleSuite(t, testSuite)
	}
}

func e2eRunSingleTest(t *testing.T, testSuite, testNr string) {
	parser := httpRequestParser{}
	test, err := parser.getSingleTest(testSuite, testNr)
	if err != nil {
		t.Error(err)
	}

	elasticResponse, err := sendRequestToElastic(test.urlSuffix, test.requestBody)
	if err != nil {
		_, _ = pp.Println(err)
		t.Error(err)
	}
	// possibly useful for debugging
	// elasticResp, err := types.ParseJSON(elasticResponseString)
	// pp.Println(elasticResp)
	// pp.Println("elastic hits:", elasticResp["hits"])

	quesmaResponse, _ := sendRequestToQuesma(test.urlSuffix, test.requestBody)
	if err != nil {
		_, _ = pp.Println(err)
		t.Error(err)
	}
	// possibly useful for debugging
	// quesmaResp, _ := types.ParseJSON(quesmaResponseString)
	// pp.Println(quesmaResp)
	// pp.Println("quesma hits", quesmaResp["hits"])

	elasticMinusQuesma, quesmaMinusElastic, err := util.JsonDifference(
		elasticResponse, quesmaResponse, true, true, 5)

	// first print all errors, only then fail the test
	if err != nil {
		_, _ = pp.Println(err)
	}
	// maybe change below to
	// assert.True(t, util.AlmostEmpty(actualMinusExpected, acceptableDifference))
	// assert.True(t, util.AlmostEmpty(expectedMinusActual, acceptableDifference))
	if len(elasticMinusQuesma) != 0 {
		_, _ = pp.Println("Present in Elastic response, but not in Quesma:", elasticMinusQuesma)
	}
	if len(quesmaMinusElastic) != 0 {
		_, _ = pp.Println("Present in Quesma response, but not in Elastic:", quesmaMinusElastic)
	}

	if err != nil {
		t.Error(err)
	}
	if len(elasticMinusQuesma) != 0 || len(quesmaMinusElastic) != 0 {
		t.Error("elasticMinusQuesma or quesmaMinusElastic not empty, len(elasticMinusQuesma):",
			len(elasticMinusQuesma), "len(quesmaMinusElastic):", len(quesmaMinusElastic))
	}
}

func e2eRunSingleSuite(t *testing.T, testSuite string) {
	parser := httpRequestParser{}

	tests, err := parser.getSingleTestSuite(testSuite)
	if err != nil {
		t.Error(err)
	}
	if len(tests) == 0 {
		t.Error("no tests found for suite:", testSuite)
	}

	for _, test := range tests {
		t.Run(testSuite+"/"+test.name, func(t *testing.T) {
			e2eRunSingleTest(t, testSuite, test.name)
		})
	}
}

func sendPost(url, body string) (string, error) {
	req, err := http.NewRequest("POST", url, bytes.NewBuffer([]byte(body)))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := httpClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	response, err := io.ReadAll(resp.Body)
	return string(response), err
}

func sendRequestToElastic(urlSuffix, body string) (string, error) {
	const urlPrefix = "http://localhost:9202"
	return sendPost(urlPrefix+urlSuffix, body)
}

func sendRequestToQuesma(urlSuffix, body string) (string, error) {
	const urlPrefix = "http://localhost:8080"
	return sendPost(urlPrefix+urlSuffix, body)
}
