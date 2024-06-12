package end2end

import (
	"fmt"
	"github.com/k0kubun/pp"
	"mitmproxy/quesma/util"
	"testing"
)

const index = "kibana_sample_data_logs"

type singleE2ETest struct {
	name        string
	requestBody string
	urlSuffix   string // without "http://name:port", so /index-pattern/...
}

var httpClient = newHttpClient()

// useful if you want to debug one single request
func TestE2ESingleRequest(t *testing.T) {
	const testSuite = "1"
	const testNr = "87"
	t.Skip("It fails now, there are differences in output for every testcase")
	e2eRunSingleTest(t, testSuite, testNr)
}

// useful if you want to debug one single test suite
func TestE2ESingleSuite(t *testing.T) {
	const testSuite = "1"
	t.Skip("It fails now, there are differences in output for every testcase")
	e2eRunSingleSuite(t, testSuite)
}

// all tests
func TestE2EAll(t *testing.T) {
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
	fmt.Println(test.urlSuffix, test.requestBody)
	elasticResponseString, err := httpClient.sendRequestToElastic(test.urlSuffix, test.requestBody)
	if err != nil {
		pp.Println(err)
	}
	// elasticResp, err := types.ParseJSON(elasticResponseString)
	// pp.Println(elasticResp)

	// pp.Println("elastic hits:", elasticResp["hits"])

	quesmaResponseString, _ := httpClient.sendRequestToQuesma(test.urlSuffix, test.requestBody)
	// quesmaResp, _ := types.ParseJSON(quesmaResponseString)
	// pp.Println("quesma hits", quesmaResp["hits"])

	elasticMinusQuesma, quesmaMinusElastic, err := util.JsonDifference(
		elasticResponseString, quesmaResponseString, true, true, 5)

	if err != nil {
		pp.Println(err)
	}
	// maybe change below to
	// assert.True(t, util.AlmostEmpty(actualMinusExpected, acceptableDifference))
	// assert.True(t, util.AlmostEmpty(expectedMinusActual, acceptableDifference))
	if len(elasticMinusQuesma) != 0 {
		pp.Println("elasticMinusQuesma", elasticMinusQuesma)
	}
	if len(quesmaMinusElastic) != 0 {
		pp.Println("quesmaMinusElastic", quesmaMinusElastic)
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
		t.Error("no tests found")
	}

	t.Skip("It fails now, there are differences in output for every testcase")
	for _, test := range tests {
		t.Run(testSuite+"/"+test.name, func(t *testing.T) {
			fmt.Println("running test", test.name)
			e2eRunSingleTest(t, testSuite, test.name)
		})
	}
}
