package end2end

import (
	"fmt"
	"github.com/k0kubun/pp"
	"mitmproxy/quesma/quesma/types"
	"mitmproxy/quesma/util"
	"testing"
)

const index = "kibana_sample_data_logs"

func TestNameBetter(t *testing.T) {
	cli := newHttpClient()
	elasticResponseString, _ := cli.sendSearchRequestToElastic(index, testRequests[0])
	elasticResp, err := types.ParseJSON(elasticResponseString)
	//pp.Println(elasticResp)

	for k, _ := range elasticResp {
		pp.Println(k)
	}
	pp.Println("elastic hits:", elasticResp["hits"])
	if err != nil {
		pp.Println(err)
	}

	quesmaResponseString, _ := cli.sendSearchRequestToQuesma(index, testRequests[0])
	quesmaResp, _ := types.ParseJSON(quesmaResponseString)
	for k, _ := range quesmaResp {
		pp.Println(k)
	}
	pp.Println("quesma hits", quesmaResp["hits"])

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
	if len(elasticMinusQuesma) != 0 {
		pp.Println("elasticMinusQuesma", elasticMinusQuesma)
		t.Error("elasticMinusQuesma", elasticMinusQuesma)
	}
	if len(quesmaMinusElastic) != 0 {
		pp.Println("quesmaMinusElastic", quesmaMinusElastic)
		t.Error("quesmaMinusElastic", quesmaMinusElastic)
	}
	fmt.Println("Test end!")
}
