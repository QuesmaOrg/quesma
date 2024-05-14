//go:build integration

package e2e

import (
	"fmt"
	"github.com/qri-io/jsonpointer"
	"github.com/stretchr/testify/assert"
	"strings"
	"testing"
	"time"
)

func TestCompareResults(t *testing.T) {

	err := deleteIndex("windows_logs")
	if err != nil {
		t.Fatal(err)
		return
	}
	err = deleteIndex("device_logs")
	if err != nil {

	}

	bulkJson, err := assets.ReadFile("assets/compare_requests/bulk.json")
	if err != nil {
		t.Fatal(err)
		return
	}

	bulk := string(bulkJson)

	sendBulk(bulk)

	time.Sleep(10 * time.Second)

	const dir = "assets/compare_requests"

	files, err := assets.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
		return
	}

	for _, file := range files {

		if !strings.HasSuffix(file.Name(), ".http") {
			continue
		}

		t.Run(file.Name(), func(tt *testing.T) {

			content, err := assets.ReadFile(dir + "/" + file.Name())
			if err != nil {
				tt.Fatalf("reading a file failed %v: %v", file.Name(), err)
				return
			}

			httpFile, err := NewHttpFile(string(content))
			if err != nil {
				tt.Fatalf("parsing a file failed %v: %v", file.Name(), err)
				return
			}

			elasticResponse, err := elasticClient(elasticUrl, httpFile)
			if err != nil {
				tt.Fatal("calling Elastic failed: ", err)
				return
			}

			elasticParsed, err := parseElastic(elasticResponse)
			if err != nil {
				tt.Fatal("parsing Elastic response failed: ", err)
				return
			}

			quesmaResponse, err := elasticClient(quesmaUrl, httpFile)
			if err != nil {
				tt.Fatal("calling Quesma failed: ", err)
				return
			}

			quesmaParsed, err := parseElastic(quesmaResponse)
			if err != nil {
				tt.Fatal("parsing Quesma response failed: ", err)
				return
			}

			if strings.TrimSpace(elasticResponse) == "" {
				tt.Log("Elastic response is empty")
				return
			}

			for _, comparator := range httpFile.Comparator {

				tt.Run(comparator.A, func(ttt *testing.T) {
					ptrA, err := jsonpointer.Parse(comparator.A)
					if err != nil {
						ttt.Fatal("parsing pointer failed: ", err)
						return
					}

					elasticValue, err := ptrA.Eval(elasticParsed)
					if err != nil {
						fmt.Println("elasticResponse:", elasticResponse)
						ttt.Fatal("getting value from Elastic failed: ", err)
						return
					}

					ptrB, err := jsonpointer.Parse(comparator.B)
					if err != nil {
						ttt.Fatal("parsing pointer failed: ", err)
						return
					}

					quesmaValue, err := ptrB.Eval(quesmaParsed)
					if err != nil {
						fmt.Println("quesmaResponse:", quesmaResponse)
						ttt.Fatal("getting value from Quesma failed: ", err)
						return
					}

					assert.Equal(ttt, elasticValue, quesmaValue)
				})
			}
		})
	}

}
