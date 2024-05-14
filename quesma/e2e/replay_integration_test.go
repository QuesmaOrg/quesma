//go:build integration

package e2e

import (
	"github.com/stretchr/testify/assert"
	"strings"
	"testing"
)

func TestReplay(t *testing.T) {

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

			quesmaResponse, err := elasticClient(quesmaUrl, httpFile)
			if err != nil {
				tt.Fatal("calling Quesma failed: ", err)
				return
			}

			expectedResponse, err := assets.ReadFile(dir + "/" + file.Name() + ".response")
			if err != nil {
				tt.Fatalf("reading a file failed %v: %v", file.Name(), err)
				return
			}

			// TODO
			assert.Equal(tt, expectedResponse, quesmaResponse)

		})
	}
}
