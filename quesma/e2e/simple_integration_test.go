//go:build integration

package e2e

import (
	"embed"
	"encoding/json"
	"fmt"
	"github.com/qri-io/jsonpointer"
	"github.com/stretchr/testify/assert"
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"
)

//go:embed assets
var simpleRequests embed.FS

const quesmaUrl = "localhost:8080"
const elasticUrl = "localhost:9201"

type HttpFile struct {
	Method  string
	URL     *url.URL
	Eq      []string
	Headers map[string]string
	Body    string
}

func NewHttpFile(data string) (*HttpFile, error) {
	res := &HttpFile{}

	res.Eq = make([]string, 0)
	lines := strings.Split(data, "\n")

	if len(lines) == 0 {
		return nil, fmt.Errorf("empty file")
	}

	res.Headers = make(map[string]string)

	readFirstLine := false
	var lastLine int
	for lineNo, header := range lines {
		lastLine = lineNo

		if strings.HasPrefix(header, "#") {
			if strings.HasPrefix(header, "#eq ") {
				res.Eq = append(res.Eq, strings.TrimPrefix(header, "#eq "))
			}
			continue
		}

		if header == "" {
			break
		}

		if !readFirstLine {

			firstLine := strings.Split(lines[lineNo], " ")

			if len(firstLine) != 2 {
				return nil, fmt.Errorf("invalid method, url line")
			}

			res.Method = firstLine[0]

			u, err := url.Parse(firstLine[1])
			if err != nil {
				return nil, err
			}
			res.URL = u

			readFirstLine = true
			continue
		}

		headerParts := strings.Split(header, ":")
		if len(headerParts) != 2 {
			return nil, fmt.Errorf("invalid header: %v", header)
		}

		res.Headers[headerParts[0]] = headerParts[1]

	}

	res.Body = strings.Join(lines[lastLine:], "\n")

	return res, nil
}

func (r *HttpFile) ToRequest() *http.Request {

	req := &http.Request{}
	req.Method = r.Method
	req.URL = r.URL
	req.Body = io.NopCloser(strings.NewReader(r.Body))

	req.Header = make(http.Header)
	for k, v := range r.Headers {
		req.Header.Add(k, v)
	}

	return req
}

func elasticClient(target string, httpFile *HttpFile) (string, error) {

	req := httpFile.ToRequest()

	req.Host = target
	req.URL.Host = target

	res, err := http.DefaultClient.Do(req)

	if err != nil {
		return "", err
	}

	defer res.Body.Close()

	response, err := io.ReadAll(res.Body)
	if err != nil {
		return "", err
	}

	if res.StatusCode != http.StatusOK {
		fmt.Println("response", string(response))
		return "", fmt.Errorf("Unexpected status code: %v, %v", res.StatusCode, res.Status)
	}

	return string(response), nil
}

func TestSimpleIntegration(t *testing.T) {

	const dir = "assets/simple_requests"

	files, err := simpleRequests.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
		return
	}

	for _, file := range files {

		if !strings.HasSuffix(file.Name(), ".http") {
			continue
		}

		t.Run(file.Name(), func(tt *testing.T) {

			content, err := simpleRequests.ReadFile(dir + "/" + file.Name())
			if err != nil {
				tt.Fatalf("reading a file failed %v: %v", file.Name(), err)
				return
			}

			httpFile, err := NewHttpFile(string(content))
			if err != nil {
				tt.Fatalf("parsing a file failed %v: %v", file.Name(), err)
				return
			}

			elastiResponse, err := elasticClient(elasticUrl, httpFile)
			if err != nil {
				tt.Fatal("calling Elastic failed: ", err)
				return
			}

			quesmaResponse, err := elasticClient(quesmaUrl, httpFile)
			if err != nil {
				tt.Fatal("calling Quesma failed: ", err)
				return
			}

			if strings.TrimSpace(elastiResponse) == "" {
				tt.Log("Elastic response is empty")
				return
			}

			elasticParsed := map[string]interface{}{}
			quesmaParsed := map[string]interface{}{}

			err = json.Unmarshal([]byte(elastiResponse), &elasticParsed)
			if err != nil {
				tt.Fatal("parsing Elastic response failed: ", err)
				return
			}

			err = json.Unmarshal([]byte(quesmaResponse), &quesmaParsed)
			if err != nil {
				tt.Fatal("parsing Quesma response failed: ", err)
				return
			}

			for _, pointer := range httpFile.Eq {

				tt.Run(pointer, func(ttt *testing.T) {
					ptr, err := jsonpointer.Parse(pointer)
					if err != nil {
						ttt.Fatal("parsing pointer failed: ", err)
						return
					}

					elasticValue, err := ptr.Eval(elasticParsed)
					if err != nil {
						ttt.Fatal("getting value from Elastic failed: ", err)
						return
					}

					quesmaValue, err := ptr.Eval(quesmaParsed)
					if err != nil {
						ttt.Fatal("getting value from Quesma failed: ", err)
						return
					}

					assert.Equal(ttt, elasticValue, quesmaValue)
				})
			}
		})
	}

}
