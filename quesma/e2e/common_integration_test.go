//go:build integration

package e2e

import (
	"bufio"
	"bytes"
	"embed"
	"encoding/json"
	"fmt"
	"github.com/ClickHouse/clickhouse-go/v2"
	"io"
	"log"
	"net/http"
	"net/url"
	"strings"
)

//go:embed assets
var assets embed.FS

const quesmaUrl = "localhost:8080"
const elasticUrl = "localhost:9201"

func sendBulkTo(targetUrl string, bulk string) {

	if resp, err := http.Post("http://"+targetUrl+"/_bulk", "application/json", bytes.NewBuffer([]byte(bulk))); err != nil {
		log.Printf("Failed to send bulk: %v", err)
	} else {
		fmt.Printf("Sent bulk to %s response=%s\n", targetUrl, resp.Status)
		if err := resp.Body.Close(); err != nil {
			log.Fatal(err)
		}
	}
}

func sendBulk(bulk string) {
	sendBulkTo(quesmaUrl, bulk)
	sendBulkTo(elasticUrl, bulk)
}

func elasticDelete(index string) error {
	req, err := http.NewRequest("DELETE", "http://"+elasticUrl+"/"+index, nil)
	if err != nil {
		return err
	}

	res, err := http.DefaultClient.Do(req)

	if err != nil {
		return err
	}

	defer res.Body.Close()
	return nil
}

func clickhouseDelete(table string) error {

	options := clickhouse.Options{Addr: []string{"localhost:9000"}}

	db := clickhouse.OpenDB(&options)

	err := db.Ping()
	if err != nil {
		return err
	}

	_, err = db.Exec("TRUNCATE TABLE " + table)

	if err != nil {
		return err
	}

	return nil
}

func deleteIndex(index string) error {
	err := elasticDelete(index)
	if err != nil {
		return fmt.Errorf("Failed to delete index %v: %v", index, err)

	}

	err = clickhouseDelete(index)
	if err != nil {
		return fmt.Errorf("Failed to delete table %v: %v", index, err)
	}
	return nil
}

type Comparator struct {
	A string
	B string
}

type HttpFile struct {
	Method     string
	URL        *url.URL
	Comparator []Comparator
	Headers    map[string]string
	Body       string
}

func NewHttpFile(data string) (*HttpFile, error) {
	res := &HttpFile{}

	res.Comparator = make([]Comparator, 0)
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

				parts := strings.Split(header, " ")

				var comparator Comparator
				switch len(parts) {
				case 2:
					comparator.A = parts[1]
					comparator.B = parts[1]
				case 3:
					comparator.A = parts[1]
					comparator.B = parts[2]

				default:
					return nil, fmt.Errorf("invalid eq line: %v", header)
				}

				res.Comparator = append(res.Comparator, comparator)
			}
			continue
		}

		if header == "" {
			break
		}

		if !readFirstLine {

			firstLine := strings.Split(lines[lineNo], " ")

			if len(firstLine) < 2 {
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

func parseElastic(body string) (map[string]interface{}, error) {

	parsed := map[string]interface{}{}
	err := json.Unmarshal([]byte(body), &parsed)
	if err != nil {
		return nil, fmt.Errorf("parsing response failed: %v", err)
	}

	return parsed, nil
}

func parseHttpResponse(req *http.Request, body string) (*http.Response, error) {

	resp, err := http.ReadResponse(bufio.NewReader(strings.NewReader(body)), req)

	if err != nil {
		return nil, fmt.Errorf("parsing response failed: %v", err)
	}

	return resp, nil
}
