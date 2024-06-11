package end2end

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
)

type HttpClient struct {
	client *http.Client
}

func newHttpClient() HttpClient {
	return HttpClient{&http.Client{}}
}

func (cli *HttpClient) sendPost(url string) (string, error) {
	fmt.Println("URL:>", url)

	// var jsonStr = []byte(`{"title":"Buy cheese and bread for breakfast."}`)
	req, err := http.NewRequest("POST", url, bytes.NewBuffer([]byte(testRequests[0])))
	req.Header.Set("X-Custom-Header", "myvalue")
	req.Header.Set("Content-Type", "application/json")

	resp, err := cli.client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	fmt.Println("response Status:", resp.Status)
	fmt.Println("response Headers:", resp.Header)

	body, _ := io.ReadAll(resp.Body)
	// fmt.Println("body:", string(body))
	return string(body), nil
}

func (cli *HttpClient) sendSearchRequestToElastic(index, body string) (string, error) {
	//const urlPrefix = "http://elasticsearch_direct:9200/"
	const urlPrefix = "http://localhost:9202/"
	const urlSuffix = "/_search"
	return cli.sendPost(urlPrefix + index + urlSuffix)
}

func (cli *HttpClient) sendSearchRequestToQuesma(index, body string) (string, error) {
	const urlPrefix = "http://localhost:8080/"
	const urlSuffix = "/_search"
	return cli.sendPost(urlPrefix + index + urlSuffix)
}
