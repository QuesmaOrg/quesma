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

func (cli *HttpClient) sendPost(url, body string) (string, error) {
	fmt.Println("URL:>", url)

	req, err := http.NewRequest("POST", url, bytes.NewBuffer([]byte(body)))
	req.Header.Set("Content-Type", "application/json")

	resp, err := cli.client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	fmt.Println("response Status:", resp.Status)
	fmt.Println("response Headers:", resp.Header)

	response, err := io.ReadAll(resp.Body)
	return string(response), err
}

func (cli *HttpClient) sendRequestToElastic(urlSuffix, body string) (string, error) {
	const urlPrefix = "http://localhost:9202"
	return cli.sendPost(urlPrefix+urlSuffix, body)
}

func (cli *HttpClient) sendRequestToQuesma(urlSuffix, body string) (string, error) {
	const urlPrefix = "http://localhost:8080"
	return cli.sendPost(urlPrefix+urlSuffix, body)
}
