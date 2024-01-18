package main

import (
	"bytes"
	"io"
	"log"
	"math/rand"
	"net/http"
	"time"
)

const url = "http://mitmproxy:8080/_search?pretty"

func main() {

	for {
		time.Sleep(time.Duration(1000+rand.Intn(2000)) * time.Millisecond)
		bstr := `
		{
			"query": {
			  "match_all": {}
			}
		 }
		`
		body := []byte(bstr)
		resp, err := http.Post(url, "application/json", bytes.NewBuffer(body))

		if err != nil {
			log.Fatal(err)
		}
		_, err = io.ReadAll(resp.Body)
		if err != nil {
			log.Println(err)
		}

		resp.Body.Close()
	}
}
