package main

import (
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

		resp, err := http.Get(url)

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
