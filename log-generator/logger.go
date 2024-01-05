package main

import (
	"bytes"
	"encoding/json"
	"log"
	"math/rand"
	"net/http"
	"time"
)

const url = "http://mitmproxy:8080/logs-generic-default/_doc"

func main() {
	for {
		time.Sleep(time.Duration(1000+rand.Intn(5000)) * time.Millisecond)

		body, err := json.Marshal(map[string]string{
			"timestamp": time.Now().Format("2006-01-02T15:04:05.999Z"),
			"message":   "Something happened!",
			"severity":  "info",
			"source":    "oracle",
		})

		if err != nil {
			log.Fatal(err)
		}

		resp, err := http.Post(url, "application/json", bytes.NewBuffer(body))

		if err != nil {
			log.Fatal(err)
		}

		resp.Body.Close()
	}
}
