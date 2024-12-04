// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0

package main

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
)

func sendRequest(url string, requestBody []byte) {
	// Send POST request
	resp, err := http.Post(url, "application/json", bytes.NewBuffer(requestBody))
	if err != nil {
		fmt.Println("Error sending request:", err)
		return
	}
	defer resp.Body.Close()
	if err != nil {
		fmt.Println(err)
	} else {
		respBody, err := io.ReadAll(resp.Body)
		resp.Body = io.NopCloser(bytes.NewBuffer(respBody))
		if err != nil {
			fmt.Println(err)
		} else {
			fmt.Println(string(respBody))
		}
	}
}
