// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package main

import (
	"fmt"
	"net/http"
	"os"
)

func main() {
	url := "http://localhost:9999/_quesma/health"

	resp, err := http.Get(url)
	if err != nil {
		fmt.Println("Error:", err)
		os.Exit(1)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusOK {
		os.Exit(0)
	} else {
		fmt.Println("Fail, Response Status Code:", resp.StatusCode)
		os.Exit(1)
	}
}
