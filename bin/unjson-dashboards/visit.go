// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package main

import (
	"encoding/json"
	"fmt"
	"github.com/gorilla/websocket"
	"io/ioutil"
	"log"
	"net/http"
	"os/exec"
	"time"
)

const (
// Change this if Kibana runs on another port

)

// Dashboard struct to parse JSON response
type KibanaResponse struct {
	SavedObjects []struct {
		ID string `json:"id"`
	} `json:"saved_objects"`
}

func fetchDashboardIDs() ([]string, error) {
	// Create HTTP request
	req, err := http.NewRequest("GET", kibanaURL+dashboardListEndpoint, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("kbn-xsrf", "true")

	// Send request
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	// Read response
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	// Parse JSON
	var kibanaResp KibanaResponse
	err = json.Unmarshal(body, &kibanaResp)
	if err != nil {
		return nil, err
	}

	// Extract IDs
	var ids []string
	for _, obj := range kibanaResp.SavedObjects {
		ids = append(ids, obj.ID)
	}
	return ids, nil
}

func OpenPageInChrome(url string) error {
	webSocketURL, err := GetDebuggerWebSocketURL()
	if err != nil {
		return err
	}

	// Connect to the WebSocket debugger
	conn, _, err := websocket.DefaultDialer.Dial(webSocketURL, nil)
	if err != nil {
		return fmt.Errorf("failed to connect to Chrome WebSocket: %v", err)
	}
	defer conn.Close()

	// Create the navigation command
	cmd := CDPRequest{
		ID:     1,
		Method: "Page.navigate",
		Params: map[string]string{"url": url},
	}

	// Convert command to JSON
	cmdJSON, err := json.Marshal(cmd)
	if err != nil {
		return fmt.Errorf("failed to marshal JSON: %v", err)
	}

	// Send command to Chrome
	err = conn.WriteMessage(websocket.TextMessage, cmdJSON)
	if err != nil {
		return fmt.Errorf("failed to send message: %v", err)
	}

	log.Println("Successfully navigated to:", url)

	// Keep connection open for a short time to allow navigation
	time.Sleep(2 * time.Second)
	return nil
}

func openDashboards(ids []string) {

	options := "?_g=(filters:!(),time:(from:now-1y,to:now))"

	for _, id := range ids {
		url := fmt.Sprintf("%s/app/dashboards#/view/%s%s", kibanaURL, id, options)
		fmt.Println("Opening:", url)

		err := OpenPageInChromeViaWebsocket(url)

		if err != nil {
			log.Printf("Failed to open dashboard %s: %v", id, err)
		}

		log.Println("Waiting 10 seconds before opening the next dashboard...")
		time.Sleep(10 * time.Second)
	}
}

func startChrome() *exec.Cmd {
	chromePath := "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome" //

	cmd := exec.Command(chromePath, "--remote-debugging-port=9223", "--new-window")
	err := cmd.Start()
	if err != nil {
		log.Fatalf("Failed to start Chrome: %v", err)
	}
	fmt.Println("✅ Chrome started.")
	time.Sleep(5 * time.Second) // Wait for Chrome to fully open
	return cmd
}

func stopChrome(cmd *exec.Cmd) {
	if cmd.Process != nil {
		cmd.Process.Kill()
		fmt.Println("Chrome stopped.")
	}
}

// CDPRequest represents the structure of a DevTools Protocol command
type CDPRequest struct {
	ID     int               `json:"id"`
	Method string            `json:"method"`
	Params map[string]string `json:"params"`
}

// GetDebuggerWebSocketURL fetches the WebSocket debugger URL from Chrome DevTools API
func GetDebuggerWebSocketURL() (string, error) {
	resp, err := http.Get("http://localhost:9223/json")
	if err != nil {
		return "", fmt.Errorf("failed to fetch Chrome DevTools API: %v", err)
	}
	defer resp.Body.Close()

	var tabs []map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&tabs); err != nil {
		return "", fmt.Errorf("failed to parse response: %v", err)
	}

	if len(tabs) == 0 {
		return "", fmt.Errorf("no open tabs found in Chrome")
	}

	webSocketURL, ok := tabs[0]["webSocketDebuggerUrl"].(string)
	if !ok {
		return "", fmt.Errorf("failed to get WebSocket debugger URL")
	}

	return webSocketURL, nil
}

// OpenPageInChrome navigates to a specified URL using Chrome DevTools Protocol
func OpenPageInChromeViaWebsocket(url string) error {
	webSocketURL, err := GetDebuggerWebSocketURL()
	if err != nil {
		return err
	}

	// Connect to the WebSocket debugger
	conn, _, err := websocket.DefaultDialer.Dial(webSocketURL, nil)
	if err != nil {
		return fmt.Errorf("failed to connect to Chrome WebSocket: %v", err)
	}
	defer conn.Close()

	// Create the navigation command
	cmd := CDPRequest{
		ID:     1,
		Method: "Page.navigate",
		Params: map[string]string{"url": url},
	}

	// Convert command to JSON
	cmdJSON, err := json.Marshal(cmd)
	if err != nil {
		return fmt.Errorf("failed to marshal JSON: %v", err)
	}

	// Send command to Chrome
	err = conn.WriteMessage(websocket.TextMessage, cmdJSON)
	if err != nil {
		return fmt.Errorf("failed to send message: %v", err)
	}

	log.Println("Successfully navigated to:", url)

	// Keep connection open for a short time to allow navigation
	time.Sleep(2 * time.Second)
	return nil
}

func visitDashboards() {

	cmd := startChrome()
	defer stopChrome(cmd)

	ids, err := fetchDashboardIDs()
	if err != nil {
		log.Fatalf("Failed to fetch dashboard IDs: %v", err)
	}

	fmt.Printf("Found %d dashboards. Opening...\n", len(ids))
	openDashboards(ids)
}
