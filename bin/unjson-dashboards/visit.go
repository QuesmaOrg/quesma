// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/chromedp/chromedp"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"regexp"
	"strings"
	"time"
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

func openDashboards(ids []string) {

	// this is URL of Kibana dashboard as seen from the chromeDP container
	kibanaURL := "http://kibana:5601"

	options := "?_g=(filters:!(),time:(from:now-1y,to:now))"

	for _, id := range ids {
		dashboardUrl := fmt.Sprintf("%s/app/dashboards#/view/%s%s", kibanaURL, id, options)

		localUrl := fmt.Sprintf("http://localhost:5601/app/dashboards#/view/%s%s", id, options)

		fmt.Println("Opening:", dashboardUrl)

		openPageChromeDP(dashboardUrl, localUrl, id)

		log.Println("Waiting 10 seconds before opening the next dashboard...")
		time.Sleep(1 * time.Second)
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

var invalidChars = regexp.MustCompile(`[<>:"/\\|?*\x00-\x1F]`)

func toFilename(s string) string {

	// Use the host and path, replacing invalid characters
	s = invalidChars.ReplaceAllString(s, "_")

	s = strings.Trim(s, "_") // Remove trailing underscores

	s = strings.TrimSpace(s)
	if len(s) > 255 {
		s = s[:255] // Truncate to safe length
	}

	return s
}

func openPageChromeDP(url string, localUrl string, dashboardId string) {

	allocCtx, cancel := chromedp.NewRemoteAllocator(context.Background(), "http://localhost:9222/json")
	defer cancel()

	ctx, cancel := chromedp.NewContext(allocCtx) //	chromedp.WithDebugf(log.Printf)

	defer cancel()

	// capture screenshot of an element
	var buf []byte

	title := "Kibana Dashboard"

	tasks := chromedp.Tasks{
		chromedp.EmulateViewport(1920, 1080),
		chromedp.Navigate(url),
		chromedp.Sleep(10 * time.Second),
		chromedp.Title(&title),
		chromedp.FullScreenshot(&buf, 90),
	}

	// capture entire browser viewport, returning png with quality=90
	if err := chromedp.Run(ctx, tasks); err != nil {
		log.Fatal(err)
	}

	screenshotFilename := fmt.Sprintf("screenshots/%d-%s.png", time.Now().UnixMilli(), dashboardId)

	if err := os.WriteFile(screenshotFilename, buf, 0o644); err != nil {
		log.Fatal(err)
	}

	indexHtml := fmt.Sprintf("screenshots/index.html")

	// Create index.html file
	if _, err := os.Stat(indexHtml); os.IsNotExist(err) {
		_, err := os.Create(indexHtml)
		if err != nil {
			log.Fatal(err)
		}
	}

	// Append to index.html
	f, err := os.OpenFile(indexHtml, os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatal(err)

	}
	defer f.Close()

	_, err = f.WriteString(fmt.Sprintf(`%s %s - <a href="%s">screenshot</a>  <a href="%s">dashboard</a><br>`, dashboardId, title, strings.ReplaceAll(screenshotFilename, "screenshots/", ""), localUrl))

	_, err = f.WriteString(fmt.Sprintf(`<img src="%s" width="800">`, strings.ReplaceAll(screenshotFilename, "screenshots/", "")))
	f.WriteString("<hr>\n")

	log.Println("Screenshot '", title, "'  saved to", url, screenshotFilename)

}

func visitDashboards() {

	ids, err := fetchDashboardIDs()
	if err != nil {
		log.Fatalf("Failed to fetch dashboard IDs: %v", err)
	}

	fmt.Printf("Found %d dashboards. Opening...\n", len(ids))
	openDashboards(ids)
}
