package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"

	"log"
	"math/rand"
	"net/http"
	"os"
	"strings"
	"time"
)

const elasticSearchBulkUrl = "http://elasticsearch:9200/_bulk"
const windowsJsonFile = "assets/windows_logs.json"

const windowsBulkJson = `{"create":{"_index":"windows_logs"}}`

var random = rand.New(rand.NewSource(time.Now().UnixNano()))

type process struct {
	name       string
	executable string
}

var processes []process = []process{
	{"cmd.exe", "C:\\Windows\\System32\\cmd.exe"},
	{"powershell.exe", "C:\\Windows\\System32\\powershell.exe"},
	{"explorer.exe", "C:\\Windows\\System32\\explorer.exe"},
	{"quesma.exe", "C:\\Qesma\\quesma.exe"},
}

type networkLogEntry struct {
	Process struct {
		Name       string `json:"name"`
		Pid        int    `json:"pid"`
		EntityID   string `json:"entity_id"`
		Executable string `json:"executable"`
	} `json:"process"`
	Destination struct {
		Address string `json:"address"`
		Port    string `json:"port"`
	} `json:"destination"`
	Source struct {
		Address string `json:"address"`
		Port    string `json:"port"`
	} `json:"source"`
	Network struct {
		Direction string `json:"direction"`
		Protocol  string `json:"protocol"`
	} `json:"network"`
	Timestamp string `json:"@timestamp"`
	Event     struct {
		Category string `json:"category"`
	} `json:"event"`
	User struct {
		FullName string `json:"full_name"`
		Domain   string `json:"domain"`
		ID       string `json:"id"`
	} `json:"user"`
}

type registryLogEntry struct {
	Registry struct {
		Path  string `json:"path"`
		Value string `json:"value"`
		Key   string `json:"key"`
	} `json:"registry"`
	Process struct {
		Name       string `json:"name"`
		Pid        int    `json:"pid"`
		EntityID   string `json:"entity_id"`
		Executable string `json:"executable"`
	} `json:"process"`
	Timestamp string `json:"@timestamp"`
	Event     struct {
		Category string `json:"category"`
	} `json:"event"`
}

type processLogEntry struct {
	Process struct {
		Name       string `json:"name"`
		Pid        int    `json:"pid"`
		EntityID   string `json:"entity_id"`
		Executable string `json:"executable"`
	} `json:"process"`
	Timestamp string `json:"@timestamp"`
	Event     struct {
		Category string `json:"category"`
		Type     string `json:"type"`
	} `json:"event"`
}

type libraryLogEntry struct {
	Process struct {
		Name       string `json:"name"`
		Pid        int    `json:"pid"`
		EntityID   string `json:"entity_id"`
		Executable string `json:"executable"`
	} `json:"process"`
	Dll struct {
		Path string `json:"path"`
		Name string `json:"name"`
	} `json:"dll"`
	Timestamp string `json:"@timestamp"`
	Event     struct {
		Category string `json:"category"`
	} `json:"event"`
}

func randomRegistryEntry(shift time.Duration) []byte {

	var entry registryLogEntry

	start := time.Now().Add(shift)
	entry.Timestamp = start.Format(time.RFC3339)

	entry.Event.Category = "registry"

	switch random.Intn(2) {
	case 0:
		entry.Registry.Path = `HKEY_LOCAL_MACHINE\Software\Microsoft\Windows\CurrentVersion\Run`
		entry.Registry.Key = "explorer"
		entry.Registry.Value = "C:\\Windows\\System32\\explorer.exe"
	case 1:
		entry.Registry.Path = `HKEY_LOCAL_MACHINE\Software\Microsoft\Windows\CurrentVersion\Run`
		entry.Registry.Key = "cmd"
		entry.Registry.Value = "C:\\Windows\\System32\\cmd.exe"
	}

	p := random.Intn(len(processes))

	entry.Process.Name = processes[p].name
	entry.Process.Executable = processes[p].executable
	entry.Process.Pid = random.Intn(1024)

	data, err := json.Marshal(entry)
	if err != nil {
		log.Println(err)
	}

	return toBulk(data)
}

func randomNetworkEntry(shift time.Duration) []byte {

	var entry networkLogEntry

	entry.Event.Category = "network"

	start := time.Now().Add(shift)
	entry.Timestamp = start.Format(time.RFC3339)

	p := random.Intn(len(processes))

	entry.Process.Name = processes[p].name
	entry.Process.Executable = processes[p].executable
	entry.Process.Pid = random.Intn(1024)
	entry.Process.EntityID = uuid()

	switch random.Intn(2) {
	case 0:
		entry.Network.Protocol = "tcp"
		entry.Network.Direction = "outbound"
		entry.Source.Address = "quesma.com"
	case 1:
		entry.Network.Protocol = "udp"
		entry.Network.Direction = "inbound"
		entry.Source.Address = "quesma.com"
	}

	entry.User.Domain = "quesma"
	entry.User.FullName = "Quesma User"
	entry.User.ID = "quesma"

	data, err := json.Marshal(entry)
	if err != nil {
		log.Println(err)
	}
	return toBulk(data)

}

func randomProcessEntry(shift time.Duration) []byte {

	var entry processLogEntry

	entry.Event.Category = "process"

	switch random.Intn(3) {
	case 0:
		entry.Event.Type = "start"
	case 1:
		entry.Event.Type = "stop"
	case 2:
		entry.Event.Type = "crash"
	}

	start := time.Now().Add(shift)
	entry.Timestamp = start.Format(time.RFC3339)

	p := random.Intn(len(processes))

	entry.Process.Name = processes[p].name
	entry.Process.Executable = processes[p].executable
	entry.Process.Pid = random.Intn(1024)

	data, err := json.Marshal(entry)
	if err != nil {
		log.Println(err)
	}

	return toBulk(data)
}

func randomLibraryEntry(shift time.Duration) []byte {

	var entry libraryLogEntry

	entry.Event.Category = "process"

	start := time.Now().Add(shift)
	entry.Timestamp = start.Format(time.RFC3339)

	p := random.Intn(len(processes))

	entry.Process.Name = processes[p].name
	entry.Process.Executable = processes[p].executable
	entry.Process.Pid = random.Intn(1024)

	data, err := json.Marshal(entry)
	if err != nil {
		log.Println(err)
	}

	return toBulk(data)
}

func toBulk(serialized []byte) (logBytes []byte) {
	logBytes = append(logBytes, []byte(windowsBulkJson)...)
	logBytes = append(logBytes, []byte("\n")...)
	logBytes = append(logBytes, serialized...)
	logBytes = append(logBytes, []byte("\n")...)
	return logBytes
}

func sendToWindowsLog(logBytes []byte) {

	// We need the same data in both places for manual testing purposes.
	// This is temporary and will be removed when we'll have end-to-end tests.
	//

	sendToWindowsLogTo(elasticSearchBulkUrl, logBytes)
	sendToWindowsLogTo(configureTargetUrl(), logBytes)
}

func sendToWindowsLogTo(targetUrl string, logBytes []byte) {

	if resp, err := http.Post(targetUrl, "application/json", bytes.NewBuffer(logBytes)); err != nil {
		log.Printf("Failed to send windows logs: %v", err)
	} else {
		fmt.Printf("Sent windows_logs response=%s\n", resp.Status)
		if err := resp.Body.Close(); err != nil {
			log.Fatal(err)
		}
	}
}

func windowsRandomEntry(shift time.Duration) []byte {

	generators := []func(time.Duration) []byte{
		randomRegistryEntry,
		randomNetworkEntry,
		randomProcessEntry,
		randomLibraryEntry,
	}

	generator := generators[random.Intn(len(generators))]
	return generator(shift)
}

func windowsLogGeneratorRandom() {

	// some past events
	const numberOfPastEvents = 120
	const pastEventsWindow = 2 * 3600 // definition of the past  in seconds

	for range numberOfPastEvents {

		// throttle a bit
		time.Sleep(10 * time.Millisecond)

		logBytes := windowsRandomEntry(-time.Second * time.Duration(random.Intn(pastEventsWindow)))
		sendToWindowsLog(logBytes)
	}

	// some future events

	const futureEventsInterval = 300
	const futureEventsIntervalJitter = 40

	for {
		time.Sleep(time.Duration(futureEventsInterval+random.Intn(futureEventsIntervalJitter)) * time.Second)

		logBytes := windowsRandomEntry(0)
		sendToWindowsLog(logBytes)
	}
}

func windowEntryAlterTime(entry map[string]interface{}, when time.Time) (logBytes []byte) {

	entry["@timestamp"] = when.Format(time.RFC3339)

	serialized, err := json.Marshal(entry)
	if err != nil {
		log.Println(err)
	}

	return toBulk(serialized)
}

func windowsLogGeneratorAssetBased() {

	targetUrl := configureTargetUrl()

	file, err := os.Open(windowsJsonFile)
	if err != nil {

		fmt.Println("Error opening file ", windowsJsonFile, err)
		fmt.Println(`
Warning: 

We can't commit the file to the repository because of licensing issues.


Run the following command to download the file:
curl https://raw.githubusercontent.com/elastic/elasticsearch/8.13/docs/src/yamlRestTest/resources/normalized-T1117-AtomicRed-regsvr32.json -o docker/device-log-generator/assets/windows_logs.json

This is temporary and will be removed in the future.
`)
		return
	}
	defer file.Close()

	entries := make([]map[string]interface{}, 0)

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()

		if strings.HasPrefix(line, "{\"create\":{}") {
			continue
		}

		entry := make(map[string]interface{})
		if err := json.Unmarshal([]byte(line), &entry); err != nil {
			log.Fatal(err)
		}
		entries = append(entries, entry)
	}

	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}

	start := time.Now()

	fmt.Println("entries:", len(entries))

	logBytes := []byte{}

	shift := -len(entries)
	for _, entry := range entries {
		shift++
		logBytes = append(logBytes, windowEntryAlterTime(entry, start.Add(time.Duration(shift)*time.Minute))...)
	}
	fmt.Println("Sending logs to :", targetUrl, "\n", string(logBytes))
	sendToWindowsLog(logBytes)

	sleepDuration := time.Duration(300) * time.Second

	r := rand.NewSource(time.Now().UnixNano())
	for {
		time.Sleep(sleepDuration)

		p := r.Int63() % int64(len(entries))
		entry := entries[p]
		logBytes := []byte{}
		logBytes = windowEntryAlterTime(entry, time.Now())
		sendToWindowsLog(logBytes)

	}
}
