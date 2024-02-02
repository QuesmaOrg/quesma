package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"strconv"
	"strings"
	"time"
)

const (
	url                  = "http://mitmproxy:8080/_bulk"
	bulkJson             = `{"create":{"_index":"device_logs"}}`
	generateEveryMs      = 1000
	generateRandomnessMs = 2000
	generateLogLines     = 10
)

const (
	epochTimeFormat   = "2006-01-02T15:04:05-0700"
	etDayHourFormat   = "2006.01.02.15"
	etDayFormat       = "2006.01.02"
	etDayFormatDashes = "2006-01-02"
)

const ( // Distribution types for randomization of generated values
	NormalDistribution  = "normal"
	UniformDistribution = "uniform"
)

// A generic function which returns random element of an array of any type
func randomizedValue[T any](values []T, distributionType string) T {
	switch distributionType {
	case UniformDistribution:
		return randomElementUniformDist(values)
	case NormalDistribution:
		return randomElementNormalDist(values)
	default:
		return randomElementUniformDist(values)
	}
}

func randomElementUniformDist[T any](array []T) T {
	return array[rand.Intn(len(array))]
}

func randomElementNormalDist[T any](array []T) T {
	meanIndex, stdDev := len(array)/2, len(array)/3
	randomIndex := int(rand.NormFloat64()*float64(stdDev) + float64(meanIndex))
	return array[randomIndex%len(array)]
}

func ipv6Address() string {
	return randomizedValue([]string{
		"2409:4070:4003:a299:40f9:28e1:eb5b:215e",
		"2409:4070:4003:a299:40f9:28e1:ec9a:150d",
		"2409:4070:4003:a299:40f9:28e1:ab6f:78ca",
		"2409:4070:4003:a299:40f9:28e1:fc3e:4321",
		"2409:4070:4003:a299:40f9:28e1:8d2b:1aef",
		"2409:4070:4003:a299:40f9:28e1:3f76:9cda",
		"2409:4070:4003:a299:40f9:28e1:6079:3b8c",
		"2409:4070:4003:a299:40f9:28e1:2167:e940",
		"2409:4070:4003:a299:40f9:28e1:2d1f:60b8",
		"2409:4070:4003:a299:40f9:28e1:f8b3:a45d",
		"2001:0db8:85a3:0000:0000:8a2e:0370:7334",
		"2001:0db8:85a3:0000:0000:8a2e:0370:7334",
		"2001:0db8:85a3:0000:0000:8a2e:0370:7334",
		"2001:0db8:85a3:0000:0000:8a2e:0370:7334",
		"2001:0db8:85a3:0000:0000:8a2e:0370:7334",
	}, UniformDistribution)
}

type Handset struct {
	Maker, Model string
}

func handset() Handset {
	return randomizedValue([]Handset{
		{"Xiaomi", "Abcdee-Rdddi 66A"},
		{"Xiaomi", "Xyz 12B"},
		{"Samsung", "Samsung Galaxy S21"},
		{"Apple", "iPhone 13 Pro"},
		{"Google", "Google Pixel 6"},
		{"OnePlus", "OnePlus 9"},
		{"Sony", "Sony Xperia 5 III"},
		{"Huawei", "Huawei P40"},
	}, NormalDistribution)
}

func connectionQuality() string {
	return randomizedValue([]string{
		"veryfast",
		"fast",
		"average",
		"slow",
		"veryslow",
	}, NormalDistribution)
}

const charset = "abcdef0123456789"

func randomHexString(length int) string {
	b := make([]byte, length)
	for i := range b {
		b[i] = charset[rand.Intn(len(charset))]
	}
	return string(b)
}

func randomBoolString() string {
	randomBooleanVal := randomizedValue([]bool{true, false}, NormalDistribution)
	return strconv.FormatBool(randomBooleanVal)
}

func randomInteger(upperBound int) string {
	return fmt.Sprintf("%d", rand.Intn(upperBound))
}

func uuid() string {
	return fmt.Sprintf("%s-%s-%s-%s-%s", randomHexString(8), randomHexString(4), randomHexString(4), randomHexString(4), randomHexString(12))
}

func generateLogLine(logTime time.Time) []byte {
	userHandset := handset()
	deviceLogArray := []string{`
    {
    "properties": {
    "enriched_client_ip": "`, ipv6Address(), `",
    "user_handset_model": "`, userHandset.Model, `",
    "time_taken_for_network_operation": `, randomInteger(4000), `,
    "enriched_app_id": "DH",
    "is_in_fg": `, randomBoolString(), `,
    "signed_state": "signed_in",
    "enriched_event_attribution": "deep_link",
    "user_connection_quality": "`, connectionQuality(), `",
    "estimated_connection_speedinkbps": 23067.048828125,
    "server_loc": "#,#,c32_800,s32,",
    "app_id": "DH_APP",
    "signin_medium": "MOBILE",
    "user_type": "user",
    "enriched_user_id": "dh123456",
    "tabname": "మీ కోసం",
    "fbestimation_connection_speedinkbps": 23067.048828125,
    "feed_latency": "`, randomInteger(2000), `",
    "pv_event": "`, randomBoolString(), `",
    "user_language_primary": "te",
    "session_start_time": "2024-01-01T22:25:21+0530",
    "card_count": 10,
    "exoestimation_connection_speedinkbps": `, randomInteger(10000), `,
    "tabtype": "hashtag",
    "user_feed_type": "LR",
    "enriched_user_language_primary": "te",
    "entry_time": 1704129696028,
    "user_app_ver": "27.2.9",
    "session_id": "`, uuid(), `",
    "fg_session_duration": `, randomInteger(10000), `,
    "ftd_session_count": 202,
    "network_service_provider": "AAA 4G",
    "fg_session_id": "`, uuid(), `",
    "referrer_action": "scroll",
    "user_os_ver": "9",
    "user_os_name": "rel",
    "selected_country": "in",
	"user_handset_maker": "`, userHandset.Maker, `",
    "fg_session_count": `, randomInteger(300), `,
    "ab_NewsStickyType": "TYPE1",
    "country_detection_mechanism": "network_country",
    "event_attribution": "deep_link",
    "isreg": `, randomBoolString(), `,
    "tabindex": 0,
    "ftd_session_time": `, randomInteger(10000), `,
    "tabitem_id": "`, randomHexString(32), `",
    "latest_pagenumber": "7",
    "user_connection": "4G"
    },
    "dedup_id": "hashtag`, randomHexString(32), `hashtag",
    "client_id": "dh.12345678",
    "timestamps": {
    "topology_entry_time": "`, logTime.Format(etDayFormatDashes), `T22:51:36+0530"
    },
    "client_ip": "`, ipv6Address(), `",
    "event_section": "news",
    "ts_day": "`, logTime.Format(etDayFormatDashes), `",
    "user_id": "dh123456789",
    "event_name": "story_list_view",
    "ts_time_druid": "`, logTime.Format(etDayFormatDashes), `T22:00:00",
    "epoch_time": "`, logTime.Format(epochTimeFormat), `",
    "et_day_hour": "`, logTime.Format(etDayHourFormat), `",
    "et_day": "`, logTime.Format(etDayFormat), `",
    "epoch_time_original": 1704129690,
    "ts_day_hour": "`, logTime.Format(etDayFormatDashes), `-22"
    }`}

	deviceLog := strings.Join(deviceLogArray, "")

	data := map[string]interface{}{}
	if err := json.Unmarshal([]byte(deviceLog), &data); err != nil {
		log.Fatal(err)
	}
	serialized, err := json.Marshal(data)
	if err != nil {
		log.Fatal(err)
	}

	return serialized
}

func main() {
	for {
		sleepDuration := time.Duration(generateEveryMs+rand.Intn(generateRandomnessMs)) * time.Millisecond
		time.Sleep(sleepDuration)

		now := time.Now().UTC()

		logBytes := []byte{}
		for i := 0; i < generateLogLines; i++ {
			shift := rand.Int63n(sleepDuration.Milliseconds())
			logTime := now.Add(-time.Duration(shift) * time.Millisecond)
			logLine := generateLogLine(logTime)

			logBytes = append(logBytes, []byte(bulkJson)...)
			logBytes = append(logBytes, []byte("\n")...)
			logBytes = append(logBytes, logLine...)
			logBytes = append(logBytes, []byte("\n")...)
		}

		// fmt.Println("Sending logs:", url)
		// fmt.Println(string(logBytes))

		resp, err := http.Post(url, "application/json", bytes.NewBuffer(logBytes))

		if err != nil {
			log.Fatal(err)
		}

		if err := resp.Body.Close(); err != nil {
			log.Fatal(err)
		}
	}
}
