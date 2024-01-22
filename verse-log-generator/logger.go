package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"time"
)

const url = "http://mitmproxy:8080/device_logs/_doc"

const (
	epochTimeFormat = "2006-01-02T15:04:05-0700"
	etDayHourFormat = "2006.01.02.15"
	etDayFormat     = "2006.01.02"
)

func main() {
	for {
		time.Sleep(time.Duration(1000+rand.Intn(2000)) * time.Millisecond)

		now := time.Now().UTC()

		deviceLog := fmt.Sprintf(`
        {
        "properties": {
        "enriched_client_ip": "2409:4070:4003:a299:40f9:28e1:eb5b:215e",
        "user_handset_model": "Abcdee-Rdddi 66A",
        "time_taken_for_network_operation": 1749,
        "enriched_app_id": "DH",
        "is_in_fg": true,
        "signed_state": "signed_in",
        "enriched_event_attribution": "deep_link",
        "user_connection_quality": "veryfast",
        "estimated_connection_speedinkbps": 23067.048828125,
        "server_loc": "#,#,c32_800,s32,",
        "app_id": "DH_APP",
        "signin_medium": "MOBILE",
        "user_type": "user",
        "enriched_user_id": "dh123456",
        "tabname": "మీ కోసం",
        "fbestimation_connection_speedinkbps": 23067.048828125,
        "feed_latency": "531",
        "pv_event": "true",
        "user_language_primary": "te",
        "session_start_time": "2024-01-01T22:25:21+0530",
        "card_count": 10,
        "exoestimation_connection_speedinkbps": 6775,
        "tabtype": "hashtag",
        "user_feed_type": "LR",
        "enriched_user_language_primary": "te",
        "entry_time": 1704129696028,
        "user_app_ver": "27.2.9",
        "session_id": "8ce01230-a8c6-11ee-b7d7-faccb72d3c70",
        "fg_session_duration": 59037,
        "ftd_session_count": 202,
        "network_service_provider": "AAA 4G",
        "fg_session_id": "8cc7cf40-a8c6-11ee-9e91-2e6000f92ab9",
        "referrer_action": "scroll",
        "user_os_ver": "9",
        "user_os_name": "rel",
        "selected_country": "in",
        "user_handset_maker": "Xiaomi",
        "fg_session_count": 202,
        "ab_NewsStickyType": "TYPE1",
        "country_detection_mechanism": "network_country",
        "event_attribution": "deep_link",
        "isreg": true,
        "tabindex": 0,
        "ftd_session_time": 297,
        "tabitem_id": "91581308b67fdfbcd24028a0c513bc37",
        "latest_pagenumber": "7",
        "user_connection": "4G"
        },
        "dedup_id": "hashtag91581308b67fdfbcd24028a0c513bc37hashtag",
        "client_id": "dh.12345678",
        "timestamps": {
        "topology_entry_time": "2024-01-01T22:51:36+0530"
        },
        "client_ip": "2409:4070:4v03:a2v9:40f9:28e1:eb5b:21ve",
        "event_section": "news",
        "ts_day": "2024-01-01",
        "user_id": "dh123456789",
        "event_name": "story_list_view",
        "ts_time_druid": "2024-01-01T22:00:00",
        "epoch_time": "%s",
        "et_day_hour": "%s",
        "et_day": "%s",
        "epoch_time_original": 1704129690,
        "ts_day_hour": "2024-01-01-22"
        }`, now.Format(epochTimeFormat), now.Format(etDayHourFormat), now.Format(etDayFormat))

		data := map[string]interface{}{}
		json.Unmarshal([]byte(deviceLog), &data)
		serialized, err := json.Marshal(data)
		if err != nil {
			log.Fatal(err)
		}

		if err != nil {
			log.Fatal(err)
		}

		resp, err := http.Post(url, "application/json", bytes.NewBuffer(serialized))

		if err != nil {
			log.Fatal(err)
		}

		resp.Body.Close()
	}
}
