package quesma

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"mitmproxy/quesma/clickhouse"
	"mitmproxy/quesma/queryparser"
	"mitmproxy/quesma/quesma/config"
	"mitmproxy/quesma/quesma/recovery"
	"mitmproxy/quesma/stats"
	"mitmproxy/quesma/util"
	"strings"
)

func dualWriteBulk(ctx context.Context, optionalTableName string, body string, lm *clickhouse.LogManager, cfg config.QuesmaConfiguration) {
	_ = ctx
	if config.TrafficAnalysis.Load() {
		log.Printf("analysing traffic, not writing to Clickhouse %s\n", queryparser.TableName)
		return
	}
	defer recovery.LogPanic()
	jsons := strings.Split(body, "\n")
	for i := 0; i+1 < len(jsons); i += 2 {
		action := jsons[i]
		document := jsons[i+1]

		var jsonData map[string]interface{}

		// Unmarshal the JSON data into the map
		err := json.Unmarshal([]byte(action), &jsonData)
		if err != nil {
			fmt.Println("Invalid action JSON in _bulk:", err, action)
			continue
		}
		if jsonData["create"] != nil {
			createObj, ok := jsonData["create"].(map[string]interface{})
			if !ok {
				fmt.Println("Invalid create JSON in _bulk:", action)
				continue
			}
			indexName, ok := createObj["_index"].(string)
			if !ok {
				if len(indexName) == 0 {
					fmt.Println("Invalid create JSON in _bulk, no _index name:", action)
					continue
				} else {
					indexName = optionalTableName
				}
			}

			withConfiguration(ctx, cfg, indexName, document, func() error {
				stats.GlobalStatistics.Process(indexName, document, clickhouse.NestedSeparator)
				return lm.ProcessInsertQuery(indexName, document)
			})
		} else if jsonData["index"] != nil {
			fmt.Println("Not supporting 'index' _bulk.")
		} else if jsonData["update"] != nil {
			fmt.Println("Not supporting 'update' _bulk.")
		} else if jsonData["delete"] != nil {
			fmt.Println("Not supporting 'delete' _bulk.")
		} else {
			fmt.Println("Invalid action JSON in _bulk:", action)
		}
	}
}

func dualWrite(ctx context.Context, tableName string, body string, lm *clickhouse.LogManager, cfg config.QuesmaConfiguration) {
	_ = ctx
	stats.GlobalStatistics.Process(tableName, body, clickhouse.NestedSeparator)
	if config.TrafficAnalysis.Load() {
		log.Printf("analysing traffic, not writing to Clickhouse %s\n", queryparser.TableName)
		return
	}

	defer recovery.LogPanic()
	if len(body) == 0 {
		return
	}

	withConfiguration(ctx, cfg, tableName, body, func() error {
		return lm.ProcessInsertQuery(tableName, body)
	})
}

func withConfiguration(ctx context.Context, cfg config.QuesmaConfiguration, indexName string, body string, action func() error) {
	if len(cfg.IndexConfig) == 0 {
		log.Printf("%s  --> clickhouse, body(shortened): %s\n", indexName, util.Truncate(body))
		err := action()
		if err != nil {
			log.Fatal(err)
		}
	} else {
		matchingConfig, ok := config.FindMatchingConfig(indexName, cfg)
		if !ok {
			log.Printf("index '%s' is not configured, skipping\n", indexName)
			return
		}
		if matchingConfig.Enabled {
			log.Printf("%s  --> clickhouse, body(shortened): %s\n", indexName, util.Truncate(body))
			err := action()
			if err != nil {
				log.Fatal(err)
			}
		} else {
			log.Printf("index '%s' is disabled, ignoring\n", indexName)
		}
	}
}
