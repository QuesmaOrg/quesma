package quesma

import (
	"encoding/json"
	"fmt"
	"log"
	"mitmproxy/quesma/clickhouse"
	"mitmproxy/quesma/quesma/recovery"
	"mitmproxy/quesma/util"
	"strings"
)

func dualWriteBulk(optionalTableName string, body string, lm *clickhouse.LogManager) {
	defer recovery.LogPanic()
	fmt.Printf("%s/_bulk  --> clickhouse, body(shortened): %s\n", optionalTableName, util.Truncate(body))
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
			if !ok || (createObj["_index]"] == nil || len(tableName) > 0) {
				fmt.Println("Invalid create JSON in _bulk:", action)
				continue
			}
			tableName, ok := createObj["_index"].(string)
			if !ok {
				if len(tableName) == 0 {
					fmt.Println("Invalid create JSON in _bulk, no _index name:", action)
					continue
				} else {
					tableName = optionalTableName
				}
			}
			err := lm.ProcessInsertQuery(tableName, document)
			if err != nil {
				log.Fatal(err)
			}
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

func dualWrite(tableName string, body string, lm *clickhouse.LogManager) {
	defer recovery.LogPanic()
	fmt.Printf("%s  --> clickhouse, body(shortened): %s\n", tableName, util.Truncate(body))
	if len(body) == 0 {
		return
	}
	err := lm.ProcessInsertQuery(tableName, body)
	if err != nil {
		log.Fatal(err)
	}
}
