package util

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
)

func Truncate(body string) string {
	if len(body) < 70 {
		return body
	}
	return body[:70]
}

func IsValidJson(jsonStr string) bool {
	var js map[string]interface{}
	return json.Unmarshal([]byte(jsonStr), &js) == nil
}

func prettify(jsonStr string) string {
	data := []byte(jsonStr)
	empty := []byte{}
	buf := bytes.NewBuffer(empty)
	err := json.Indent(buf, data, "", "  ")
	if err != nil {
		panic(err)
	}
	readBuf, _ := io.ReadAll(buf)
	return string(readBuf)
}

func Shorten(body interface{}) interface{} {
	switch bodyType := body.(type) {
	case map[string]interface{}:
		for k, nested := range bodyType {
			bodyType[k] = Shorten(nested)
		}
	case []interface{}:
		if len(bodyType) > 3 {
			t := bodyType[:3]
			t[2] = "..."
			return t
		}
	}
	return body
}

func JsonPrettify(jsonStr string, shorten bool) string {
	var jsonData map[string]interface{}

	err := json.Unmarshal([]byte(jsonStr), &jsonData)
	if err != nil {
		return fmt.Sprintf("Error unmarshalling JSON: %v", err)
	}

	for k, nested := range jsonData {
		if shorten {
			jsonData[k] = Shorten(nested)
		}
	}
	v, err := json.Marshal(jsonData)
	if err != nil {
		return fmt.Sprintf("Error marshalling JSON: %v", err)
	}
	return prettify(string(v))
}
