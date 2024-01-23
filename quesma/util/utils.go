package util

import (
	"bytes"
	"encoding/json"
	"io"
)

func Truncate(body string) string {
	if len(body) < 70 {
		return body
	}
	return body[:70]
}

func JsonPrettify(jsonStr string) string {
	data := []byte(jsonStr)
	empty := []byte{}
	buf := bytes.NewBuffer(empty)
	err := json.Indent(buf, data, "", "    ")
	if err != nil {
		panic(err)
	}
	readBuf, _ := io.ReadAll(buf)
	return string(readBuf)
}
