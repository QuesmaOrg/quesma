package gzip

import (
	"bytes"
	"compress/gzip"
	"io"
	"net/http"
	"strings"
)

func Zip(content []byte) ([]byte, error) {
	var b bytes.Buffer
	gz := gzip.NewWriter(&b)
	if _, err := gz.Write(content); err != nil {
		return nil, err
	}
	if err := gz.Close(); err != nil {
		return nil, err
	}
	return b.Bytes(), nil
}

func UnZip(gzippedData []byte) ([]byte, error) {
	reader := bytes.NewReader(gzippedData)
	gzipReader, err := gzip.NewReader(reader)
	if err != nil {
		return nil, err
	}
	defer gzipReader.Close()
	return io.ReadAll(gzipReader)
}

func IsGzipped(elkResponse *http.Response) bool {
	return strings.Contains(elkResponse.Header.Get("Content-Encoding"), "gzip")
}
