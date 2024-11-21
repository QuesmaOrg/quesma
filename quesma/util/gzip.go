// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0

package util

import (
	"bytes"
	"compress/gzip"
	"io"
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
