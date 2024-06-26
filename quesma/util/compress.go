// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package util

import (
	"bytes"
	"github.com/klauspost/compress/zstd"
)

func Compress(data []byte) ([]byte, error) {
	var compressedData bytes.Buffer
	writer, err := zstd.NewWriter(&compressedData)
	if err == nil {
		if _, err = writer.Write(data); err == nil {
			if err = writer.Close(); err == nil {
				return compressedData.Bytes(), nil
			}
		}
	}
	return nil, err
}

func Decompress(data []byte) ([]byte, error) {
	decoder, err := zstd.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	defer decoder.Close()
	var decompressedBuffer bytes.Buffer
	_, err = decompressedBuffer.ReadFrom(decoder)
	return decompressedBuffer.Bytes(), err
}
