// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package util

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestCompress(t *testing.T) {
	data := []byte("Hello Quesma! Quesma is the best. Quesma is truely awesome.")
	data = append(data, data...)
	compressedData, err := Compress(data)
	assert.NoError(t, err, "Compress() returned an error.")
	decompressedData, err2 := Decompress(compressedData)
	assert.NoError(t, err2, "Decompress() returned an error.")
	assert.Greater(t, len(data), len(compressedData), "Compressed data is not smaller than original data.")
	assert.Equal(t, data, decompressedData)
}
