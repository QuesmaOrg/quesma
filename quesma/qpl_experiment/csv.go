// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package qpl_experiment

import (
	"bytes"
	"encoding/csv"
	"strings"
)

func WriteTableToString(table Table) (string, error) {
	var buffer bytes.Buffer
	writer := csv.NewWriter(&buffer)

	// Write header (Names)
	if err := writer.Write(table.Names); err != nil {
		return "", err
	}

	// Write rows
	for _, row := range table.Rows {
		if err := writer.Write(row); err != nil {
			return "", err
		}
	}

	writer.Flush()

	if err := writer.Error(); err != nil {
		return "", err
	}

	return buffer.String(), nil
}

func ReadTableFromString(data string) (Table, error) {
	reader := csv.NewReader(strings.NewReader(data))

	// Read all lines
	records, err := reader.ReadAll()
	if err != nil {
		return Table{}, err
	}

	if len(records) == 0 {
		return Table{}, nil // Empty table
	}

	// First line is the header (Names)
	table := Table{
		Names: records[0],
	}

	table.Rows = make([]Row, len(records)-1)
	for i, record := range records[1:] {
		table.Rows[i] = record
	}

	return table, nil
}
