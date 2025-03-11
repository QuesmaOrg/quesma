// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package tables

import (
	"fmt"
	"math/rand"
)

type E2eTable1 struct{}

func (t E2eTable1) Name() string {
	return "e2e_table_1"
}

func (t E2eTable1) RowsNr() int {
	return 5
}

// 'meme' will be a full text field
// 'keyword' - a keyword field
func (t E2eTable1) GenerateCreateTableString() string {
	return fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			"meme" String CODEC(ZSTD(1)),
			"keyword" LowCardinality(String),
			"shoe_size" Int64 CODEC(DoubleDelta, LZ4),
			"timestamp" DateTime64 DEFAULT now64()
		)
		ENGINE = MergeTree
		ORDER BY timestamp`, t.Name())
}

// using every r.Intn in a new line to be more sure I'll get exactly the same data with the same seed
func (t E2eTable1) GenerateOneRow(r *rand.Rand) (clickhouse, elastic string) {
	memes := []string{"Distracted Boyfriend", "Mocking SpongeBob", "Two Buttons", "Expanding Brain", "Roll Safe",
		"Is This a Pigeon?", "Drake Hotline Bling", "Surprised Pikachu", "Change My Mind", "Y Tho"}
	keywords := []string{"funny", "meme", "lol", "haha", "comedy", "humor", "joke", "hilarious",
		"laugh", "silly", "witty", "amusing", "entertaining", "jolly", "jocular", "facetious", "droll",
		"waggish", "absurd", "ridiculous", "ludicrous", "farce", "mockery", "parody", "satire", "irony",
		"sarcasm", "wit", "banter", "raillery", "teasing", "mocking", "derision", "scorn"}
	shoeSizes := []int{2, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45}

	meme := memes[r.Intn(len(memes))]
	keyword := keywords[r.Intn(len(keywords))]
	shoeSize := shoeSizes[r.Intn(len(shoeSizes))]
	tsClickhouse, tsElastic := generateRandomTimestamp(r)
	return fmt.Sprintf("('%s', '%s', %d, '%s')", meme, keyword, shoeSize, tsClickhouse),
		fmt.Sprintf(`{ "meme" : "%s", "keyword" : "%s", "shoe_size" : %d, "timestamp" : "%s" }`, meme, keyword, shoeSize, tsElastic)
}

// from 2021-12-30 00:00:00 to 2022-01-02 23:59:59
// using every r.Intn in a new line to be more sure I'll get exactly the same data with the same seed
func generateRandomTimestamp(r *rand.Rand) (clickhouse, elastic string) {
	year := 2021 + r.Intn(2)
	var month, day int
	if year == 2021 {
		month = 12
		day = 30 + r.Intn(2)
	} else {
		month = 1
		day = r.Intn(2) + 1
	}
	hour := r.Intn(24)
	minute := r.Intn(60)
	second := r.Intn(60)
	return fmt.Sprintf("%d-%02d-%02d %02d:%02d:%02d", year, month, day, hour, minute, second),
		fmt.Sprintf("%d/%02d/%02d %02d:%02d:%02d", year, month, day, hour, minute, second)
}
