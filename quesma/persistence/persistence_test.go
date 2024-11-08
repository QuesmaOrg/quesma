// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package persistence

import (
	"context"
	"fmt"
	"github.com/stretchr/testify/assert"
	"net/url"
	"quesma/logger"
	"quesma/quesma/config"
	"quesma/quesma/types"
	"testing"
	"time"
)

func TestNewElasticPersistence(t *testing.T) {

	var p JSONDatabase

	// change to false if you want to test non-trivial persistence
	if false {
		p = NewStaticJSONDatabase()
	} else {
		indexName := fmt.Sprintf("quesma_test_%d", time.Now().UnixMicro())

		realUrl, err := url.Parse("http://localhost:9200")

		if err != nil {
			t.Fatal(err)
		}

		var cfgUrl config.Url = config.Url(*realUrl)

		cfg := config.ElasticsearchConfiguration{
			Url:      &cfgUrl,
			User:     "",
			Password: "",
		}

		p = NewElasticJSONDatabase(cfg, indexName)
		fmt.Println("??")
	}

	m1 := make(types.JSON)
	m1["foo"] = "bar"

	d1, ok, err := p.Get("t1")

	if err != nil {
		t.Fatal(err)
	}

	if d1 != "" {
		t.Fatal("expected emptiness")
	}

	if ok {
		t.Fatal("expected not ok")
	}

	m1str, err := m1.Bytes()
	if err != nil {
		t.Fatal(err)
	}

	err = p.Put("t1", string(m1str))
	if err != nil {
		t.Fatal(err)
	}

	d2str, ok, err := p.Get("t1")
	if err != nil {
		t.Fatal(err)
	}

	if !ok {
		t.Fatal("expected ok")
	}

	d2 := types.MustJSON(d2str)
	if d2["foo"] != "bar" {
		t.Fatal("expected bar")
	}

}

func TestJSONDatabaseWithEviction_noEviction(t *testing.T) {
	const precise = true

	logger.InitSimpleLoggerForTests()
	indexName := fmt.Sprintf("quesma_test_%d", time.Now().UnixMilli())
	fmt.Println("indexName:", indexName)

	realUrl, err := url.Parse("http://localhost:9200")
	assert.NoError(t, err)

	cfgUrl := config.Url(*realUrl)
	cfg := config.ElasticsearchConfiguration{
		Url:      &cfgUrl,
		User:     "",
		Password: "",
	}

	const bigSizeLimit = int64(1_000_000_000)
	db := NewElasticDatabaseWithEviction(context.Background(), cfg, indexName, bigSizeLimit)

	// check initial state
	assert.Equal(t, bigSizeLimit, db.SizeInBytesLimit())

	docCount, ok := db.DocCount()
	assert.True(t, ok)
	assert.Equal(t, 0, docCount)

	sizeInBytes, ok := db.SizeInBytes()
	assert.True(t, ok)
	assert.Equal(t, int64(0), sizeInBytes)

	// put first documents
	docs := []*document{
		doc("doc1", 100),
		doc("doc2", 200),
		doc("doc3", 300),
		doc("doc4", 400),
		doc("doc5", 500),
	}
	for _, d := range docs {
		assert.True(t, db.Put(d))
	}

	if precise {
		time.Sleep(4 * time.Second)
		docCount, ok = db.DocCount()
		assert.True(t, ok)
		assert.Equal(t, 5, docCount)
	} else {
		docCount, ok = db.DocCount()
		assert.True(t, ok)
		assert.True(t, docCount >= 0)
	}

	val, ok := db.Get(docs[0].Id)
	fmt.Println(val, ok)
	// TODO: deserialize and check content

	db.Delete(docs[1].Id)
	db.Delete(docs[3].Id)

	if precise {
		time.Sleep(1 * time.Second)
		docCount, ok = db.DocCount()
		assert.True(t, ok)
		assert.Equal(t, 3, docCount)
	} else {
		docCount, ok = db.DocCount()
		assert.True(t, ok)
		assert.True(t, docCount >= 0)
	}

	assert.Equal(t, bigSizeLimit, db.SizeInBytesLimit())
}

const updateTime = 4 * time.Second

func TestJSONDatabaseWithEviction_withEviction(t *testing.T) {
	logger.InitSimpleLoggerForTests()
	indexName := fmt.Sprintf("quesma_test_%d", time.Now().UnixMilli())

	realUrl, err := url.Parse("http://localhost:9200")
	assert.NoError(t, err)

	cfgUrl := config.Url(*realUrl)
	cfg := config.ElasticsearchConfiguration{
		Url:      &cfgUrl,
		User:     "",
		Password: "",
	}

	const smallSizeLimit = int64(1200)
	db := NewElasticDatabaseWithEviction(context.Background(), cfg, indexName, smallSizeLimit)
	fmt.Println("indexName:", indexName, "fullIndexName:", db.fullIndexName())

	// check initial state
	assert.Equal(t, smallSizeLimit, db.SizeInBytesLimit())

	docCount, ok := db.DocCount()
	assert.True(t, ok)
	assert.Equal(t, 0, docCount)

	sizeInBytes, ok := db.SizeInBytes()
	assert.True(t, ok)
	assert.Equal(t, int64(0), sizeInBytes)

	// put first documents
	docs := []*document{
		doc("doc1", 200),
		doc("doc2", 300),
		doc("doc3", 400),
		doc("doc4", 500),
		doc("doc5", 500),
	}
	for _, d := range docs[:2] {
		fmt.Println("put", d.SizeInBytes, db.Put(d))
	}
	time.Sleep(updateTime)
	fmt.Println("put", docs[2].SizeInBytes, db.Put(docs[2]))
	time.Sleep(updateTime)

	docCount, ok = db.DocCount()
	assert.True(t, ok)
	assert.Equal(t, 3, docCount)

	db.Delete("doc2")
	time.Sleep(updateTime)

	docCount, ok = db.DocCount()
	assert.True(t, ok)
	assert.Equal(t, 2, docCount)

	put4 := db.Put(docs[4])
	fmt.Println("put", docs[4].SizeInBytes, put4)
	assert.False(t, put4)

	time.Sleep(3000 * time.Millisecond)

	docCount, ok = db.DocCount()
	assert.True(t, ok)
	assert.Equal(t, 3, docCount)

	//
	/*
		val, ok := db.Get(docs[0].Id)
		fmt.Println(val, ok)
		// TODO: deserialize and check content

		db.Delete(docs[1].Id)
		db.Delete(docs[3].Id)

			time.Sleep(1 * time.Second)
			docCount, ok = db.DocCount()
			assert.True(t, ok)
			assert.Equal(t, 3, docCount)
		} else {
			docCount, ok = db.DocCount()
			assert.True(t, ok)
			assert.True(t, docCount >= 0)
		}


	*/
	assert.Equal(t, smallSizeLimit, db.SizeInBytesLimit())
}

func doc(id string, size int64) *document {
	return &document{
		Id:          id,
		SizeInBytes: size,
		Timestamp:   time.Now(),
	}
}
