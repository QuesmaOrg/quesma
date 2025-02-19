// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package persistence

import (
	"fmt"
	"github.com/QuesmaOrg/quesma/quesma/quesma/config"
	"github.com/QuesmaOrg/quesma/quesma/quesma/types"
	"github.com/stretchr/testify/assert"
	"net/url"
	"testing"
	"time"
)

const elasticUpdateTime = 2 * time.Second // time to wait for elastic to update

func TestNewElasticPersistence(t *testing.T) {

	var p JSONDatabase

	// change to false if you want to test non-trivial persistence
	if true {
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
	t.Skip("Test passes locally (20.12.2024), but requires elasticsearch to be running, so skipping for now")

	indexName := fmt.Sprintf("quesma_test_%d", time.Now().UnixMilli())
	fmt.Println("indexName:", indexName)

	realUrl, err := url.Parse("http://localhost:9200")
	assert.NoError(t, err)
	cfgUrl := config.Url(*realUrl)
	cfg := config.ElasticsearchConfiguration{Url: &cfgUrl}

	const bigSizeLimit = int64(1_000_000_000)
	db := NewElasticDatabaseWithEviction(cfg, indexName, bigSizeLimit)

	// check initial state
	assert.Equal(t, bigSizeLimit, db.SizeInBytesLimit())

	docCount, err := db.DocCount()
	assert.NoError(t, err)
	assert.Equal(t, 0, docCount)

	sizeInBytes, err := db.SizeInBytes()
	assert.NoError(t, err)
	assert.Equal(t, int64(0), sizeInBytes)

	// put first documents
	docs := []*JSONWithSize{
		doc("doc1", 100),
		doc("doc2", 200),
		doc("doc3", 300),
		doc("doc4", 400),
		doc("doc5", 500),
	}
	for _, d := range docs {
		assert.NoError(t, db.Put(d))
	}

	// check state after put (5 documents  + "get" OK)
	time.Sleep(elasticUpdateTime)
	docCount, err = db.DocCount()
	assert.NoError(t, err)
	assert.Equal(t, 5, docCount)

	val, err := db.Get(docs[0].id)
	assert.NoError(t, err)
	assert.Contains(t, string(val), `"id":"doc1"`)
	assert.Contains(t, string(val), `"sizeInBytes":100`)

	// delete some documents
	err = db.Delete(docs[1].id)
	assert.NoError(t, err)
	err = db.Delete(docs[3].id)
	assert.NoError(t, err)

	// doc_count should be 3 and "get" should fail for deleted documents
	time.Sleep(elasticUpdateTime)
	docCount, err = db.DocCount()
	assert.NoError(t, err)
	assert.Equal(t, 3, docCount)
	val, err = db.Get(docs[1].id)
	assert.Error(t, err)
	assert.Empty(t, val)
	val, err = db.Get(docs[3].id)
	assert.Error(t, err)
	assert.Empty(t, val)

	assert.Equal(t, bigSizeLimit, db.SizeInBytesLimit())
}

func TestJSONDatabaseWithEviction_withEviction(t *testing.T) {
	t.Skip("Test passes locally (20.12.2024), but requires elasticsearch to be running, so skipping for now")

	indexName := fmt.Sprintf("quesma_test_%d", time.Now().UnixMilli())

	realUrl, err := url.Parse("http://localhost:9200")
	assert.NoError(t, err)

	cfgUrl := config.Url(*realUrl)
	cfg := config.ElasticsearchConfiguration{Url: &cfgUrl}

	const smallSizeLimit = int64(1100)
	db := NewElasticDatabaseWithEviction(cfg, indexName, smallSizeLimit)
	fmt.Println("indexName:", indexName, "fullIndexName:", db.fullIndexName())

	// check initial state
	assert.Equal(t, smallSizeLimit, db.SizeInBytesLimit())

	docCount, err := db.DocCount()
	assert.NoError(t, err)
	assert.Equal(t, 0, docCount)

	sizeInBytes, err := db.SizeInBytes()
	assert.NoError(t, err)
	assert.Equal(t, int64(0), sizeInBytes)

	// put first documents
	docs := []*JSONWithSize{
		doc("doc1", 200),
		doc("doc2", 300),
		doc("doc3", 400),
		doc("doc4", 600),
		doc("doc5", 500),
	}
	for _, d := range docs[:2] {
		fmt.Println("put", d.SizeInBytesTotal, db.Put(d))
	}
	time.Sleep(elasticUpdateTime)
	fmt.Println("put", docs[2].SizeInBytesTotal, db.Put(docs[2]))
	time.Sleep(elasticUpdateTime)

	docCount, err = db.DocCount()
	assert.NoError(t, err)
	assert.Equal(t, 3, docCount)

	// storage should be full => error on put
	err = db.Put(docs[3])
	assert.Error(t, err)

	err = db.Delete("doc2")
	assert.NoError(t, err)

	time.Sleep(elasticUpdateTime)

	docCount, err = db.DocCount()
	assert.NoError(t, err)
	assert.Equal(t, 2, docCount)

	err = db.Put(docs[4])
	assert.NoError(t, err)

	time.Sleep(elasticUpdateTime)

	docCount, err = db.DocCount()
	assert.NoError(t, err)
	assert.Equal(t, 3, docCount)

	val, ok := db.Get(docs[0].id)
	fmt.Println(val, ok)
	// TODO: deserialize and check content

	err = db.Delete(docs[0].id)
	assert.NoError(t, err)
	err = db.Delete(docs[3].id)
	assert.Error(t, err)

	time.Sleep(elasticUpdateTime)
	docCount, err = db.DocCount()
	assert.NoError(t, err)
	assert.Equal(t, 2, docCount)

	assert.Equal(t, smallSizeLimit, db.SizeInBytesLimit())
}

func doc(id string, size int64) *JSONWithSize {
	json := types.JSON{}
	json["id"] = id
	json["sizeInBytes"] = size
	json["timestamp"] = time.Now()
	return NewJSONWithSize(json, id, size)
}
