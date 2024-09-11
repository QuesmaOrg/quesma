package persistence

import (
	"fmt"
	"quesma/quesma/types"
	"testing"
	"time"
)

func TestNewElasticPersistence(t *testing.T) {

	var p JSONDatabase

	if true {
		p = NewStaticJSONDatabase()
	} else {
		indexName := fmt.Sprintf("quesma_test_%d", time.Now().UnixMicro())
		p = NewElasticJSONDatabase("http://localhost:9200", indexName)
	}

	m1 := make(types.JSON)
	m1["foo"] = "bar"

	d1, err := p.Get("t1")

	if err != nil {
		t.Fatal(err)
	}

	if d1 != nil {
		t.Fatal("expected nil")
	}

	err = p.Put("t1", m1)
	if err != nil {
		t.Fatal(err)
	}

	d2, err := p.Get("t1")
	if err != nil {
		t.Fatal(err)
	}
	if d2["foo"] != "bar" {
		t.Fatal("expected bar")
	}

}
