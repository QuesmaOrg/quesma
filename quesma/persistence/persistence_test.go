// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package persistence

import (
	"fmt"
	"github.com/QuesmaOrg/quesma/quesma/quesma/config"
	"github.com/QuesmaOrg/quesma/quesma/quesma/types"
	"net/url"
	"testing"
	"time"
)

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
