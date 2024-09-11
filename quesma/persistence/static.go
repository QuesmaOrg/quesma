package persistence

import (
	"quesma/quesma/types"
	"sync"
)

type StaticJSONDatabase struct {
	m sync.Mutex
	data map[string]types.JSON
}


func NewStaticJSONDatabase() *StaticJSONDatabase {
	return &StaticJSONDatabase{
		data: make(map[string]types.JSON),
	}
}

func (db *StaticJSONDatabase) List() ([]string, error) {
	db.m.Lock()
	defer db.m.Unlock()

	keys := make([]string, 0, len(db.data))
	for k := range db.data {
		keys = append(keys, k)
	}

	return keys, nil
}

func (db *StaticJSONDatabase) Get(key string) (types.JSON, error) {
	db.m.Lock()
	defer db.m.Unlock()

	if val, ok := db.data[key]; ok {
		return val, nil
	}

	return nil, nil
}

func (db *StaticJSONDatabase) Put(key string, val types.JSON) error {
	db.m.Lock()
	defer db.m.Unlock()

	db.data[key] = val
	return nil
}
