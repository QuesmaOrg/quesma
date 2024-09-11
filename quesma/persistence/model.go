package persistence

import "quesma/quesma/types"

type JSONDatabase interface {
	List() (keys []string, err error)
	Get(key string) (types.JSON, error)
	Put(key string, data types.JSON) error
}
