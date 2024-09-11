package persistance

import "quesma/quesma/types"

type Persistance interface {
	List() ([]string, error)
	Get(string) (types.JSON, error)
	Put(string, types.JSON) error
}
