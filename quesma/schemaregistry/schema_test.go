package schemaregistry

import (
	"fmt"
	"testing"
)

func TestExample(t *testing.T) {
	schema := Schema{Fields: map[FieldName]Field{
		"timestamp": {Name: "timestamp", Type: TypeTimestamp},
		"some.text": {Name: "some.text", Type: TypeText},
		"some.long": {Name: "some.long", Type: TypeLong},
		"some.date": {Name: "some.date", Type: TypeDate},
	}}

	fmt.Printf("schema: %+v\n", schema)
}
