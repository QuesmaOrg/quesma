package main

import (
	"fmt"
	"log"
	"time"
)

type StdOutIngest struct{}

func (c *StdOutIngest) Query(query JSON) ([]JSON, error) {
	fmt.Println(query.String())
	return emptyList, nil
}

func makeLogger() DatabaseLet {

	console := &StdOutIngest{}

	trans := &QueryTransformer{Transformer: TransformerFunc(func(in JSON) JSON {
		in["@timestamp"] = time.Now().Format(time.RFC3339)
		return in

	})}

	trans.Source = console

	return trans
}

//

func Print(m string, a ...any) {

	if logger == nil {
		return
	}

	doc := make(JSON)

	doc["message"] = fmt.Sprintf(m, a...)

	_, err := logger.Query(doc)
	if err != nil {
		log.Println(err)
	}
}
