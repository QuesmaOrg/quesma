package main

import (
	"fmt"
	"log"
	"time"
)

type StdOutIngest struct{}

func (c *StdOutIngest) Query(document Document) ([]Document, error) {
	fmt.Println(document.String())
	return emptyDocuments, nil
}

func makeLogger() DatabaseLet {

	console := &StdOutIngest{}

	trans := &QueryTransformer{Transformer: TransformerFunc(func(in Document) Document {
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

	doc := make(Document)

	doc["message"] = fmt.Sprintf(m, a...)

	_, err := logger.Query(doc)
	if err != nil {
		log.Println(err)
	}
}
