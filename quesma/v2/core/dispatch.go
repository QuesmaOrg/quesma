// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package quesma_api

import (
	"context"
	"net/http"
)

type HTTPFrontendHandler func(ctx context.Context, req *Request, writer http.ResponseWriter) (*Result, error)

type HandlersPipe struct {
	Path       string
	Predicate  RequestMatcher
	Handler    HTTPFrontendHandler
	Processors []Processor
}

type Dispatcher struct {
}

func (d *Dispatcher) Dispatch(processors []Processor, metadata map[string]interface{}, message any) (map[string]interface{}, any) {
	return d.dispatch(processors, metadata, message)
}

func (d *Dispatcher) dispatch(processors []Processor, metadata map[string]interface{}, message any) (map[string]any, any) {
	// Process the response using the processor
	var inputMessages []any
	inputMessages = append(inputMessages, message)
	if processors == nil {
		return metadata, inputMessages[0]
	}
	var outMessage any
	for _, processor := range processors {
		metadata, outerMessage, _ := processor.Handle(metadata, inputMessages...)
		outMessage = outerMessage
		inputMessages = make([]any, 0)
		innerProcessors := processor.GetProcessors()
		for _, innerProc := range innerProcessors {
			// TODO inner processor can have its own processors
			metadata, outMessage, _ = innerProc.Handle(metadata, outerMessage)
			inputMessages = append(inputMessages, outMessage)
		}
	}
	return metadata, outMessage
}
