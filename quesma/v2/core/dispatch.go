// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package quesma_api

import (
	"context"
	"net/http"
)

// TODO currently there are two types of handlers, HTTPFrontendHandler and Handler
// first one comes from v2 POC and the second one comes from v1 quesma
// we need to unify them
type HTTPFrontendHandler func(request *http.Request) (map[string]interface{}, any, error)
type Handler func(ctx context.Context, req *Request) (*Result, error)

type HandlersPipe struct {
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
