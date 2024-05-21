package mux

import (
	"context"
	"mitmproxy/quesma/logger"
)

type RequestProcessor interface {
	Applies(req *Request) bool
	PreprocessRequest(req *Request) *Request
	ProcessRequest(req *Request) (*Result, error)
	IsFinal() bool
}

func AsHandler(rp RequestProcessor) Handler {
	return func(ctx context.Context, req *Request) (*Result, error) {
		return rp.ProcessRequest(req)
	}
}

func NewFieldDroppingProcessor(fields ...string) RequestProcessor {
	return FieldDroppingProcessor{Fields: fields}
}

func NewIdentityRequestProcessor() RequestProcessor {
	return IdentityRequestProcessor{}
}

type (
	IdentityRequestProcessor struct {
	}
	FieldDroppingProcessor struct {
		Fields []string
	}
)

func (f FieldDroppingProcessor) Applies(*Request) bool {
	return true
}

func (f FieldDroppingProcessor) PreprocessRequest(req *Request) *Request {
	switch body := req.ParsedBody.(type) {
	case JSON:
		logger.Info().Msgf("Dropping fields %v", f.Fields)
		for _, field := range f.Fields {
			delete(body, field)
		}
	//
	case NDJSON:
		logger.Info().Msgf("Dropping fields %v", f.Fields)
		for _, json := range body {
			for _, field := range f.Fields {
				delete(json, field)
			}
		}
	//
	case Unknown:
		// ignore
	default:
		logger.Info().Msgf("Unknown body type %T, ignoring", body)
	}
	return req
}

func (f FieldDroppingProcessor) ProcessRequest(*Request) (*Result, error) {
	return nil, nil
}

func (f FieldDroppingProcessor) IsFinal() bool {
	return false
}

func (i IdentityRequestProcessor) Applies(req *Request) bool { return req.Method == "GET" }

func (i IdentityRequestProcessor) PreprocessRequest(req *Request) *Request { return req }

func (i IdentityRequestProcessor) ProcessRequest(req *Request) (*Result, error) { return nil, nil }

func (i IdentityRequestProcessor) IsFinal() bool { return false }

var _ RequestProcessor = IdentityRequestProcessor{}
var _ RequestProcessor = FieldDroppingProcessor{}
