package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
)

type RestClient struct {
}

func (h *RestClient) Query(query JSON) ([]JSON, error) {

	url := query["url"].(string)

	body := query["body"]

	b, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}

	resp, err := http.Post(url, "application/json", bytes.NewReader(b))
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()

	b, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var doc JSON

	err = json.Unmarshal(b, &doc)
	if err != nil {
		return nil, err
	}

	return []JSON{doc}, nil
}

//

type RestServer struct {
	mux    *http.ServeMux
	Source DatabaseLet
}

func (h *RestServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {

	defer r.Body.Close()
	b, err := ioutil.ReadAll(r.Body)

	internalError := func(err error) {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(err.Error()))
	}

	if err != nil {
		internalError(err)
		return
	}

	query := make(JSON)

	query["method"] = r.Method
	query["path"] = r.URL.Path

	body := make(JSON)
	err = json.Unmarshal(b, &body)

	if err != nil {
		internalError(err)
		return
	}
	query["body"] = body

	if h.Source == nil {
		internalError(fmt.Errorf("no source"))
		return
	}

	docs, err := h.Source.Query(query)
	if err != nil {
		internalError(err)
		return
	}

	for _, doc := range docs {

		out, err := json.MarshalIndent(doc, "", " ")
		if err != nil {
			internalError(err)
			return
		}

		w.Write([]byte(out))
	}
}

func (h *RestServer) ListenAndServe(addr string) error {

	h.mux = http.NewServeMux()
	h.mux.Handle("/", h)
	go http.ListenAndServe(addr, h.mux)
	return nil
}

//

