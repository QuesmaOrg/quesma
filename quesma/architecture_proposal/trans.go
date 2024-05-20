package main

type Transformer interface {
	Transform(document JSON) JSON
}

type TransformerFunc func(JSON) JSON

func (f TransformerFunc) Transform(document JSON) JSON {
	return f(document)
}

// Transforms query document before passing it to the source

type QueryTransformer struct {
	Transformer Transformer
	Source      DatabaseLet
}

func (i *QueryTransformer) Query(query JSON) ([]JSON, error) {
	query = i.Transformer.Transform(query)
	return i.Source.Query(query)
}

// Transforms documents after they are returned from the source

type ResultsTransformer struct {
	Transformer Transformer
	Source      DatabaseLet
}

func (t *ResultsTransformer) Query(query JSON) ([]JSON, error) {
	query = t.Transformer.Transform(query)
	docs, err := t.Source.Query(query)
	if err != nil {
		return nil, err
	}

	for i, _ := range docs {
		docs[i] = t.Transformer.Transform(docs[i])
	}

	return docs, nil
}

// ------------------- []JSON -> JSON

type Reducer interface {
	Reduce([]JSON) JSON
}

type ReducerFunc func([]JSON) JSON

func (r ReducerFunc) Reduce(docs []JSON) JSON {
	return r(docs)
}

type ResultsReducer struct {
	Reducer Reducer
	Source  DatabaseLet
}

func (r *ResultsReducer) Query(query JSON) ([]JSON, error) {
	docs, err := r.Source.Query(query)
	if err != nil {
		return nil, err
	}
	return []JSON{r.Reducer.Reduce(docs)}, nil
}

// Exploder JSON -> []JSON

type Exploder interface {
	Explode(JSON) []JSON
}

type ExploderFunc func(JSON) []JSON

func (e ExploderFunc) Explode(doc JSON) []JSON {
	return e(doc)
}

type ResultsExploder struct {
	Exploder Exploder
	Source   DatabaseLet
}

func (e *ResultsExploder) Query(query JSON) ([]JSON, error) {
	docs, err := e.Source.Query(query)
	if err != nil {
		return nil, err
	}

	var out []JSON

	for _, doc := range docs {
		out = append(out, e.Exploder.Explode(doc)...)
	}

	return out, nil
}
