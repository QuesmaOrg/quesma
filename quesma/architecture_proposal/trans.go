package main

type Transformer interface {
	Transform(document Document) Document
}

type TransformerFunc func(Document) Document

func (f TransformerFunc) Transform(document Document) Document {
	return f(document)
}

// Transforms query document before passing it to the source

type QueryTransformer struct {
	Transformer Transformer
	Source      DatabaseLet
}

func (i *QueryTransformer) Query(query Document) ([]Document, error) {
	query = i.Transformer.Transform(query)
	return i.Source.Query(query)
}

// Transforms documents after they are returned from the source

type DocumentsTransformer struct {
	Transformer Transformer
	Source      DatabaseLet
}

func (t *DocumentsTransformer) Query(query Document) ([]Document, error) {
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

// ------------------- []Document -> Document

type Reducer interface {
	Reduce([]Document) Document
}

type ReducerFunc func([]Document) Document

func (r ReducerFunc) Reduce(docs []Document) Document {
	return r(docs)
}

type DocumentReducer struct {
	Reducer Reducer
	Source  DatabaseLet
}

func (r *DocumentReducer) Query(query Document) ([]Document, error) {
	docs, err := r.Source.Query(query)
	if err != nil {
		return nil, err
	}
	return []Document{r.Reducer.Reduce(docs)}, nil
}

// Exploder Document -> []Document

type Exploder interface {
	Explode(Document) []Document
}

type ExploderFunc func(Document) []Document

func (e ExploderFunc) Explode(doc Document) []Document {
	return e(doc)
}

type DocumentExploder struct {
	Exploder Exploder
	Source   DatabaseLet
}

func (e *DocumentExploder) Query(query Document) ([]Document, error) {
	docs, err := e.Source.Query(query)
	if err != nil {
		return nil, err
	}

	var out []Document

	for _, doc := range docs {
		out = append(out, e.Exploder.Explode(doc)...)
	}

	return out, nil
}
