package repository

type diffTransformer struct {
}

func (t *diffTransformer) process(in Data) (out Data, drop bool, err error) {

	if in.A.Body != in.B.Body {
		in.Diff.BodyDiff = "Compute diff here"
		in.Diff.IsDiff = true
	}
	return in, false, nil
}
