package repository

import "math/rand"

type probabilisticSampler struct {
	ratio float64
}

func (t *probabilisticSampler) process(in Data) (out Data, drop bool, err error) {

	if rand.Float64() > t.ratio {
		return in, true, nil
	}

	return in, false, nil
}
