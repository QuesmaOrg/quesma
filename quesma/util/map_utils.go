package util

func MapKeys[K comparable, V any](m map[K]V) []K {
	keys := make([]K, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

func MapValues[K comparable, V any](m map[K]V) []V {
	keys := make([]V, 0, len(m))
	for _, v := range m {
		keys = append(keys, v)
	}
	return keys
}
