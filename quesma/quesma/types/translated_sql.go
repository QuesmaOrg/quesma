package types

type TranslatedSQLQuery struct {
	Query []byte

	AppliedOptimizations []string
}
