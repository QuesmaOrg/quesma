package schema

type TypeAdapter interface {
	Adapt(string) (Type, bool)
}
