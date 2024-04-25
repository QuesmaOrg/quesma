package clickhouse

func genericString(name string) *Column {
	return &Column{
		Name: name,
		Type: BaseType{
			Name:   "String",
			goType: NewBaseType("String").goType,
		},
		Modifiers: "CODEC(ZSTD(1))",
	}
}

func lowCardinalityString(name string) *Column {
	return &Column{
		Name: name,
		Type: BaseType{
			Name:   "LowCardinality(String)",
			goType: NewBaseType("LowCardinality(String)").goType,
		},
	}
}

func dateTime(name string) *Column {
	return &Column{
		Name: name,
		Type: BaseType{
			Name:   "DateTime64",
			goType: NewBaseType("DateTime64").goType,
		},
		Modifiers: "CODEC(DoubleDelta, LZ4)",
	}
}
