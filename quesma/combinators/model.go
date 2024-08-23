package combinators

import "strings"

type AggregateFunction struct {
	Name string

	If          bool
	Array       bool
	Map         bool
	SimpleState bool
	State       bool
	Merge       bool
	MergeState  bool
	ForEach     bool
	Distinct    bool
	OrDefault   bool
	OrNull      bool
	Resample    bool
	ArgMin      bool
	ArgMax      bool
}

func NewFunction(name string) *AggregateFunction {
	return &AggregateFunction{Name: name}
}

// Method to enable/disable State combinator
func (f *AggregateFunction) SetState(enable bool) *AggregateFunction {
	f.State = enable
	return f
}

// Method to enable/disable SimpleState combinator
func (f *AggregateFunction) SetSimpleState(enable bool) *AggregateFunction {
	f.SimpleState = enable
	return f
}

// Method to enable/disable Merge combinator
func (f *AggregateFunction) SetMerge(enable bool) *AggregateFunction {
	f.Merge = enable
	return f
}

// Method to enable/disable If combinator
func (f *AggregateFunction) SetIf(enable bool) *AggregateFunction {
	f.If = enable
	return f
}

// Method to enable/disable Array combinator
func (f *AggregateFunction) SetArray(enable bool) *AggregateFunction {
	f.Array = enable
	return f
}

// Method to enable/disable Map combinator
func (f *AggregateFunction) SetMap(enable bool) *AggregateFunction {
	f.Map = enable
	return f
}

// Method to enable/disable ForEach combinator
func (f *AggregateFunction) SetForEach(enable bool) *AggregateFunction {
	f.ForEach = enable
	return f
}

// Method to enable/disable Distinct combinator
func (f *AggregateFunction) SetDistinct(enable bool) *AggregateFunction {
	f.Distinct = enable
	return f
}

// Method to enable/disable Resample combinator
func (f *AggregateFunction) SetResample(enable bool) *AggregateFunction {
	f.Resample = enable
	return f
}

// Method to enable/disable OrDefault combinator
func (f *AggregateFunction) SetOrDefault(enable bool) *AggregateFunction {
	f.OrDefault = enable
	return f
}

// Method to enable/disable OrNull combinator
func (f *AggregateFunction) SetOrNull(enable bool) *AggregateFunction {
	f.OrNull = enable
	return f
}

// Method to enable/disable ArgMin combinator
func (f *AggregateFunction) SetArgMin(enable bool) *AggregateFunction {
	f.ArgMin = enable
	return f
}

// Method to enable/disable ArgMax combinator
func (f *AggregateFunction) SetArgMax(enable bool) *AggregateFunction {
	f.ArgMax = enable
	return f
}

func (f *AggregateFunction) String() string {
	var builder strings.Builder

	builder.WriteString(f.Name)

	if f.State {
		builder.WriteString("State")
	}
	if f.SimpleState {
		builder.WriteString("SimpleState")
	}
	if f.Merge {
		builder.WriteString("Merge")
	}
	if f.Array {
		builder.WriteString("Array")
	}
	if f.Map {
		builder.WriteString("Map")
	}
	if f.ForEach {
		builder.WriteString("ForEach")
	}
	if f.Distinct {
		builder.WriteString("Distinct")
	}
	if f.Resample {
		builder.WriteString("Resample")
	}
	if f.OrDefault {
		builder.WriteString("OrDefault")
	}
	if f.OrNull {
		builder.WriteString("OrNull")
	}
	if f.ArgMin {
		builder.WriteString("ArgMin")
	}
	if f.ArgMax {
		builder.WriteString("ArgMax")
	}
	if f.If {
		builder.WriteString("If")
	}

	return builder.String()
}

func (f *AggregateFunction) Clone() *AggregateFunction {
	return &AggregateFunction{
		Name:        f.Name,
		State:       f.State,
		SimpleState: f.SimpleState,
		Merge:       f.Merge,
		If:          f.If,
		Array:       f.Array,
		Map:         f.Map,
		ForEach:     f.ForEach,
		Distinct:    f.Distinct,
		Resample:    f.Resample,
		OrDefault:   f.OrDefault,
		OrNull:      f.OrNull,
		ArgMin:      f.ArgMin,
		ArgMax:      f.ArgMax,
	}
}

func ParseAggregateFunction(name string) *AggregateFunction {
	f := AggregateFunction{}

	// Order matters, so we check combinators in the correct sequence
	if strings.Contains(name, "State") && !strings.Contains(name, "SimpleState") {
		f.State = true
		name = strings.Replace(name, "State", "", 1)
	}
	if strings.Contains(name, "SimpleState") {
		f.SimpleState = true
		name = strings.Replace(name, "SimpleState", "", 1)
	}
	if strings.Contains(name, "Merge") && !strings.Contains(name, "MergeState") {
		f.Merge = true
		name = strings.Replace(name, "Merge", "", 1)
	}
	if strings.Contains(name, "If") {
		f.If = true
		name = strings.Replace(name, "If", "", 1)
	}
	if strings.Contains(name, "Array") {
		f.Array = true
		name = strings.Replace(name, "Array", "", 1)
	}
	if strings.Contains(name, "Map") {
		f.Map = true
		name = strings.Replace(name, "Map", "", 1)
	}
	if strings.Contains(name, "ForEach") {
		f.ForEach = true
		name = strings.Replace(name, "ForEach", "", 1)
	}
	if strings.Contains(name, "Distinct") {
		f.Distinct = true
		name = strings.Replace(name, "Distinct", "", 1)
	}
	if strings.Contains(name, "Resample") {
		f.Resample = true
		name = strings.Replace(name, "Resample", "", 1)
	}
	if strings.Contains(name, "OrDefault") {
		f.OrDefault = true
		name = strings.Replace(name, "OrDefault", "", 1)
	}
	if strings.Contains(name, "OrNull") {
		f.OrNull = true
		name = strings.Replace(name, "OrNull", "", 1)
	}
	if strings.Contains(name, "ArgMin") {
		f.ArgMin = true
		name = strings.Replace(name, "ArgMin", "", 1)
	}
	if strings.Contains(name, "ArgMax") {
		f.ArgMax = true
		name = strings.Replace(name, "ArgMax", "", 1)
	}

	f.Name = name

	return &f
}
