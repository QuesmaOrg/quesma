package transform

import (
	"fmt"
)

type ParametersExtractorTransformer struct {
	counter    int
	Parameters map[string]interface{}
}

func NewParametersExtractorTransformer() *ParametersExtractorTransformer {
	return &ParametersExtractorTransformer{
		Parameters: make(map[string]interface{}),
	}
}

func (v *ParametersExtractorTransformer) VisitConst(e *Const) interface{} {

	if e == TRUE || e == FALSE {
		return e
	}

	v.counter++

	paramName := fmt.Sprintf("P_%d", v.counter)
	v.Parameters[paramName] = fmt.Sprintf("%v", e.Value)

	var typeName string
	switch e.Value.(type) {

	case int:
		typeName = "Int64"
	case string:
		typeName = "String"
	case bool:
		typeName = "Boolean"
	default:
		typeName = "String"
	}

	return NewSymbol(fmt.Sprintf("{%s:%s}", paramName, typeName))
}

func (v *ParametersExtractorTransformer) VisitSymbol(e *Symbol) interface{} {
	return e
}

func (v *ParametersExtractorTransformer) VisitGroup(e *Group) interface{} {
	return NewGroup(e.Inner.Accept(v).(Exp))
}

func (v *ParametersExtractorTransformer) VisitInfixOp(e *InfixOp) interface{} {
	return NewInfixOp(e.Op, e.Left.Accept(v).(Exp), e.Right.Accept(v).(Exp))
}

func (v *ParametersExtractorTransformer) visitChildren(c []Exp) []Exp {
	var result []Exp
	for _, child := range c {
		result = append(result, child.Accept(v).(Exp))
	}
	return result
}

func (v *ParametersExtractorTransformer) VisitPrefixOp(e *PrefixOp) interface{} {
	return NewPrefixOp(e.Op, v.visitChildren(e.Args))
}

func (v *ParametersExtractorTransformer) VisitFunction(e *Function) interface{} {
	return NewFunction(e.Name.Name, v.visitChildren(e.Args)...)
}

func (v *ParametersExtractorTransformer) VisitArray(e *Array) interface{} {
	return NewArray(v.visitChildren(e.Values)...)
}
