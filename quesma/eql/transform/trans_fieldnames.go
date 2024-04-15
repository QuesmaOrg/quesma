package transform

import "fmt"

type FieldNameTransformer struct {
	Translate func(*Symbol) (*Symbol, error)
	Errors    []string
}

func (v *FieldNameTransformer) error(msg string, args ...interface{}) Exp {
	v.Errors = append(v.Errors, fmt.Sprintf(msg, args...))

	// this is paranoid
	// if some else ignores the error, we return expression that will throw an error
	return clickhouseRaiseError(msg, args...)
}

func (v *FieldNameTransformer) VisitConst(e *Const) interface{} {
	return e
}

func (v *FieldNameTransformer) VisitSymbol(e *Symbol) interface{} {

	if e == NULL {
		return e
	}

	if v.Translate == nil {
		return e
	}

	newSymbol, err := v.Translate(e)

	if err != nil {
		return v.error("error translating field name: %v", err)
	}

	return newSymbol
}

func (v *FieldNameTransformer) VisitGroup(e *Group) interface{} {
	return NewGroup(e.Inner.Accept(v).(Exp))
}

func (v *FieldNameTransformer) VisitInfixOp(e *InfixOp) interface{} {
	return NewInfixOp(e.Op, e.Left.Accept(v).(Exp), e.Right.Accept(v).(Exp))
}

func (v *FieldNameTransformer) visitChildren(c []Exp) []Exp {
	var result []Exp
	for _, child := range c {
		result = append(result, child.Accept(v).(Exp))
	}
	return result
}

func (v *FieldNameTransformer) VisitPrefixOp(e *PrefixOp) interface{} {
	return NewPrefixOp(e.Op, v.visitChildren(e.Args))
}

func (v *FieldNameTransformer) VisitFunction(e *Function) interface{} {
	return NewFunction(e.Name.Name, v.visitChildren(e.Args)...)
}

func (v *FieldNameTransformer) VisitArray(e *Array) interface{} {
	return NewArray(v.visitChildren(e.Values)...)
}
