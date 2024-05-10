package transform

import (
	"fmt"
	"strings"
)

type Renderer struct {
	// TODO add pretty print options
}

func (v *Renderer) VisitConst(e *Const) interface{} {
	switch val := e.Value.(type) {

	case string:

		val = strings.ReplaceAll(val, `\`, `\\`)
		val = strings.ReplaceAll(val, "'", `\'`)
		val = strings.ReplaceAll(val, "\n", `\n`)
		val = strings.ReplaceAll(val, "\t", `\t`)
		val = strings.ReplaceAll(val, "\r", `\r`)

		return fmt.Sprintf("'%v'", val)
	}

	return fmt.Sprintf("%v", e.Value)
}

func (v *Renderer) VisitSymbol(e *Symbol) interface{} {
	return e.Name
}

func (v *Renderer) VisitGroup(e *Group) interface{} {
	return fmt.Sprintf("(%v)", e.Inner.Accept(v))
}

func (v *Renderer) VisitInfixOp(e *InfixOp) interface{} {
	return fmt.Sprintf("(%v %v %v)", e.Left.Accept(v), e.Op, e.Right.Accept(v))
}

func (v *Renderer) VisitPrefixOp(e *PrefixOp) interface{} {
	args := make([]string, len(e.Args))
	for i, arg := range e.Args {
		args[i] = arg.Accept(v).(string)
	}

	argsAsString := strings.Join(args, ", ")
	return fmt.Sprintf("(%v %v)", e.Op, argsAsString)
}

func (v *Renderer) VisitFunction(e *Function) interface{} {
	args := make([]string, len(e.Args))
	for i, arg := range e.Args {
		args[i] = arg.Accept(v).(string)
	}

	argsAsString := strings.Join(args, ", ")
	return fmt.Sprintf("%v(%v)", e.Name.Accept(v), argsAsString)
}

func (v *Renderer) VisitArray(e *Array) interface{} {
	args := make([]string, len(e.Values))
	for i, arg := range e.Values {
		args[i] = arg.Accept(v).(string)
	}

	argsAsString := strings.Join(args, ", ")
	return fmt.Sprintf("(%s)", argsAsString)
}
