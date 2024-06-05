package where_clause

import (
	"mitmproxy/quesma/logger"
	"mitmproxy/quesma/quesma/config"
)

// AliasResolver is a visitor that takes all the columns referenced in a given where clause
// and changes their names according to alias configuration
type AliasResolver struct {

	//Deprecated
	IndexCfg config.IndexConfiguration // we should use the new schema config (*config.SchemaConfiguration) which is not ready yet
}

func (v *AliasResolver) VisitLiteral(e *Literal) interface{} {
	return nil
}

func (v *AliasResolver) VisitInfixOp(e *InfixOp) interface{} {
	e.Left.Accept(v)
	e.Right.Accept(v)
	return nil
}

func (v *AliasResolver) VisitPrefixOp(e *PrefixOp) interface{} {
	for _, arg := range e.Args {
		arg.Accept(v)
	}
	return nil
}

func (v *AliasResolver) VisitFunction(e *Function) interface{} {
	for _, arg := range e.Args {
		arg.Accept(v)
	}
	return nil
}

func (v *AliasResolver) VisitColumnRef(e *ColumnRef) interface{} {
	e.ColumnName = v.resolveFieldName(e.ColumnName)
	return nil
}

func (v *AliasResolver) VisitNestedProperty(e *NestedProperty) interface{} {
	return nil
}

func (v *AliasResolver) VisitArrayAccess(e *ArrayAccess) interface{} {
	return nil
}

// resolveFieldName takes a field name and returns the corresponding alias based on the configuration
func (v *AliasResolver) resolveFieldName(fieldName string) string {
	for _, alias := range v.IndexCfg.Aliases {
		if alias.SourceFieldName == fieldName {
			logger.Debug().Msgf("Resolving field alias [Config: target=%s,source=%s], swapping [%s] with [%s]", alias.TargetFieldName, alias.SourceFieldName, fieldName, alias.TargetFieldName)
			return alias.TargetFieldName
		}
	}
	return fieldName
}
