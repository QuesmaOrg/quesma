package where_clause

import (
	"mitmproxy/quesma/logger"
	"mitmproxy/quesma/quesma/config"
	"sort"
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
	// Create a slice of the map keys (alias names)
	// that's to ensure deterministic order when iterating over alias
	//     (first alias with matching source field name will be used)
	aliasesNames := make([]string, 0, len(v.IndexCfg.Aliases))
	for k := range v.IndexCfg.Aliases {
		aliasesNames = append(aliasesNames, k)
	}
	sort.Strings(aliasesNames)

	for _, key := range aliasesNames {
		alias := v.IndexCfg.Aliases[key]
		if fieldName == alias.SourceFieldName {
			logger.Debug().Msgf("Resolving field alias [Config: target=%s,source=%s], swapping [%s] with [%s]", alias.TargetFieldName, alias.SourceFieldName, fieldName, alias.TargetFieldName)
			return alias.TargetFieldName
		}
	}
	return fieldName
}
