package quesma

import (
	"context"
	"fmt"
	"github.com/stretchr/testify/assert"
	"mitmproxy/quesma/model"
	"mitmproxy/quesma/queryparser/aexp"
	"mitmproxy/quesma/queryparser/where_clause"
	"mitmproxy/quesma/quesma/config"
	"testing"
)

func Test_ipRangeTransform(t *testing.T) {
	const isIPAddressInRangePrimitive = "isIPAddressInRange"
	const CASTPrimitive = "CAST"
	const StringLiteral = "'String'"
	const IpFieldContent = "111.42.223.209/16"
	const IpFieldName = "clientip"

	indexConfig := map[string]config.IndexConfiguration{
		"kibana_sample_data_logs": {
			Name:           "kibana_sample_data_logs",
			Enabled:        true,
			FullTextFields: []string{"message", "content"},
			TypeMappings:   map[string]string{IpFieldName: "ip"},
		},
	}
	transform := &SchemaCheckPass{cfg: indexConfig}

	expectedQuery := model.Query{
		FromClause: "kibana_sample_data_logs",
		Columns: []model.SelectColumn{{
			Expression: aexp.Wildcard,
		},
		},
		WhereClause: &where_clause.Function{
			Name: where_clause.Literal{Name: isIPAddressInRangePrimitive},
			Args: []where_clause.Statement{
				&where_clause.Function{
					Name: where_clause.Literal{Name: CASTPrimitive},
					Args: []where_clause.Statement{
						&where_clause.Literal{Name: IpFieldName},
						&where_clause.Literal{Name: StringLiteral},
					},
				},
				&where_clause.Literal{Name: IpFieldContent},
			},
		},
	}
	queries := []model.Query{
		{FromClause: "kibana_sample_data_logs",
			Columns: []model.SelectColumn{{
				Expression: aexp.Wildcard,
			},
			},
			WhereClause: &where_clause.InfixOp{
				Left:  &where_clause.Literal{Name: IpFieldName},
				Op:    "=",
				Right: &where_clause.Literal{Name: IpFieldContent},
			},
		},
	}

	fmt.Println(queries[0].String(context.Background()))
	resultQuery, err := transform.Transform(queries)
	assert.NoError(t, err)
	assert.Equal(t, expectedQuery.String(context.Background()), resultQuery[0].String(context.Background()))
}
