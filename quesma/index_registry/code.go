// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package index_registry

import (
	"fmt"
	"strings"
)

func (d *Decision) String() string {

	var lines []string

	if d.IsClosed {
		lines = append(lines, "Returns a closed index message.")
	}

	if d.IsEmpty {
		lines = append(lines, "Returns an empty result.")
	}

	if d.Err != nil {
		lines = append(lines, fmt.Sprintf("Returns error: '%v'.", d.Err))
	}

	for _, connector := range d.UseConnectors {
		lines = append(lines, connector.Message())
	}

	lines = append(lines, fmt.Sprintf("%s (%s).", d.Message, d.ResolverName))

	return strings.Join(lines, " ")
}

// ---

type indexPattern struct {
	pattern   string
	isPattern bool
	patterns  []string
}

type namedResolver struct {
	name     string
	resolver func(pattern indexPattern) *Decision
}

type composedIndexResolver struct {
	decisionLadder []namedResolver
}

func (ir *composedIndexResolver) Resolve(indexName string) *Decision {

	patterns := strings.Split(indexName, ",")

	input := indexPattern{
		pattern:   indexName,
		isPattern: len(patterns) > 1 || strings.Contains(indexName, "*"),
		patterns:  patterns,
	}

	for _, resolver := range ir.decisionLadder {
		decision := resolver.resolver(input)

		if decision != nil {
			decision.ResolverName = resolver.name
			return decision
		}
	}
	return &Decision{
		Message: "Could not resolve pattern. This is a bug.",
		Err:     fmt.Errorf("could not resolve index"), // TODO better error
	}
}
