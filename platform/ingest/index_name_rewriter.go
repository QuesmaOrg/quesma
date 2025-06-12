// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package ingest

import (
	"fmt"
	"github.com/QuesmaOrg/quesma/platform/config"
	"github.com/QuesmaOrg/quesma/platform/logger"
	"regexp"
)

type IndexNameRewriter interface {
	RewriteIndex(indexName string) string
}

type NoOpIndexNameRewriter struct {
}

func (n *NoOpIndexNameRewriter) RewriteIndex(indexName string) string {
	// no-op rewriter, returns the index name as is
	return indexName
}

type rewriteRule struct {
	Pattern     *regexp.Regexp
	Replacement string
}

func (r *rewriteRule) String() string {
	return fmt.Sprintf("RewriteRule `%s` -> `%s`", r.Pattern.String(), r.Replacement)
}

type indexNameRegexpRewriter struct {
	rules []rewriteRule
}

func NewIndexNameRewriter(cfg *config.QuesmaConfiguration) IndexNameRewriter {

	if len(cfg.IndexNameRewriteRules) == 0 {
		logger.Debug().Msgf("No index name rewrite rules configured, using no-op rewriter")
		// if no rewrite rules are configured, return a no-op rewriter
		return &NoOpIndexNameRewriter{}
	}

	var rules []rewriteRule

	for _, rule := range cfg.IndexNameRewriteRules {
		if rule.From == "" || rule.To == "" {
			continue // skip invalid rules
		}
		pattern, err := regexp.Compile(rule.From)
		if err != nil {
			logger.Error().Msgf("Unable to compile regexp for index name rewrite: %s", rule.From)
			continue // skip invalid regex patterns
		}
		r := rewriteRule{
			Pattern:     pattern,
			Replacement: rule.To,
		}
		rules = append(rules, r)
		logger.Info().Msgf("Added index name rewrite rule: %s", r.String())
	}

	return &indexNameRegexpRewriter{rules: rules}
}

func (i *indexNameRegexpRewriter) RewriteIndex(indexName string) string {

	rewritten := indexName
	for _, rule := range i.rules {
		rewritten = rule.Pattern.ReplaceAllString(rewritten, rule.Replacement)
	}
	return rewritten
}
