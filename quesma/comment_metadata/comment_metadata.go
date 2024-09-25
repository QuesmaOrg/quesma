// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package comment_metadata

import (
	"fmt"
	"net/url"
	"regexp"
)

const ElasticFieldName = "fieldName"

const commentMetadataVersion = "1"
const metadataPrefix = "quesmaMetadata:"
const versionParameter = "v"

type CommentMetadata struct {
	Values map[string]string
}

func NewCommentMetadata() *CommentMetadata {
	return &CommentMetadata{
		Values: make(map[string]string),
	}
}

func (c *CommentMetadata) Marshall() string {

	params := url.Values{}
	for k, v := range c.Values {
		params.Add(k, v)
	}
	params.Add(versionParameter, commentMetadataVersion)

	return metadataPrefix + params.Encode()
}

func UnmarshallCommentMetadata(s string) (*CommentMetadata, error) {

	rx := regexp.MustCompile(metadataPrefix + `([^\s]+)`)

	groups := rx.FindStringSubmatch(s)

	if len(groups) == 0 {
		return nil, fmt.Errorf("quesma metadata not found")
	}

	if len(groups) != 2 {
		return nil, fmt.Errorf("invalid metadata format")
	}

	s = groups[1]

	params, err := url.ParseQuery(s)
	if err != nil {
		return nil, err
	}

	version := params.Get(versionParameter)

	if version != commentMetadataVersion {
		return nil, fmt.Errorf("invalid metadata version: %s", version)
	}

	values := make(map[string]string)
	for k, v := range params {
		if k == versionParameter {
			continue
		}
		values[k] = v[0]
	}

	return &CommentMetadata{
		Values: values,
	}, nil
}
