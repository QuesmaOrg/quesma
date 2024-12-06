// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package comment_metadata

import (
	"fmt"
	"net/url"
	"regexp"
)

const ElasticFieldName = "fieldName"
const CreatedAt = "createdAt"
const CreatedBy = "createdBy"

const metadataVersion = "1"
const metadataPrefix = "quesmaMetadata"

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

	return metadataPrefix + "V" + metadataVersion + ":" + params.Encode()
}

func UnmarshallCommentMetadata(s string) (*CommentMetadata, error) {

	rx := regexp.MustCompile(metadataPrefix + `V([0-9+]):([^\s]+)`)

	groups := rx.FindStringSubmatch(s)

	if len(groups) == 0 {
		return nil, nil // no metadata, we return nil here, that's not an error
	}

	if len(groups) != 3 {
		return nil, fmt.Errorf("invalid metadata format")
	}

	version := groups[1]
	metadata := groups[2]

	params, err := url.ParseQuery(metadata)
	if err != nil {
		return nil, err
	}

	if version != metadataVersion {
		return nil, fmt.Errorf("invalid metadata version: %s", version)
	}

	values := make(map[string]string)
	for k, v := range params {
		values[k] = v[0]
	}

	return &CommentMetadata{
		Values: values,
	}, nil
}
