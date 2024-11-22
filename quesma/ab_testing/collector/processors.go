// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package collector

import (
	"github.com/goccy/go-json"
	"fmt"
	"quesma/quesma/types"
	"regexp"
)

// unifySyncAsyncResponse is a processor that processes that removes async "wrapper" from the response
type unifySyncAsyncResponse struct {
}

func (t *unifySyncAsyncResponse) name() string {
	return "unifySyncAsyncResponse"
}

func (t *unifySyncAsyncResponse) process(in EnrichedResults) (out EnrichedResults, drop bool, err error) {

	deAsync := func(elasticResponse string) (string, error) {

		asJson, err := types.ParseJSON(elasticResponse)

		if err != nil {
			return "", err
		}

		if res, ok := asJson["response"]; ok {
			b, err := json.Marshal(res)

			if err != nil {
				return "", err
			}

			return string(b), nil
		}

		return elasticResponse, nil
	}

	respA, err := deAsync(in.A.Body)
	if err != nil {
		err := fmt.Errorf("failed to unify A response: %w", err)
		in.Errors = append(in.Errors, err.Error())
		return in, false, nil
	}

	respB, err := deAsync(in.B.Body)
	if err != nil {
		err := fmt.Errorf("failed to unify B response: %w", err)
		in.Errors = append(in.Errors, err.Error())
		return in, false, nil
	}

	in.A.Body = respA
	in.B.Body = respB

	return in, false, nil
}

type extractKibanaIds struct {
}

func (t *extractKibanaIds) name() string {
	return "extractKibanaIds"
}

var opaqueIdKibanaDashboardIdRegexp = regexp.MustCompile(`dashboards:([0-9a-f-]+)`)
var opaqueIdKibanaPanelIdRegexp = regexp.MustCompile(`dashboard:dashboards:.*;.*:.*:([0-9a-f-]+)`)

func (t *extractKibanaIds) process(in EnrichedResults) (out EnrichedResults, drop bool, err error) {

	opaqueId := in.OpaqueID

	in.KibanaDashboardId = "n/a"
	in.KibanaDashboardPanelId = "n/a"

	if opaqueId == "" {
		return in, false, nil
	}

	matches := opaqueIdKibanaDashboardIdRegexp.FindStringSubmatch(opaqueId)

	if len(matches) < 2 {
		return in, false, nil
	}

	in.KibanaDashboardId = matches[1]

	panelsMatches := opaqueIdKibanaPanelIdRegexp.FindStringSubmatch(opaqueId)
	if len(panelsMatches) < 2 {
		return in, false, nil
	}
	in.KibanaDashboardPanelId = panelsMatches[1]

	return in, false, nil
}
