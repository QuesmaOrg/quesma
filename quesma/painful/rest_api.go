// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package painful

import (
	"quesma/quesma/types"
)

type ScriptRequest struct {
	Context string `json:"context"`
	Script  struct {
		Source string `json:"source"`
	} `json:"script"`

	ContextSetup struct {
		Document  types.JSON `json:"document"`
		IndexName string     `json:"index_name"`
	} `json:"context_setup"`
}

type ScriptResponse struct {
	Result []any `json:"result"`
}

func (s ScriptRequest) Eval() (res ScriptResponse, err error) {
	env := &Env{
		Doc: s.ContextSetup.Document,
	}

	evalTree, err := ParsePainless(s.Script.Source)
	if err != nil {
		return res, err
	}

	_, err = evalTree.Eval(env)
	if err != nil {
		return res, err
	}

	res.Result = []any{env.EmitValue}

	return res, nil

}
