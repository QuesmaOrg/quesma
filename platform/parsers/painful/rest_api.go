// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package painful

import (
	"github.com/QuesmaOrg/quesma/platform/v2/core/types"
	"net/http"
)

type ScriptRequest struct {
	Context string `json:"context"`
	Script  struct {
		Source string `json:"source"`
	} `json:"script"`

	ContextSetup struct {
		Document  types.JSON `json:"document"`
		IndexName string     `json:"index"`
	} `json:"context_setup"`
}

type ScriptResponse struct {
	Result []any `json:"result"`
}

type ScriptErrorErrorElement struct {
	Lang     string `json:"lang"`
	Position struct {
		End    int `json:"end"`
		Offset int `json:"offset"`
		Start  int `json:"start"`
	} `json:"position"`
	Reason      string                    `json:"reason"`
	Script      string                    `json:"script"`
	ScriptStack []string                  `json:"script_stack"`
	Type        string                    `json:"type"`
	RootCause   []ScriptErrorErrorElement `json:"root_cause"`
}

type ScriptErrorResponse struct {
	Error struct {
		CausedBy struct {
			Reason string `json:"reason"`
			Type   string `json:"type"`
		} `json:"caused_by"`
		Lang     string `json:"lang"`
		Position struct {
			End    int `json:"end"`
			Offset int `json:"offset"`
			Start  int `json:"start"`
		} `json:"position"`
		Reason      string                    `json:"reason"`
		RootCause   []ScriptErrorErrorElement `json:"root_cause"`
		Script      string                    `json:"script"`
		ScriptStack []string                  `json:"script_stack"`
		Type        string                    `json:"type"`
	} `json:"error"`
	Status int `json:"status"`
}

func RenderErrorResponse(script string, err error) ScriptErrorResponse {
	res := ScriptErrorResponse{}

	rootCause := ScriptErrorErrorElement{}
	rootCause.Reason = err.Error()
	rootCause.Type = "script_exception"
	rootCause.Lang = "painless"
	rootCause.Position.Start = 0
	rootCause.Position.End = 0
	rootCause.Position.Offset = 0
	rootCause.Script = script
	rootCause.ScriptStack = []string{script}

	res.Error.CausedBy.Reason = err.Error()
	res.Error.CausedBy.Type = "illegal_argument_exception"
	res.Error.Lang = "painless"

	res.Error.Position.Start = 0
	res.Error.Position.End = 0
	res.Error.Position.Offset = 0

	res.Error.Type = "script_exception"
	res.Error.Reason = "compile error"

	res.Error.RootCause = []ScriptErrorErrorElement{rootCause}

	res.Status = http.StatusBadRequest

	return res
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
