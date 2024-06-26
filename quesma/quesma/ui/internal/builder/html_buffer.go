// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package builder

import (
	"bytes"
	"html"
)

type HtmlBuffer struct {
	buffer bytes.Buffer
}

// Write without any escaping
func (h *HtmlBuffer) Grow(n int) {
	h.buffer.Grow(n)
}

// Write without any escaping
func (h *HtmlBuffer) Write(p []byte) (n int, err error) {
	return h.buffer.Write(p)
}

// Html without any escaping
func (h *HtmlBuffer) Html(s string) *HtmlBuffer {
	_, err := h.buffer.WriteString(s)
	if err != nil {
		panic(err)
	}
	return h
}

// Text with Xss escaping
func (h *HtmlBuffer) Text(s string) *HtmlBuffer {
	_, err := h.buffer.WriteString(html.EscapeString(s))
	if err != nil {
		panic(err)
	}
	return h
}

func (h *HtmlBuffer) Bytes() []byte {
	return h.buffer.Bytes()
}
