package buffer

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestHtmlBuffer_Html(t *testing.T) {
	xss := "<script>alert('xss')</script>"
	t.Run("Html without any escaping", func(t *testing.T) {
		buf := HtmlBuffer{}
		buf.Html(xss)
		assert.Equal(t, xss, string(buf.Bytes()))
	})
	t.Run("Text with XSS escaping", func(t *testing.T) {
		buf := HtmlBuffer{}
		buf.Text(xss)
		assert.Equal(t, "&lt;script&gt;alert(&#39;xss&#39;)&lt;/script&gt;", string(buf.Bytes()))
	})
}
