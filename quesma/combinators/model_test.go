package combinators

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestNewFunction(t *testing.T) {

	fn := NewFunction("testIf")

	assert.Equal(t, "testStateIf", fn.SetState(true).String())

	assert.Equal(t, "testMergeIf", fn.SetMerge(true).String())

}
