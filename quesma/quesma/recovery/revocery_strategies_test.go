package recovery

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestLogPanic(t *testing.T) {

	fn := func() {
		defer LogPanic()
		panic("test")
	}

	before := PanicCounter.Load()
	fn()

	assert.Equal(t, before+1, PanicCounter.Load())

}
