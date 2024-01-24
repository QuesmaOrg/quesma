package recovery

import (
	"errors"
	"log"
	"runtime/debug"
)

func LogPanic() {
	r := recover()
	if r != nil {
		var err error
		switch t := r.(type) {
		case string:
			err = errors.New(t)
		case error:
			err = t
		default:
			err = errors.New("unknown error")
		}
		log.Printf("Panic recovered: %s\n%s", err, string(debug.Stack()))
	}
}
