package recovery

import (
	"errors"
	"log"
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
		log.Println("Panic recovered:", err)
	}
}
