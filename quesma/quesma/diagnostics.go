package quesma

import (
	"errors"
	"log"
)

func quesmaRecover() {
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
		log.Println("Crashed:", err)
	}
}
