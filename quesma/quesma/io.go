package quesma

import (
	"io"
	"log"
)

func copyAndSignal(signal chan struct{}, dst io.Writer, src io.Reader) {
	defer quesmaRecover()
	_, err := io.Copy(dst, src)
	if err != nil {
		log.Println(err)
	}
	signal <- struct{}{}
}
