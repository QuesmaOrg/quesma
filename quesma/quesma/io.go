package quesma

import (
	"io"
	"log"
	"sync"
)

func copyAndSignal(copyCompletionBarrier *sync.WaitGroup, dst io.Writer, src io.Reader) {
	defer quesmaRecover()
	_, err := io.Copy(dst, src)
	if err != nil {
		log.Println("Copy error :" + err.Error())
	}
	copyCompletionBarrier.Done()
}
