package quesma

import (
	"io"
	"log"
	"mitmproxy/quesma/quesma/recovery"
	"sync"
)

func copyAndSignal(copyCompletionBarrier *sync.WaitGroup, dst io.Writer, src io.Reader) {
	defer recovery.LogPanic()
	_, err := io.Copy(dst, src)
	if err != nil {
		log.Println("Copy error :" + err.Error())
	}
	copyCompletionBarrier.Done()
}
