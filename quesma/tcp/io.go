package tcp

import (
	"io"
	"log"
	"mitmproxy/quesma/quesma/recovery"
	"sync"
)

func CopyAndSignal(copyCompletionBarrier *sync.WaitGroup, dst io.Writer, src io.Reader) {
	defer recovery.LogPanic()
	if _, err := io.Copy(dst, src); err != nil {
		log.Println("Copy error :" + err.Error())
	}
	copyCompletionBarrier.Done()
}
