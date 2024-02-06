package logger

type channelWriter struct {
	ch chan string
}

func (w channelWriter) Write(p []byte) (n int, err error) {
	s := string(p)
	w.ch <- s
	return len(s), nil
}
