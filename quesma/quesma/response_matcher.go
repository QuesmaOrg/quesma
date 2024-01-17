package quesma

import "log"

type QResponse struct {
	id   string
	body []byte
}

type ResponseMatcher struct {
	responses chan *QResponse
}

func NewResponseMatcher() *ResponseMatcher {
	return &ResponseMatcher{
		responses: make(chan *QResponse, 5),
	}
}

func (rm *ResponseMatcher) Push(response *QResponse) {
	rm.responses <- response
}

func (rm *ResponseMatcher) Compare() {
	responseMap := make(map[string][]QResponse)
	for {
		resp, ok := <-rm.responses
		if ok {
			if value, ok := responseMap[resp.id]; !ok {
				responseMap[resp.id] = append(responseMap[resp.id], *resp)
			} else {
				if string(value[0].body) != string(resp.body) {
					log.Println("Responses are different:")
					log.Println("First:" + string(value[0].body))
					log.Println("Second:" + string(resp.body))
				}
			}
		}
	}
}
