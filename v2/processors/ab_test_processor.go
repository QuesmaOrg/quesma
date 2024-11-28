package processors

import (
	"encoding/json"
	"fmt"
	"github.com/google/go-cmp/cmp"
	quesma_api "quesma_v2/core"
	"strconv"
)

type ABTestProcessor struct {
	Id string
	BaseProcessor
	messageStorage     map[string][][]byte
	doResultComparison bool
}

func NewABTestProcessor(id string, doResultComparison bool) *ABTestProcessor {
	return &ABTestProcessor{
		Id:                 id,
		BaseProcessor:      NewBaseProcessor(),
		messageStorage:     make(map[string][][]byte),
		doResultComparison: doResultComparison,
	}
}

func (p *ABTestProcessor) GetId() string {
	return p.Id
}

func (p *ABTestProcessor) compare(json1 string, json2 string) (bool, string) {
	var obj1, obj2 map[string]interface{}
	err := json.Unmarshal([]byte(json1), &obj1)
	if err != nil {
		fmt.Println("Error unmarshalling JSON1:", err)
		return false, ""
	}
	json.Unmarshal([]byte(json2), &obj2)
	if err != nil {
		fmt.Println("Error unmarshalling JSON2:", err)
		return false, ""
	}

	diff := cmp.Diff(obj1, obj2)
	if diff == "" {
		fmt.Println("JSON objects are equal")
		return true, ""
	}
	fmt.Println("JSON objects are not equal:", diff)
	return false, diff
}

func (p *ABTestProcessor) Handle(metadata map[string]interface{}, message ...any) (map[string]interface{}, any, error) {
	var data []byte

	for _, m := range message {
		mCasted, err := quesma_api.CheckedCast[[]byte](m)
		if err != nil {
			panic("ABTestProcessor: invalid message type")
		}
		data = append(data, mCasted...)
		level := metadata["level"].(int)
		correlationId := quesma_api.GetCorrelationId(metadata)
		currentSlice, exists := p.messageStorage[correlationId]
		if !exists {
			currentSlice = [][]byte{}
		}
		currentSlice = append(currentSlice, mCasted)
		p.messageStorage[correlationId] = currentSlice

		data = append(data, strconv.Itoa(level)...)
		data = append(data, []byte(p.GetId())...)
		data = append(data, []byte(",correlationId:")...)
		data = append(data, []byte(correlationId)...)
		data = append(data, []byte("\n")...)
	}

	if !p.doResultComparison {
		return metadata, data, nil
	}
	resp := make([]byte, 0)
	for _, messages := range p.messageStorage {
		if len(messages) == 2 {
			equal, diff := p.compare(string(messages[0]), string(messages[1]))
			if equal {
				resp = append(resp, []byte("ABTestProcessor processor: Responses are equal\n\n")...)
				resp = append(resp, []byte("\n")...)
				resp = append(resp, []byte(diff)...)

			} else {
				resp = append(resp, []byte("ABTestProcessor processor: Responses are not equal\n\n")...)
				resp = append(resp, []byte("\n")...)
				resp = append(resp, []byte(diff)...)
			}
			// clean storage
			p.messageStorage = make(map[string][][]byte)
		}
	}

	return metadata, resp, nil
}

func (p *ABTestProcessor) GetSupportedBackendConnectors() []quesma_api.BackendConnectorType {
	return []quesma_api.BackendConnectorType{quesma_api.NoopBackend}
}
