package processors

import (
	"fmt"
)

type TcpProcessor struct {
	BaseProcessor
}

func NewTcpProcessor() *TcpProcessor {
	return &TcpProcessor{
		BaseProcessor: NewBaseProcessor(),
	}
}

func (p *TcpProcessor) GetId() string {
	return "tcp"
}

func (p *TcpProcessor) Handle(metadata map[string]interface{}, message any) (map[string]interface{}, any, error) {
	fmt.Println("TCP processor")
	data := message.([]byte)
	return metadata, data, nil
}
