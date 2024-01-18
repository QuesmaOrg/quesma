package quesma

import (
	"log"
)

type QueryDebugPrimarySource struct {
	id        string
	queryResp []byte
}

type QueryDebugSecondarySource struct {
	id string

	incomingQueryBody []byte

	queryBodyTranslated    []byte
	queryRawResults        []byte
	queryTranslatedResults []byte
}

type QueryDebugger struct {
	queryDebugPrimarySource   chan *QueryDebugPrimarySource
	queryDebugSecondarySource chan *QueryDebugSecondarySource
}

func NewQueryDebugger() *QueryDebugger {
	return &QueryDebugger{
		queryDebugPrimarySource:   make(chan *QueryDebugPrimarySource, 5),
		queryDebugSecondarySource: make(chan *QueryDebugSecondarySource, 5),
	}
}

func (qd *QueryDebugger) PushPrimaryInfo(qdebugInfo *QueryDebugPrimarySource) {
	qd.queryDebugPrimarySource <- qdebugInfo
}

func (qd *QueryDebugger) PushSecondaryInfo(qdebugInfo *QueryDebugSecondarySource) {
	qd.queryDebugSecondarySource <- qdebugInfo
}

func (qd *QueryDebugger) GenerateReport() {
	for {
		select {
		case msg := <-qd.queryDebugPrimarySource:
			log.Println("Received debug info from primary source:", msg.id)
		case msg := <-qd.queryDebugSecondarySource:
			log.Println("Received debug info from secondary source:", msg.id)
		}
	}
}
