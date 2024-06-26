// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
// Code generated from quesma/eql/parser/EQL.g4 by ANTLR 4.13.1. DO NOT EDIT.

package parser // EQL
import (
	"fmt"
	"strconv"
	"sync"

	"github.com/antlr4-go/antlr/v4"
)

// Suppress unused import errors
var _ = fmt.Printf
var _ = strconv.Itoa
var _ = sync.Once{}

type EQLParser struct {
	*antlr.BaseParser
}

var EQLParserStaticData struct {
	once                   sync.Once
	serializedATN          []int32
	LiteralNames           []string
	SymbolicNames          []string
	RuleNames              []string
	PredictionContextCache *antlr.PredictionContextCache
	atn                    *antlr.ATN
	decisionToDFA          []*antlr.DFA
}

func eqlParserInit() {
	staticData := &EQLParserStaticData
	staticData.LiteralNames = []string{
		"", "'|'", "'where'", "'sequence'", "'by'", "'with'", "'maxspan'", "'='",
		"'['", "']'", "'sample'", "'not'", "'('", "')'", "'=='", "'!='", "'>'",
		"'<'", "'>='", "'<='", "':'", "'like'", "'like~'", "'regex'", "'regex~'",
		"'in'", "'in~'", "'and'", "'or'", "'?'", "','", "'null'", "'*'", "'/'",
		"'%'", "'+'", "'-'", "'head'", "'tail'", "'count'", "'unique'", "'filter'",
		"'sort'", "'add'", "'between'", "'cidrMatch'", "'concat'", "'divide'",
		"'endsWith'", "'endsWith~'", "'indexOf'", "'indexOf~'", "'length'",
		"'modulo'", "'multiply'", "'number'", "'startsWith'", "'startsWith~'",
		"'string'", "'stringContains'", "'stringContains~'", "'substring'",
		"'subtract'", "'any'",
	}
	staticData.SymbolicNames = []string{
		"", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "",
		"", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "",
		"", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "",
		"", "", "", "", "", "", "", "", "", "", "", "", "ANY", "MULTILINE_COMMENT",
		"ONELINE_COMMNET", "BOOLEAN", "INTERVAL", "NUMBER", "ESC", "STRING",
		"WS", "ID",
	}
	staticData.RuleNames = []string{
		"query", "simpleQuery", "sequenceQuery", "sampleQuery", "condition",
		"category", "field", "fieldList", "literal", "literalList", "value",
		"pipe", "funcall", "funcName", "interval",
	}
	staticData.PredictionContextCache = antlr.NewPredictionContextCache()
	staticData.serializedATN = []int32{
		4, 1, 72, 195, 2, 0, 7, 0, 2, 1, 7, 1, 2, 2, 7, 2, 2, 3, 7, 3, 2, 4, 7,
		4, 2, 5, 7, 5, 2, 6, 7, 6, 2, 7, 7, 7, 2, 8, 7, 8, 2, 9, 7, 9, 2, 10, 7,
		10, 2, 11, 7, 11, 2, 12, 7, 12, 2, 13, 7, 13, 2, 14, 7, 14, 1, 0, 1, 0,
		1, 0, 3, 0, 34, 8, 0, 1, 0, 1, 0, 5, 0, 38, 8, 0, 10, 0, 12, 0, 41, 9,
		0, 1, 0, 1, 0, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 1, 2, 1, 2, 3, 2, 52, 8, 2,
		1, 2, 1, 2, 1, 2, 1, 2, 3, 2, 58, 8, 2, 1, 2, 1, 2, 1, 2, 1, 2, 1, 2, 3,
		2, 65, 8, 2, 4, 2, 67, 8, 2, 11, 2, 12, 2, 68, 1, 3, 1, 3, 1, 3, 1, 3,
		1, 3, 1, 3, 1, 3, 4, 3, 78, 8, 3, 11, 3, 12, 3, 79, 1, 4, 1, 4, 1, 4, 1,
		4, 1, 4, 1, 4, 1, 4, 1, 4, 1, 4, 1, 4, 1, 4, 1, 4, 1, 4, 1, 4, 1, 4, 1,
		4, 1, 4, 1, 4, 1, 4, 1, 4, 1, 4, 1, 4, 1, 4, 1, 4, 3, 4, 106, 8, 4, 1,
		4, 1, 4, 1, 4, 5, 4, 111, 8, 4, 10, 4, 12, 4, 114, 9, 4, 1, 5, 1, 5, 1,
		6, 1, 6, 1, 6, 3, 6, 121, 8, 6, 1, 7, 1, 7, 1, 7, 5, 7, 126, 8, 7, 10,
		7, 12, 7, 129, 9, 7, 1, 8, 1, 8, 1, 9, 1, 9, 1, 9, 1, 9, 5, 9, 137, 8,
		9, 10, 9, 12, 9, 140, 9, 9, 1, 9, 1, 9, 1, 10, 1, 10, 1, 10, 1, 10, 1,
		10, 1, 10, 1, 10, 1, 10, 1, 10, 3, 10, 153, 8, 10, 1, 10, 1, 10, 1, 10,
		1, 10, 1, 10, 1, 10, 5, 10, 161, 8, 10, 10, 10, 12, 10, 164, 9, 10, 1,
		11, 1, 11, 1, 11, 1, 11, 1, 11, 1, 11, 1, 11, 1, 11, 1, 11, 1, 11, 1, 11,
		3, 11, 177, 8, 11, 1, 12, 1, 12, 1, 12, 1, 12, 1, 12, 5, 12, 184, 8, 12,
		10, 12, 12, 12, 187, 9, 12, 1, 12, 1, 12, 1, 13, 1, 13, 1, 14, 1, 14, 1,
		14, 0, 2, 8, 20, 15, 0, 2, 4, 6, 8, 10, 12, 14, 16, 18, 20, 22, 24, 26,
		28, 0, 9, 1, 0, 14, 24, 1, 0, 25, 26, 1, 0, 20, 26, 1, 0, 27, 28, 3, 0,
		63, 63, 70, 70, 72, 72, 3, 0, 66, 66, 68, 68, 70, 70, 1, 0, 32, 34, 1,
		0, 35, 36, 1, 0, 43, 62, 210, 0, 33, 1, 0, 0, 0, 2, 44, 1, 0, 0, 0, 4,
		48, 1, 0, 0, 0, 6, 70, 1, 0, 0, 0, 8, 105, 1, 0, 0, 0, 10, 115, 1, 0, 0,
		0, 12, 120, 1, 0, 0, 0, 14, 122, 1, 0, 0, 0, 16, 130, 1, 0, 0, 0, 18, 132,
		1, 0, 0, 0, 20, 152, 1, 0, 0, 0, 22, 176, 1, 0, 0, 0, 24, 178, 1, 0, 0,
		0, 26, 190, 1, 0, 0, 0, 28, 192, 1, 0, 0, 0, 30, 34, 3, 2, 1, 0, 31, 34,
		3, 4, 2, 0, 32, 34, 3, 6, 3, 0, 33, 30, 1, 0, 0, 0, 33, 31, 1, 0, 0, 0,
		33, 32, 1, 0, 0, 0, 34, 39, 1, 0, 0, 0, 35, 36, 5, 1, 0, 0, 36, 38, 3,
		22, 11, 0, 37, 35, 1, 0, 0, 0, 38, 41, 1, 0, 0, 0, 39, 37, 1, 0, 0, 0,
		39, 40, 1, 0, 0, 0, 40, 42, 1, 0, 0, 0, 41, 39, 1, 0, 0, 0, 42, 43, 5,
		0, 0, 1, 43, 1, 1, 0, 0, 0, 44, 45, 3, 10, 5, 0, 45, 46, 5, 2, 0, 0, 46,
		47, 3, 8, 4, 0, 47, 3, 1, 0, 0, 0, 48, 51, 5, 3, 0, 0, 49, 50, 5, 4, 0,
		0, 50, 52, 3, 14, 7, 0, 51, 49, 1, 0, 0, 0, 51, 52, 1, 0, 0, 0, 52, 57,
		1, 0, 0, 0, 53, 54, 5, 5, 0, 0, 54, 55, 5, 6, 0, 0, 55, 56, 5, 7, 0, 0,
		56, 58, 3, 28, 14, 0, 57, 53, 1, 0, 0, 0, 57, 58, 1, 0, 0, 0, 58, 66, 1,
		0, 0, 0, 59, 60, 5, 8, 0, 0, 60, 61, 3, 2, 1, 0, 61, 64, 5, 9, 0, 0, 62,
		63, 5, 4, 0, 0, 63, 65, 3, 14, 7, 0, 64, 62, 1, 0, 0, 0, 64, 65, 1, 0,
		0, 0, 65, 67, 1, 0, 0, 0, 66, 59, 1, 0, 0, 0, 67, 68, 1, 0, 0, 0, 68, 66,
		1, 0, 0, 0, 68, 69, 1, 0, 0, 0, 69, 5, 1, 0, 0, 0, 70, 71, 5, 10, 0, 0,
		71, 72, 5, 4, 0, 0, 72, 77, 3, 14, 7, 0, 73, 74, 5, 8, 0, 0, 74, 75, 3,
		2, 1, 0, 75, 76, 5, 9, 0, 0, 76, 78, 1, 0, 0, 0, 77, 73, 1, 0, 0, 0, 78,
		79, 1, 0, 0, 0, 79, 77, 1, 0, 0, 0, 79, 80, 1, 0, 0, 0, 80, 7, 1, 0, 0,
		0, 81, 82, 6, 4, -1, 0, 82, 106, 5, 66, 0, 0, 83, 84, 5, 11, 0, 0, 84,
		106, 3, 8, 4, 8, 85, 86, 5, 12, 0, 0, 86, 87, 3, 8, 4, 0, 87, 88, 5, 13,
		0, 0, 88, 106, 1, 0, 0, 0, 89, 90, 3, 20, 10, 0, 90, 91, 7, 0, 0, 0, 91,
		92, 3, 20, 10, 0, 92, 106, 1, 0, 0, 0, 93, 94, 3, 12, 6, 0, 94, 95, 5,
		11, 0, 0, 95, 96, 7, 1, 0, 0, 96, 97, 3, 18, 9, 0, 97, 106, 1, 0, 0, 0,
		98, 99, 3, 12, 6, 0, 99, 100, 7, 2, 0, 0, 100, 101, 3, 18, 9, 0, 101, 106,
		1, 0, 0, 0, 102, 106, 3, 24, 12, 0, 103, 104, 5, 11, 0, 0, 104, 106, 3,
		24, 12, 0, 105, 81, 1, 0, 0, 0, 105, 83, 1, 0, 0, 0, 105, 85, 1, 0, 0,
		0, 105, 89, 1, 0, 0, 0, 105, 93, 1, 0, 0, 0, 105, 98, 1, 0, 0, 0, 105,
		102, 1, 0, 0, 0, 105, 103, 1, 0, 0, 0, 106, 112, 1, 0, 0, 0, 107, 108,
		10, 3, 0, 0, 108, 109, 7, 3, 0, 0, 109, 111, 3, 8, 4, 4, 110, 107, 1, 0,
		0, 0, 111, 114, 1, 0, 0, 0, 112, 110, 1, 0, 0, 0, 112, 113, 1, 0, 0, 0,
		113, 9, 1, 0, 0, 0, 114, 112, 1, 0, 0, 0, 115, 116, 7, 4, 0, 0, 116, 11,
		1, 0, 0, 0, 117, 121, 5, 72, 0, 0, 118, 119, 5, 29, 0, 0, 119, 121, 5,
		72, 0, 0, 120, 117, 1, 0, 0, 0, 120, 118, 1, 0, 0, 0, 121, 13, 1, 0, 0,
		0, 122, 127, 3, 12, 6, 0, 123, 124, 5, 30, 0, 0, 124, 126, 3, 12, 6, 0,
		125, 123, 1, 0, 0, 0, 126, 129, 1, 0, 0, 0, 127, 125, 1, 0, 0, 0, 127,
		128, 1, 0, 0, 0, 128, 15, 1, 0, 0, 0, 129, 127, 1, 0, 0, 0, 130, 131, 7,
		5, 0, 0, 131, 17, 1, 0, 0, 0, 132, 133, 5, 12, 0, 0, 133, 138, 3, 16, 8,
		0, 134, 135, 5, 30, 0, 0, 135, 137, 3, 16, 8, 0, 136, 134, 1, 0, 0, 0,
		137, 140, 1, 0, 0, 0, 138, 136, 1, 0, 0, 0, 138, 139, 1, 0, 0, 0, 139,
		141, 1, 0, 0, 0, 140, 138, 1, 0, 0, 0, 141, 142, 5, 13, 0, 0, 142, 19,
		1, 0, 0, 0, 143, 144, 6, 10, -1, 0, 144, 153, 5, 31, 0, 0, 145, 153, 3,
		16, 8, 0, 146, 153, 3, 12, 6, 0, 147, 153, 3, 24, 12, 0, 148, 149, 5, 12,
		0, 0, 149, 150, 3, 20, 10, 0, 150, 151, 5, 13, 0, 0, 151, 153, 1, 0, 0,
		0, 152, 143, 1, 0, 0, 0, 152, 145, 1, 0, 0, 0, 152, 146, 1, 0, 0, 0, 152,
		147, 1, 0, 0, 0, 152, 148, 1, 0, 0, 0, 153, 162, 1, 0, 0, 0, 154, 155,
		10, 2, 0, 0, 155, 156, 7, 6, 0, 0, 156, 161, 3, 20, 10, 3, 157, 158, 10,
		1, 0, 0, 158, 159, 7, 7, 0, 0, 159, 161, 3, 20, 10, 2, 160, 154, 1, 0,
		0, 0, 160, 157, 1, 0, 0, 0, 161, 164, 1, 0, 0, 0, 162, 160, 1, 0, 0, 0,
		162, 163, 1, 0, 0, 0, 163, 21, 1, 0, 0, 0, 164, 162, 1, 0, 0, 0, 165, 166,
		5, 37, 0, 0, 166, 177, 5, 68, 0, 0, 167, 168, 5, 38, 0, 0, 168, 177, 5,
		68, 0, 0, 169, 177, 5, 39, 0, 0, 170, 171, 5, 40, 0, 0, 171, 177, 3, 14,
		7, 0, 172, 173, 5, 41, 0, 0, 173, 177, 3, 8, 4, 0, 174, 175, 5, 42, 0,
		0, 175, 177, 3, 14, 7, 0, 176, 165, 1, 0, 0, 0, 176, 167, 1, 0, 0, 0, 176,
		169, 1, 0, 0, 0, 176, 170, 1, 0, 0, 0, 176, 172, 1, 0, 0, 0, 176, 174,
		1, 0, 0, 0, 177, 23, 1, 0, 0, 0, 178, 179, 3, 26, 13, 0, 179, 180, 5, 12,
		0, 0, 180, 185, 3, 20, 10, 0, 181, 182, 5, 30, 0, 0, 182, 184, 3, 20, 10,
		0, 183, 181, 1, 0, 0, 0, 184, 187, 1, 0, 0, 0, 185, 183, 1, 0, 0, 0, 185,
		186, 1, 0, 0, 0, 186, 188, 1, 0, 0, 0, 187, 185, 1, 0, 0, 0, 188, 189,
		5, 13, 0, 0, 189, 25, 1, 0, 0, 0, 190, 191, 7, 8, 0, 0, 191, 27, 1, 0,
		0, 0, 192, 193, 5, 67, 0, 0, 193, 29, 1, 0, 0, 0, 17, 33, 39, 51, 57, 64,
		68, 79, 105, 112, 120, 127, 138, 152, 160, 162, 176, 185,
	}
	deserializer := antlr.NewATNDeserializer(nil)
	staticData.atn = deserializer.Deserialize(staticData.serializedATN)
	atn := staticData.atn
	staticData.decisionToDFA = make([]*antlr.DFA, len(atn.DecisionToState))
	decisionToDFA := staticData.decisionToDFA
	for index, state := range atn.DecisionToState {
		decisionToDFA[index] = antlr.NewDFA(state, index)
	}
}

// EQLParserInit initializes any static state used to implement EQLParser. By default the
// static state used to implement the parser is lazily initialized during the first call to
// NewEQLParser(). You can call this function if you wish to initialize the static state ahead
// of time.
func EQLParserInit() {
	staticData := &EQLParserStaticData
	staticData.once.Do(eqlParserInit)
}

// NewEQLParser produces a new parser instance for the optional input antlr.TokenStream.
func NewEQLParser(input antlr.TokenStream) *EQLParser {
	EQLParserInit()
	this := new(EQLParser)
	this.BaseParser = antlr.NewBaseParser(input)
	staticData := &EQLParserStaticData
	this.Interpreter = antlr.NewParserATNSimulator(this, staticData.atn, staticData.decisionToDFA, staticData.PredictionContextCache)
	this.RuleNames = staticData.RuleNames
	this.LiteralNames = staticData.LiteralNames
	this.SymbolicNames = staticData.SymbolicNames
	this.GrammarFileName = "EQL.g4"

	return this
}

// EQLParser tokens.
const (
	EQLParserEOF               = antlr.TokenEOF
	EQLParserT__0              = 1
	EQLParserT__1              = 2
	EQLParserT__2              = 3
	EQLParserT__3              = 4
	EQLParserT__4              = 5
	EQLParserT__5              = 6
	EQLParserT__6              = 7
	EQLParserT__7              = 8
	EQLParserT__8              = 9
	EQLParserT__9              = 10
	EQLParserT__10             = 11
	EQLParserT__11             = 12
	EQLParserT__12             = 13
	EQLParserT__13             = 14
	EQLParserT__14             = 15
	EQLParserT__15             = 16
	EQLParserT__16             = 17
	EQLParserT__17             = 18
	EQLParserT__18             = 19
	EQLParserT__19             = 20
	EQLParserT__20             = 21
	EQLParserT__21             = 22
	EQLParserT__22             = 23
	EQLParserT__23             = 24
	EQLParserT__24             = 25
	EQLParserT__25             = 26
	EQLParserT__26             = 27
	EQLParserT__27             = 28
	EQLParserT__28             = 29
	EQLParserT__29             = 30
	EQLParserT__30             = 31
	EQLParserT__31             = 32
	EQLParserT__32             = 33
	EQLParserT__33             = 34
	EQLParserT__34             = 35
	EQLParserT__35             = 36
	EQLParserT__36             = 37
	EQLParserT__37             = 38
	EQLParserT__38             = 39
	EQLParserT__39             = 40
	EQLParserT__40             = 41
	EQLParserT__41             = 42
	EQLParserT__42             = 43
	EQLParserT__43             = 44
	EQLParserT__44             = 45
	EQLParserT__45             = 46
	EQLParserT__46             = 47
	EQLParserT__47             = 48
	EQLParserT__48             = 49
	EQLParserT__49             = 50
	EQLParserT__50             = 51
	EQLParserT__51             = 52
	EQLParserT__52             = 53
	EQLParserT__53             = 54
	EQLParserT__54             = 55
	EQLParserT__55             = 56
	EQLParserT__56             = 57
	EQLParserT__57             = 58
	EQLParserT__58             = 59
	EQLParserT__59             = 60
	EQLParserT__60             = 61
	EQLParserT__61             = 62
	EQLParserANY               = 63
	EQLParserMULTILINE_COMMENT = 64
	EQLParserONELINE_COMMNET   = 65
	EQLParserBOOLEAN           = 66
	EQLParserINTERVAL          = 67
	EQLParserNUMBER            = 68
	EQLParserESC               = 69
	EQLParserSTRING            = 70
	EQLParserWS                = 71
	EQLParserID                = 72
)

// EQLParser rules.
const (
	EQLParserRULE_query         = 0
	EQLParserRULE_simpleQuery   = 1
	EQLParserRULE_sequenceQuery = 2
	EQLParserRULE_sampleQuery   = 3
	EQLParserRULE_condition     = 4
	EQLParserRULE_category      = 5
	EQLParserRULE_field         = 6
	EQLParserRULE_fieldList     = 7
	EQLParserRULE_literal       = 8
	EQLParserRULE_literalList   = 9
	EQLParserRULE_value         = 10
	EQLParserRULE_pipe          = 11
	EQLParserRULE_funcall       = 12
	EQLParserRULE_funcName      = 13
	EQLParserRULE_interval      = 14
)

// IQueryContext is an interface to support dynamic dispatch.
type IQueryContext interface {
	antlr.ParserRuleContext

	// GetParser returns the parser.
	GetParser() antlr.Parser

	// Getter signatures
	EOF() antlr.TerminalNode
	SimpleQuery() ISimpleQueryContext
	SequenceQuery() ISequenceQueryContext
	SampleQuery() ISampleQueryContext
	AllPipe() []IPipeContext
	Pipe(i int) IPipeContext

	// IsQueryContext differentiates from other interfaces.
	IsQueryContext()
}

type QueryContext struct {
	antlr.BaseParserRuleContext
	parser antlr.Parser
}

func NewEmptyQueryContext() *QueryContext {
	var p = new(QueryContext)
	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, nil, -1)
	p.RuleIndex = EQLParserRULE_query
	return p
}

func InitEmptyQueryContext(p *QueryContext) {
	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, nil, -1)
	p.RuleIndex = EQLParserRULE_query
}

func (*QueryContext) IsQueryContext() {}

func NewQueryContext(parser antlr.Parser, parent antlr.ParserRuleContext, invokingState int) *QueryContext {
	var p = new(QueryContext)

	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, parent, invokingState)

	p.parser = parser
	p.RuleIndex = EQLParserRULE_query

	return p
}

func (s *QueryContext) GetParser() antlr.Parser { return s.parser }

func (s *QueryContext) EOF() antlr.TerminalNode {
	return s.GetToken(EQLParserEOF, 0)
}

func (s *QueryContext) SimpleQuery() ISimpleQueryContext {
	var t antlr.RuleContext
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(ISimpleQueryContext); ok {
			t = ctx.(antlr.RuleContext)
			break
		}
	}

	if t == nil {
		return nil
	}

	return t.(ISimpleQueryContext)
}

func (s *QueryContext) SequenceQuery() ISequenceQueryContext {
	var t antlr.RuleContext
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(ISequenceQueryContext); ok {
			t = ctx.(antlr.RuleContext)
			break
		}
	}

	if t == nil {
		return nil
	}

	return t.(ISequenceQueryContext)
}

func (s *QueryContext) SampleQuery() ISampleQueryContext {
	var t antlr.RuleContext
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(ISampleQueryContext); ok {
			t = ctx.(antlr.RuleContext)
			break
		}
	}

	if t == nil {
		return nil
	}

	return t.(ISampleQueryContext)
}

func (s *QueryContext) AllPipe() []IPipeContext {
	children := s.GetChildren()
	len := 0
	for _, ctx := range children {
		if _, ok := ctx.(IPipeContext); ok {
			len++
		}
	}

	tst := make([]IPipeContext, len)
	i := 0
	for _, ctx := range children {
		if t, ok := ctx.(IPipeContext); ok {
			tst[i] = t.(IPipeContext)
			i++
		}
	}

	return tst
}

func (s *QueryContext) Pipe(i int) IPipeContext {
	var t antlr.RuleContext
	j := 0
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IPipeContext); ok {
			if j == i {
				t = ctx.(antlr.RuleContext)
				break
			}
			j++
		}
	}

	if t == nil {
		return nil
	}

	return t.(IPipeContext)
}

func (s *QueryContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *QueryContext) ToStringTree(ruleNames []string, recog antlr.Recognizer) string {
	return antlr.TreesStringTree(s, ruleNames, recog)
}

func (s *QueryContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(EQLListener); ok {
		listenerT.EnterQuery(s)
	}
}

func (s *QueryContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(EQLListener); ok {
		listenerT.ExitQuery(s)
	}
}

func (s *QueryContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case EQLVisitor:
		return t.VisitQuery(s)

	default:
		return t.VisitChildren(s)
	}
}

func (p *EQLParser) Query() (localctx IQueryContext) {
	localctx = NewQueryContext(p, p.GetParserRuleContext(), p.GetState())
	p.EnterRule(localctx, 0, EQLParserRULE_query)
	var _la int

	p.EnterOuterAlt(localctx, 1)
	p.SetState(33)
	p.GetErrorHandler().Sync(p)
	if p.HasError() {
		goto errorExit
	}

	switch p.GetTokenStream().LA(1) {
	case EQLParserANY, EQLParserSTRING, EQLParserID:
		{
			p.SetState(30)
			p.SimpleQuery()
		}

	case EQLParserT__2:
		{
			p.SetState(31)
			p.SequenceQuery()
		}

	case EQLParserT__9:
		{
			p.SetState(32)
			p.SampleQuery()
		}

	default:
		p.SetError(antlr.NewNoViableAltException(p, nil, nil, nil, nil, nil))
		goto errorExit
	}
	p.SetState(39)
	p.GetErrorHandler().Sync(p)
	if p.HasError() {
		goto errorExit
	}
	_la = p.GetTokenStream().LA(1)

	for _la == EQLParserT__0 {
		{
			p.SetState(35)
			p.Match(EQLParserT__0)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(36)
			p.Pipe()
		}

		p.SetState(41)
		p.GetErrorHandler().Sync(p)
		if p.HasError() {
			goto errorExit
		}
		_la = p.GetTokenStream().LA(1)
	}
	{
		p.SetState(42)
		p.Match(EQLParserEOF)
		if p.HasError() {
			// Recognition error - abort rule
			goto errorExit
		}
	}

errorExit:
	if p.HasError() {
		v := p.GetError()
		localctx.SetException(v)
		p.GetErrorHandler().ReportError(p, v)
		p.GetErrorHandler().Recover(p, v)
		p.SetError(nil)
	}
	p.ExitRule()
	if false {
		goto errorExit // Trick to prevent compiler error if the label is not used
	}
	return localctx
}

// ISimpleQueryContext is an interface to support dynamic dispatch.
type ISimpleQueryContext interface {
	antlr.ParserRuleContext

	// GetParser returns the parser.
	GetParser() antlr.Parser

	// Getter signatures
	Category() ICategoryContext
	Condition() IConditionContext

	// IsSimpleQueryContext differentiates from other interfaces.
	IsSimpleQueryContext()
}

type SimpleQueryContext struct {
	antlr.BaseParserRuleContext
	parser antlr.Parser
}

func NewEmptySimpleQueryContext() *SimpleQueryContext {
	var p = new(SimpleQueryContext)
	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, nil, -1)
	p.RuleIndex = EQLParserRULE_simpleQuery
	return p
}

func InitEmptySimpleQueryContext(p *SimpleQueryContext) {
	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, nil, -1)
	p.RuleIndex = EQLParserRULE_simpleQuery
}

func (*SimpleQueryContext) IsSimpleQueryContext() {}

func NewSimpleQueryContext(parser antlr.Parser, parent antlr.ParserRuleContext, invokingState int) *SimpleQueryContext {
	var p = new(SimpleQueryContext)

	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, parent, invokingState)

	p.parser = parser
	p.RuleIndex = EQLParserRULE_simpleQuery

	return p
}

func (s *SimpleQueryContext) GetParser() antlr.Parser { return s.parser }

func (s *SimpleQueryContext) Category() ICategoryContext {
	var t antlr.RuleContext
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(ICategoryContext); ok {
			t = ctx.(antlr.RuleContext)
			break
		}
	}

	if t == nil {
		return nil
	}

	return t.(ICategoryContext)
}

func (s *SimpleQueryContext) Condition() IConditionContext {
	var t antlr.RuleContext
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IConditionContext); ok {
			t = ctx.(antlr.RuleContext)
			break
		}
	}

	if t == nil {
		return nil
	}

	return t.(IConditionContext)
}

func (s *SimpleQueryContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *SimpleQueryContext) ToStringTree(ruleNames []string, recog antlr.Recognizer) string {
	return antlr.TreesStringTree(s, ruleNames, recog)
}

func (s *SimpleQueryContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(EQLListener); ok {
		listenerT.EnterSimpleQuery(s)
	}
}

func (s *SimpleQueryContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(EQLListener); ok {
		listenerT.ExitSimpleQuery(s)
	}
}

func (s *SimpleQueryContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case EQLVisitor:
		return t.VisitSimpleQuery(s)

	default:
		return t.VisitChildren(s)
	}
}

func (p *EQLParser) SimpleQuery() (localctx ISimpleQueryContext) {
	localctx = NewSimpleQueryContext(p, p.GetParserRuleContext(), p.GetState())
	p.EnterRule(localctx, 2, EQLParserRULE_simpleQuery)
	p.EnterOuterAlt(localctx, 1)
	{
		p.SetState(44)
		p.Category()
	}
	{
		p.SetState(45)
		p.Match(EQLParserT__1)
		if p.HasError() {
			// Recognition error - abort rule
			goto errorExit
		}
	}
	{
		p.SetState(46)
		p.condition(0)
	}

errorExit:
	if p.HasError() {
		v := p.GetError()
		localctx.SetException(v)
		p.GetErrorHandler().ReportError(p, v)
		p.GetErrorHandler().Recover(p, v)
		p.SetError(nil)
	}
	p.ExitRule()
	if false {
		goto errorExit // Trick to prevent compiler error if the label is not used
	}
	return localctx
}

// ISequenceQueryContext is an interface to support dynamic dispatch.
type ISequenceQueryContext interface {
	antlr.ParserRuleContext

	// GetParser returns the parser.
	GetParser() antlr.Parser

	// Getter signatures
	AllFieldList() []IFieldListContext
	FieldList(i int) IFieldListContext
	Interval() IIntervalContext
	AllSimpleQuery() []ISimpleQueryContext
	SimpleQuery(i int) ISimpleQueryContext

	// IsSequenceQueryContext differentiates from other interfaces.
	IsSequenceQueryContext()
}

type SequenceQueryContext struct {
	antlr.BaseParserRuleContext
	parser antlr.Parser
}

func NewEmptySequenceQueryContext() *SequenceQueryContext {
	var p = new(SequenceQueryContext)
	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, nil, -1)
	p.RuleIndex = EQLParserRULE_sequenceQuery
	return p
}

func InitEmptySequenceQueryContext(p *SequenceQueryContext) {
	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, nil, -1)
	p.RuleIndex = EQLParserRULE_sequenceQuery
}

func (*SequenceQueryContext) IsSequenceQueryContext() {}

func NewSequenceQueryContext(parser antlr.Parser, parent antlr.ParserRuleContext, invokingState int) *SequenceQueryContext {
	var p = new(SequenceQueryContext)

	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, parent, invokingState)

	p.parser = parser
	p.RuleIndex = EQLParserRULE_sequenceQuery

	return p
}

func (s *SequenceQueryContext) GetParser() antlr.Parser { return s.parser }

func (s *SequenceQueryContext) AllFieldList() []IFieldListContext {
	children := s.GetChildren()
	len := 0
	for _, ctx := range children {
		if _, ok := ctx.(IFieldListContext); ok {
			len++
		}
	}

	tst := make([]IFieldListContext, len)
	i := 0
	for _, ctx := range children {
		if t, ok := ctx.(IFieldListContext); ok {
			tst[i] = t.(IFieldListContext)
			i++
		}
	}

	return tst
}

func (s *SequenceQueryContext) FieldList(i int) IFieldListContext {
	var t antlr.RuleContext
	j := 0
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IFieldListContext); ok {
			if j == i {
				t = ctx.(antlr.RuleContext)
				break
			}
			j++
		}
	}

	if t == nil {
		return nil
	}

	return t.(IFieldListContext)
}

func (s *SequenceQueryContext) Interval() IIntervalContext {
	var t antlr.RuleContext
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IIntervalContext); ok {
			t = ctx.(antlr.RuleContext)
			break
		}
	}

	if t == nil {
		return nil
	}

	return t.(IIntervalContext)
}

func (s *SequenceQueryContext) AllSimpleQuery() []ISimpleQueryContext {
	children := s.GetChildren()
	len := 0
	for _, ctx := range children {
		if _, ok := ctx.(ISimpleQueryContext); ok {
			len++
		}
	}

	tst := make([]ISimpleQueryContext, len)
	i := 0
	for _, ctx := range children {
		if t, ok := ctx.(ISimpleQueryContext); ok {
			tst[i] = t.(ISimpleQueryContext)
			i++
		}
	}

	return tst
}

func (s *SequenceQueryContext) SimpleQuery(i int) ISimpleQueryContext {
	var t antlr.RuleContext
	j := 0
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(ISimpleQueryContext); ok {
			if j == i {
				t = ctx.(antlr.RuleContext)
				break
			}
			j++
		}
	}

	if t == nil {
		return nil
	}

	return t.(ISimpleQueryContext)
}

func (s *SequenceQueryContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *SequenceQueryContext) ToStringTree(ruleNames []string, recog antlr.Recognizer) string {
	return antlr.TreesStringTree(s, ruleNames, recog)
}

func (s *SequenceQueryContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(EQLListener); ok {
		listenerT.EnterSequenceQuery(s)
	}
}

func (s *SequenceQueryContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(EQLListener); ok {
		listenerT.ExitSequenceQuery(s)
	}
}

func (s *SequenceQueryContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case EQLVisitor:
		return t.VisitSequenceQuery(s)

	default:
		return t.VisitChildren(s)
	}
}

func (p *EQLParser) SequenceQuery() (localctx ISequenceQueryContext) {
	localctx = NewSequenceQueryContext(p, p.GetParserRuleContext(), p.GetState())
	p.EnterRule(localctx, 4, EQLParserRULE_sequenceQuery)
	var _la int

	p.EnterOuterAlt(localctx, 1)
	{
		p.SetState(48)
		p.Match(EQLParserT__2)
		if p.HasError() {
			// Recognition error - abort rule
			goto errorExit
		}
	}
	p.SetState(51)
	p.GetErrorHandler().Sync(p)
	if p.HasError() {
		goto errorExit
	}
	_la = p.GetTokenStream().LA(1)

	if _la == EQLParserT__3 {
		{
			p.SetState(49)
			p.Match(EQLParserT__3)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(50)
			p.FieldList()
		}

	}
	p.SetState(57)
	p.GetErrorHandler().Sync(p)
	if p.HasError() {
		goto errorExit
	}
	_la = p.GetTokenStream().LA(1)

	if _la == EQLParserT__4 {
		{
			p.SetState(53)
			p.Match(EQLParserT__4)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(54)
			p.Match(EQLParserT__5)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(55)
			p.Match(EQLParserT__6)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(56)
			p.Interval()
		}

	}
	p.SetState(66)
	p.GetErrorHandler().Sync(p)
	if p.HasError() {
		goto errorExit
	}
	_la = p.GetTokenStream().LA(1)

	for ok := true; ok; ok = _la == EQLParserT__7 {
		{
			p.SetState(59)
			p.Match(EQLParserT__7)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(60)
			p.SimpleQuery()
		}
		{
			p.SetState(61)
			p.Match(EQLParserT__8)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		p.SetState(64)
		p.GetErrorHandler().Sync(p)
		if p.HasError() {
			goto errorExit
		}
		_la = p.GetTokenStream().LA(1)

		if _la == EQLParserT__3 {
			{
				p.SetState(62)
				p.Match(EQLParserT__3)
				if p.HasError() {
					// Recognition error - abort rule
					goto errorExit
				}
			}
			{
				p.SetState(63)
				p.FieldList()
			}

		}

		p.SetState(68)
		p.GetErrorHandler().Sync(p)
		if p.HasError() {
			goto errorExit
		}
		_la = p.GetTokenStream().LA(1)
	}

errorExit:
	if p.HasError() {
		v := p.GetError()
		localctx.SetException(v)
		p.GetErrorHandler().ReportError(p, v)
		p.GetErrorHandler().Recover(p, v)
		p.SetError(nil)
	}
	p.ExitRule()
	if false {
		goto errorExit // Trick to prevent compiler error if the label is not used
	}
	return localctx
}

// ISampleQueryContext is an interface to support dynamic dispatch.
type ISampleQueryContext interface {
	antlr.ParserRuleContext

	// GetParser returns the parser.
	GetParser() antlr.Parser

	// Getter signatures
	FieldList() IFieldListContext
	AllSimpleQuery() []ISimpleQueryContext
	SimpleQuery(i int) ISimpleQueryContext

	// IsSampleQueryContext differentiates from other interfaces.
	IsSampleQueryContext()
}

type SampleQueryContext struct {
	antlr.BaseParserRuleContext
	parser antlr.Parser
}

func NewEmptySampleQueryContext() *SampleQueryContext {
	var p = new(SampleQueryContext)
	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, nil, -1)
	p.RuleIndex = EQLParserRULE_sampleQuery
	return p
}

func InitEmptySampleQueryContext(p *SampleQueryContext) {
	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, nil, -1)
	p.RuleIndex = EQLParserRULE_sampleQuery
}

func (*SampleQueryContext) IsSampleQueryContext() {}

func NewSampleQueryContext(parser antlr.Parser, parent antlr.ParserRuleContext, invokingState int) *SampleQueryContext {
	var p = new(SampleQueryContext)

	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, parent, invokingState)

	p.parser = parser
	p.RuleIndex = EQLParserRULE_sampleQuery

	return p
}

func (s *SampleQueryContext) GetParser() antlr.Parser { return s.parser }

func (s *SampleQueryContext) FieldList() IFieldListContext {
	var t antlr.RuleContext
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IFieldListContext); ok {
			t = ctx.(antlr.RuleContext)
			break
		}
	}

	if t == nil {
		return nil
	}

	return t.(IFieldListContext)
}

func (s *SampleQueryContext) AllSimpleQuery() []ISimpleQueryContext {
	children := s.GetChildren()
	len := 0
	for _, ctx := range children {
		if _, ok := ctx.(ISimpleQueryContext); ok {
			len++
		}
	}

	tst := make([]ISimpleQueryContext, len)
	i := 0
	for _, ctx := range children {
		if t, ok := ctx.(ISimpleQueryContext); ok {
			tst[i] = t.(ISimpleQueryContext)
			i++
		}
	}

	return tst
}

func (s *SampleQueryContext) SimpleQuery(i int) ISimpleQueryContext {
	var t antlr.RuleContext
	j := 0
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(ISimpleQueryContext); ok {
			if j == i {
				t = ctx.(antlr.RuleContext)
				break
			}
			j++
		}
	}

	if t == nil {
		return nil
	}

	return t.(ISimpleQueryContext)
}

func (s *SampleQueryContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *SampleQueryContext) ToStringTree(ruleNames []string, recog antlr.Recognizer) string {
	return antlr.TreesStringTree(s, ruleNames, recog)
}

func (s *SampleQueryContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(EQLListener); ok {
		listenerT.EnterSampleQuery(s)
	}
}

func (s *SampleQueryContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(EQLListener); ok {
		listenerT.ExitSampleQuery(s)
	}
}

func (s *SampleQueryContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case EQLVisitor:
		return t.VisitSampleQuery(s)

	default:
		return t.VisitChildren(s)
	}
}

func (p *EQLParser) SampleQuery() (localctx ISampleQueryContext) {
	localctx = NewSampleQueryContext(p, p.GetParserRuleContext(), p.GetState())
	p.EnterRule(localctx, 6, EQLParserRULE_sampleQuery)
	var _la int

	p.EnterOuterAlt(localctx, 1)
	{
		p.SetState(70)
		p.Match(EQLParserT__9)
		if p.HasError() {
			// Recognition error - abort rule
			goto errorExit
		}
	}
	{
		p.SetState(71)
		p.Match(EQLParserT__3)
		if p.HasError() {
			// Recognition error - abort rule
			goto errorExit
		}
	}
	{
		p.SetState(72)
		p.FieldList()
	}
	p.SetState(77)
	p.GetErrorHandler().Sync(p)
	if p.HasError() {
		goto errorExit
	}
	_la = p.GetTokenStream().LA(1)

	for ok := true; ok; ok = _la == EQLParserT__7 {
		{
			p.SetState(73)
			p.Match(EQLParserT__7)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(74)
			p.SimpleQuery()
		}
		{
			p.SetState(75)
			p.Match(EQLParserT__8)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}

		p.SetState(79)
		p.GetErrorHandler().Sync(p)
		if p.HasError() {
			goto errorExit
		}
		_la = p.GetTokenStream().LA(1)
	}

errorExit:
	if p.HasError() {
		v := p.GetError()
		localctx.SetException(v)
		p.GetErrorHandler().ReportError(p, v)
		p.GetErrorHandler().Recover(p, v)
		p.SetError(nil)
	}
	p.ExitRule()
	if false {
		goto errorExit // Trick to prevent compiler error if the label is not used
	}
	return localctx
}

// IConditionContext is an interface to support dynamic dispatch.
type IConditionContext interface {
	antlr.ParserRuleContext

	// GetParser returns the parser.
	GetParser() antlr.Parser
	// IsConditionContext differentiates from other interfaces.
	IsConditionContext()
}

type ConditionContext struct {
	antlr.BaseParserRuleContext
	parser antlr.Parser
}

func NewEmptyConditionContext() *ConditionContext {
	var p = new(ConditionContext)
	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, nil, -1)
	p.RuleIndex = EQLParserRULE_condition
	return p
}

func InitEmptyConditionContext(p *ConditionContext) {
	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, nil, -1)
	p.RuleIndex = EQLParserRULE_condition
}

func (*ConditionContext) IsConditionContext() {}

func NewConditionContext(parser antlr.Parser, parent antlr.ParserRuleContext, invokingState int) *ConditionContext {
	var p = new(ConditionContext)

	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, parent, invokingState)

	p.parser = parser
	p.RuleIndex = EQLParserRULE_condition

	return p
}

func (s *ConditionContext) GetParser() antlr.Parser { return s.parser }

func (s *ConditionContext) CopyAll(ctx *ConditionContext) {
	s.CopyFrom(&ctx.BaseParserRuleContext)
}

func (s *ConditionContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *ConditionContext) ToStringTree(ruleNames []string, recog antlr.Recognizer) string {
	return antlr.TreesStringTree(s, ruleNames, recog)
}

type LookupOpListContext struct {
	ConditionContext
	op   antlr.Token
	list ILiteralListContext
}

func NewLookupOpListContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *LookupOpListContext {
	var p = new(LookupOpListContext)

	InitEmptyConditionContext(&p.ConditionContext)
	p.parser = parser
	p.CopyAll(ctx.(*ConditionContext))

	return p
}

func (s *LookupOpListContext) GetOp() antlr.Token { return s.op }

func (s *LookupOpListContext) SetOp(v antlr.Token) { s.op = v }

func (s *LookupOpListContext) GetList() ILiteralListContext { return s.list }

func (s *LookupOpListContext) SetList(v ILiteralListContext) { s.list = v }

func (s *LookupOpListContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *LookupOpListContext) Field() IFieldContext {
	var t antlr.RuleContext
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IFieldContext); ok {
			t = ctx.(antlr.RuleContext)
			break
		}
	}

	if t == nil {
		return nil
	}

	return t.(IFieldContext)
}

func (s *LookupOpListContext) LiteralList() ILiteralListContext {
	var t antlr.RuleContext
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(ILiteralListContext); ok {
			t = ctx.(antlr.RuleContext)
			break
		}
	}

	if t == nil {
		return nil
	}

	return t.(ILiteralListContext)
}

func (s *LookupOpListContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(EQLListener); ok {
		listenerT.EnterLookupOpList(s)
	}
}

func (s *LookupOpListContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(EQLListener); ok {
		listenerT.ExitLookupOpList(s)
	}
}

func (s *LookupOpListContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case EQLVisitor:
		return t.VisitLookupOpList(s)

	default:
		return t.VisitChildren(s)
	}
}

type ComparisonOpContext struct {
	ConditionContext
	left  IValueContext
	op    antlr.Token
	right IValueContext
}

func NewComparisonOpContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *ComparisonOpContext {
	var p = new(ComparisonOpContext)

	InitEmptyConditionContext(&p.ConditionContext)
	p.parser = parser
	p.CopyAll(ctx.(*ConditionContext))

	return p
}

func (s *ComparisonOpContext) GetOp() antlr.Token { return s.op }

func (s *ComparisonOpContext) SetOp(v antlr.Token) { s.op = v }

func (s *ComparisonOpContext) GetLeft() IValueContext { return s.left }

func (s *ComparisonOpContext) GetRight() IValueContext { return s.right }

func (s *ComparisonOpContext) SetLeft(v IValueContext) { s.left = v }

func (s *ComparisonOpContext) SetRight(v IValueContext) { s.right = v }

func (s *ComparisonOpContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *ComparisonOpContext) AllValue() []IValueContext {
	children := s.GetChildren()
	len := 0
	for _, ctx := range children {
		if _, ok := ctx.(IValueContext); ok {
			len++
		}
	}

	tst := make([]IValueContext, len)
	i := 0
	for _, ctx := range children {
		if t, ok := ctx.(IValueContext); ok {
			tst[i] = t.(IValueContext)
			i++
		}
	}

	return tst
}

func (s *ComparisonOpContext) Value(i int) IValueContext {
	var t antlr.RuleContext
	j := 0
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IValueContext); ok {
			if j == i {
				t = ctx.(antlr.RuleContext)
				break
			}
			j++
		}
	}

	if t == nil {
		return nil
	}

	return t.(IValueContext)
}

func (s *ComparisonOpContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(EQLListener); ok {
		listenerT.EnterComparisonOp(s)
	}
}

func (s *ComparisonOpContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(EQLListener); ok {
		listenerT.ExitComparisonOp(s)
	}
}

func (s *ComparisonOpContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case EQLVisitor:
		return t.VisitComparisonOp(s)

	default:
		return t.VisitChildren(s)
	}
}

type ConditionNotFuncallContext struct {
	ConditionContext
}

func NewConditionNotFuncallContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *ConditionNotFuncallContext {
	var p = new(ConditionNotFuncallContext)

	InitEmptyConditionContext(&p.ConditionContext)
	p.parser = parser
	p.CopyAll(ctx.(*ConditionContext))

	return p
}

func (s *ConditionNotFuncallContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *ConditionNotFuncallContext) Funcall() IFuncallContext {
	var t antlr.RuleContext
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IFuncallContext); ok {
			t = ctx.(antlr.RuleContext)
			break
		}
	}

	if t == nil {
		return nil
	}

	return t.(IFuncallContext)
}

func (s *ConditionNotFuncallContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(EQLListener); ok {
		listenerT.EnterConditionNotFuncall(s)
	}
}

func (s *ConditionNotFuncallContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(EQLListener); ok {
		listenerT.ExitConditionNotFuncall(s)
	}
}

func (s *ConditionNotFuncallContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case EQLVisitor:
		return t.VisitConditionNotFuncall(s)

	default:
		return t.VisitChildren(s)
	}
}

type ConditionBooleanContext struct {
	ConditionContext
}

func NewConditionBooleanContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *ConditionBooleanContext {
	var p = new(ConditionBooleanContext)

	InitEmptyConditionContext(&p.ConditionContext)
	p.parser = parser
	p.CopyAll(ctx.(*ConditionContext))

	return p
}

func (s *ConditionBooleanContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *ConditionBooleanContext) BOOLEAN() antlr.TerminalNode {
	return s.GetToken(EQLParserBOOLEAN, 0)
}

func (s *ConditionBooleanContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(EQLListener); ok {
		listenerT.EnterConditionBoolean(s)
	}
}

func (s *ConditionBooleanContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(EQLListener); ok {
		listenerT.ExitConditionBoolean(s)
	}
}

func (s *ConditionBooleanContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case EQLVisitor:
		return t.VisitConditionBoolean(s)

	default:
		return t.VisitChildren(s)
	}
}

type ConditionNotContext struct {
	ConditionContext
}

func NewConditionNotContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *ConditionNotContext {
	var p = new(ConditionNotContext)

	InitEmptyConditionContext(&p.ConditionContext)
	p.parser = parser
	p.CopyAll(ctx.(*ConditionContext))

	return p
}

func (s *ConditionNotContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *ConditionNotContext) Condition() IConditionContext {
	var t antlr.RuleContext
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IConditionContext); ok {
			t = ctx.(antlr.RuleContext)
			break
		}
	}

	if t == nil {
		return nil
	}

	return t.(IConditionContext)
}

func (s *ConditionNotContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(EQLListener); ok {
		listenerT.EnterConditionNot(s)
	}
}

func (s *ConditionNotContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(EQLListener); ok {
		listenerT.ExitConditionNot(s)
	}
}

func (s *ConditionNotContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case EQLVisitor:
		return t.VisitConditionNot(s)

	default:
		return t.VisitChildren(s)
	}
}

type LookupNotOpListContext struct {
	ConditionContext
	op   antlr.Token
	list ILiteralListContext
}

func NewLookupNotOpListContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *LookupNotOpListContext {
	var p = new(LookupNotOpListContext)

	InitEmptyConditionContext(&p.ConditionContext)
	p.parser = parser
	p.CopyAll(ctx.(*ConditionContext))

	return p
}

func (s *LookupNotOpListContext) GetOp() antlr.Token { return s.op }

func (s *LookupNotOpListContext) SetOp(v antlr.Token) { s.op = v }

func (s *LookupNotOpListContext) GetList() ILiteralListContext { return s.list }

func (s *LookupNotOpListContext) SetList(v ILiteralListContext) { s.list = v }

func (s *LookupNotOpListContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *LookupNotOpListContext) Field() IFieldContext {
	var t antlr.RuleContext
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IFieldContext); ok {
			t = ctx.(antlr.RuleContext)
			break
		}
	}

	if t == nil {
		return nil
	}

	return t.(IFieldContext)
}

func (s *LookupNotOpListContext) LiteralList() ILiteralListContext {
	var t antlr.RuleContext
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(ILiteralListContext); ok {
			t = ctx.(antlr.RuleContext)
			break
		}
	}

	if t == nil {
		return nil
	}

	return t.(ILiteralListContext)
}

func (s *LookupNotOpListContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(EQLListener); ok {
		listenerT.EnterLookupNotOpList(s)
	}
}

func (s *LookupNotOpListContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(EQLListener); ok {
		listenerT.ExitLookupNotOpList(s)
	}
}

func (s *LookupNotOpListContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case EQLVisitor:
		return t.VisitLookupNotOpList(s)

	default:
		return t.VisitChildren(s)
	}
}

type ConditionLogicalOpContext struct {
	ConditionContext
	left  IConditionContext
	op    antlr.Token
	right IConditionContext
}

func NewConditionLogicalOpContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *ConditionLogicalOpContext {
	var p = new(ConditionLogicalOpContext)

	InitEmptyConditionContext(&p.ConditionContext)
	p.parser = parser
	p.CopyAll(ctx.(*ConditionContext))

	return p
}

func (s *ConditionLogicalOpContext) GetOp() antlr.Token { return s.op }

func (s *ConditionLogicalOpContext) SetOp(v antlr.Token) { s.op = v }

func (s *ConditionLogicalOpContext) GetLeft() IConditionContext { return s.left }

func (s *ConditionLogicalOpContext) GetRight() IConditionContext { return s.right }

func (s *ConditionLogicalOpContext) SetLeft(v IConditionContext) { s.left = v }

func (s *ConditionLogicalOpContext) SetRight(v IConditionContext) { s.right = v }

func (s *ConditionLogicalOpContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *ConditionLogicalOpContext) AllCondition() []IConditionContext {
	children := s.GetChildren()
	len := 0
	for _, ctx := range children {
		if _, ok := ctx.(IConditionContext); ok {
			len++
		}
	}

	tst := make([]IConditionContext, len)
	i := 0
	for _, ctx := range children {
		if t, ok := ctx.(IConditionContext); ok {
			tst[i] = t.(IConditionContext)
			i++
		}
	}

	return tst
}

func (s *ConditionLogicalOpContext) Condition(i int) IConditionContext {
	var t antlr.RuleContext
	j := 0
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IConditionContext); ok {
			if j == i {
				t = ctx.(antlr.RuleContext)
				break
			}
			j++
		}
	}

	if t == nil {
		return nil
	}

	return t.(IConditionContext)
}

func (s *ConditionLogicalOpContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(EQLListener); ok {
		listenerT.EnterConditionLogicalOp(s)
	}
}

func (s *ConditionLogicalOpContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(EQLListener); ok {
		listenerT.ExitConditionLogicalOp(s)
	}
}

func (s *ConditionLogicalOpContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case EQLVisitor:
		return t.VisitConditionLogicalOp(s)

	default:
		return t.VisitChildren(s)
	}
}

type ConditionGroupContext struct {
	ConditionContext
}

func NewConditionGroupContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *ConditionGroupContext {
	var p = new(ConditionGroupContext)

	InitEmptyConditionContext(&p.ConditionContext)
	p.parser = parser
	p.CopyAll(ctx.(*ConditionContext))

	return p
}

func (s *ConditionGroupContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *ConditionGroupContext) Condition() IConditionContext {
	var t antlr.RuleContext
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IConditionContext); ok {
			t = ctx.(antlr.RuleContext)
			break
		}
	}

	if t == nil {
		return nil
	}

	return t.(IConditionContext)
}

func (s *ConditionGroupContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(EQLListener); ok {
		listenerT.EnterConditionGroup(s)
	}
}

func (s *ConditionGroupContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(EQLListener); ok {
		listenerT.ExitConditionGroup(s)
	}
}

func (s *ConditionGroupContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case EQLVisitor:
		return t.VisitConditionGroup(s)

	default:
		return t.VisitChildren(s)
	}
}

type ConditionFuncallContext struct {
	ConditionContext
}

func NewConditionFuncallContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *ConditionFuncallContext {
	var p = new(ConditionFuncallContext)

	InitEmptyConditionContext(&p.ConditionContext)
	p.parser = parser
	p.CopyAll(ctx.(*ConditionContext))

	return p
}

func (s *ConditionFuncallContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *ConditionFuncallContext) Funcall() IFuncallContext {
	var t antlr.RuleContext
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IFuncallContext); ok {
			t = ctx.(antlr.RuleContext)
			break
		}
	}

	if t == nil {
		return nil
	}

	return t.(IFuncallContext)
}

func (s *ConditionFuncallContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(EQLListener); ok {
		listenerT.EnterConditionFuncall(s)
	}
}

func (s *ConditionFuncallContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(EQLListener); ok {
		listenerT.ExitConditionFuncall(s)
	}
}

func (s *ConditionFuncallContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case EQLVisitor:
		return t.VisitConditionFuncall(s)

	default:
		return t.VisitChildren(s)
	}
}

func (p *EQLParser) Condition() (localctx IConditionContext) {
	return p.condition(0)
}

func (p *EQLParser) condition(_p int) (localctx IConditionContext) {
	var _parentctx antlr.ParserRuleContext = p.GetParserRuleContext()

	_parentState := p.GetState()
	localctx = NewConditionContext(p, p.GetParserRuleContext(), _parentState)
	var _prevctx IConditionContext = localctx
	var _ antlr.ParserRuleContext = _prevctx // TODO: To prevent unused variable warning.
	_startState := 8
	p.EnterRecursionRule(localctx, 8, EQLParserRULE_condition, _p)
	var _la int

	var _alt int

	p.EnterOuterAlt(localctx, 1)
	p.SetState(105)
	p.GetErrorHandler().Sync(p)
	if p.HasError() {
		goto errorExit
	}

	switch p.GetInterpreter().AdaptivePredict(p.BaseParser, p.GetTokenStream(), 7, p.GetParserRuleContext()) {
	case 1:
		localctx = NewConditionBooleanContext(p, localctx)
		p.SetParserRuleContext(localctx)
		_prevctx = localctx

		{
			p.SetState(82)
			p.Match(EQLParserBOOLEAN)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}

	case 2:
		localctx = NewConditionNotContext(p, localctx)
		p.SetParserRuleContext(localctx)
		_prevctx = localctx
		{
			p.SetState(83)
			p.Match(EQLParserT__10)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(84)
			p.condition(8)
		}

	case 3:
		localctx = NewConditionGroupContext(p, localctx)
		p.SetParserRuleContext(localctx)
		_prevctx = localctx
		{
			p.SetState(85)
			p.Match(EQLParserT__11)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(86)
			p.condition(0)
		}
		{
			p.SetState(87)
			p.Match(EQLParserT__12)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}

	case 4:
		localctx = NewComparisonOpContext(p, localctx)
		p.SetParserRuleContext(localctx)
		_prevctx = localctx
		{
			p.SetState(89)

			var _x = p.value(0)

			localctx.(*ComparisonOpContext).left = _x
		}
		{
			p.SetState(90)

			var _lt = p.GetTokenStream().LT(1)

			localctx.(*ComparisonOpContext).op = _lt

			_la = p.GetTokenStream().LA(1)

			if !((int64(_la) & ^0x3f) == 0 && ((int64(1)<<_la)&33538048) != 0) {
				var _ri = p.GetErrorHandler().RecoverInline(p)

				localctx.(*ComparisonOpContext).op = _ri
			} else {
				p.GetErrorHandler().ReportMatch(p)
				p.Consume()
			}
		}
		{
			p.SetState(91)

			var _x = p.value(0)

			localctx.(*ComparisonOpContext).right = _x
		}

	case 5:
		localctx = NewLookupNotOpListContext(p, localctx)
		p.SetParserRuleContext(localctx)
		_prevctx = localctx
		{
			p.SetState(93)
			p.Field()
		}
		{
			p.SetState(94)
			p.Match(EQLParserT__10)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(95)

			var _lt = p.GetTokenStream().LT(1)

			localctx.(*LookupNotOpListContext).op = _lt

			_la = p.GetTokenStream().LA(1)

			if !(_la == EQLParserT__24 || _la == EQLParserT__25) {
				var _ri = p.GetErrorHandler().RecoverInline(p)

				localctx.(*LookupNotOpListContext).op = _ri
			} else {
				p.GetErrorHandler().ReportMatch(p)
				p.Consume()
			}
		}
		{
			p.SetState(96)

			var _x = p.LiteralList()

			localctx.(*LookupNotOpListContext).list = _x
		}

	case 6:
		localctx = NewLookupOpListContext(p, localctx)
		p.SetParserRuleContext(localctx)
		_prevctx = localctx
		{
			p.SetState(98)
			p.Field()
		}
		{
			p.SetState(99)

			var _lt = p.GetTokenStream().LT(1)

			localctx.(*LookupOpListContext).op = _lt

			_la = p.GetTokenStream().LA(1)

			if !((int64(_la) & ^0x3f) == 0 && ((int64(1)<<_la)&133169152) != 0) {
				var _ri = p.GetErrorHandler().RecoverInline(p)

				localctx.(*LookupOpListContext).op = _ri
			} else {
				p.GetErrorHandler().ReportMatch(p)
				p.Consume()
			}
		}
		{
			p.SetState(100)

			var _x = p.LiteralList()

			localctx.(*LookupOpListContext).list = _x
		}

	case 7:
		localctx = NewConditionFuncallContext(p, localctx)
		p.SetParserRuleContext(localctx)
		_prevctx = localctx
		{
			p.SetState(102)
			p.Funcall()
		}

	case 8:
		localctx = NewConditionNotFuncallContext(p, localctx)
		p.SetParserRuleContext(localctx)
		_prevctx = localctx
		{
			p.SetState(103)
			p.Match(EQLParserT__10)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(104)
			p.Funcall()
		}

	case antlr.ATNInvalidAltNumber:
		goto errorExit
	}
	p.GetParserRuleContext().SetStop(p.GetTokenStream().LT(-1))
	p.SetState(112)
	p.GetErrorHandler().Sync(p)
	if p.HasError() {
		goto errorExit
	}
	_alt = p.GetInterpreter().AdaptivePredict(p.BaseParser, p.GetTokenStream(), 8, p.GetParserRuleContext())
	if p.HasError() {
		goto errorExit
	}
	for _alt != 2 && _alt != antlr.ATNInvalidAltNumber {
		if _alt == 1 {
			if p.GetParseListeners() != nil {
				p.TriggerExitRuleEvent()
			}
			_prevctx = localctx
			localctx = NewConditionLogicalOpContext(p, NewConditionContext(p, _parentctx, _parentState))
			localctx.(*ConditionLogicalOpContext).left = _prevctx

			p.PushNewRecursionContext(localctx, _startState, EQLParserRULE_condition)
			p.SetState(107)

			if !(p.Precpred(p.GetParserRuleContext(), 3)) {
				p.SetError(antlr.NewFailedPredicateException(p, "p.Precpred(p.GetParserRuleContext(), 3)", ""))
				goto errorExit
			}
			{
				p.SetState(108)

				var _lt = p.GetTokenStream().LT(1)

				localctx.(*ConditionLogicalOpContext).op = _lt

				_la = p.GetTokenStream().LA(1)

				if !(_la == EQLParserT__26 || _la == EQLParserT__27) {
					var _ri = p.GetErrorHandler().RecoverInline(p)

					localctx.(*ConditionLogicalOpContext).op = _ri
				} else {
					p.GetErrorHandler().ReportMatch(p)
					p.Consume()
				}
			}
			{
				p.SetState(109)

				var _x = p.condition(4)

				localctx.(*ConditionLogicalOpContext).right = _x
			}

		}
		p.SetState(114)
		p.GetErrorHandler().Sync(p)
		if p.HasError() {
			goto errorExit
		}
		_alt = p.GetInterpreter().AdaptivePredict(p.BaseParser, p.GetTokenStream(), 8, p.GetParserRuleContext())
		if p.HasError() {
			goto errorExit
		}
	}

errorExit:
	if p.HasError() {
		v := p.GetError()
		localctx.SetException(v)
		p.GetErrorHandler().ReportError(p, v)
		p.GetErrorHandler().Recover(p, v)
		p.SetError(nil)
	}
	p.UnrollRecursionContexts(_parentctx)
	if false {
		goto errorExit // Trick to prevent compiler error if the label is not used
	}
	return localctx
}

// ICategoryContext is an interface to support dynamic dispatch.
type ICategoryContext interface {
	antlr.ParserRuleContext

	// GetParser returns the parser.
	GetParser() antlr.Parser

	// Getter signatures
	ANY() antlr.TerminalNode
	ID() antlr.TerminalNode
	STRING() antlr.TerminalNode

	// IsCategoryContext differentiates from other interfaces.
	IsCategoryContext()
}

type CategoryContext struct {
	antlr.BaseParserRuleContext
	parser antlr.Parser
}

func NewEmptyCategoryContext() *CategoryContext {
	var p = new(CategoryContext)
	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, nil, -1)
	p.RuleIndex = EQLParserRULE_category
	return p
}

func InitEmptyCategoryContext(p *CategoryContext) {
	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, nil, -1)
	p.RuleIndex = EQLParserRULE_category
}

func (*CategoryContext) IsCategoryContext() {}

func NewCategoryContext(parser antlr.Parser, parent antlr.ParserRuleContext, invokingState int) *CategoryContext {
	var p = new(CategoryContext)

	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, parent, invokingState)

	p.parser = parser
	p.RuleIndex = EQLParserRULE_category

	return p
}

func (s *CategoryContext) GetParser() antlr.Parser { return s.parser }

func (s *CategoryContext) ANY() antlr.TerminalNode {
	return s.GetToken(EQLParserANY, 0)
}

func (s *CategoryContext) ID() antlr.TerminalNode {
	return s.GetToken(EQLParserID, 0)
}

func (s *CategoryContext) STRING() antlr.TerminalNode {
	return s.GetToken(EQLParserSTRING, 0)
}

func (s *CategoryContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *CategoryContext) ToStringTree(ruleNames []string, recog antlr.Recognizer) string {
	return antlr.TreesStringTree(s, ruleNames, recog)
}

func (s *CategoryContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(EQLListener); ok {
		listenerT.EnterCategory(s)
	}
}

func (s *CategoryContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(EQLListener); ok {
		listenerT.ExitCategory(s)
	}
}

func (s *CategoryContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case EQLVisitor:
		return t.VisitCategory(s)

	default:
		return t.VisitChildren(s)
	}
}

func (p *EQLParser) Category() (localctx ICategoryContext) {
	localctx = NewCategoryContext(p, p.GetParserRuleContext(), p.GetState())
	p.EnterRule(localctx, 10, EQLParserRULE_category)
	var _la int

	p.EnterOuterAlt(localctx, 1)
	{
		p.SetState(115)
		_la = p.GetTokenStream().LA(1)

		if !((int64((_la-63)) & ^0x3f) == 0 && ((int64(1)<<(_la-63))&641) != 0) {
			p.GetErrorHandler().RecoverInline(p)
		} else {
			p.GetErrorHandler().ReportMatch(p)
			p.Consume()
		}
	}

errorExit:
	if p.HasError() {
		v := p.GetError()
		localctx.SetException(v)
		p.GetErrorHandler().ReportError(p, v)
		p.GetErrorHandler().Recover(p, v)
		p.SetError(nil)
	}
	p.ExitRule()
	if false {
		goto errorExit // Trick to prevent compiler error if the label is not used
	}
	return localctx
}

// IFieldContext is an interface to support dynamic dispatch.
type IFieldContext interface {
	antlr.ParserRuleContext

	// GetParser returns the parser.
	GetParser() antlr.Parser

	// Getter signatures
	ID() antlr.TerminalNode

	// IsFieldContext differentiates from other interfaces.
	IsFieldContext()
}

type FieldContext struct {
	antlr.BaseParserRuleContext
	parser antlr.Parser
}

func NewEmptyFieldContext() *FieldContext {
	var p = new(FieldContext)
	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, nil, -1)
	p.RuleIndex = EQLParserRULE_field
	return p
}

func InitEmptyFieldContext(p *FieldContext) {
	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, nil, -1)
	p.RuleIndex = EQLParserRULE_field
}

func (*FieldContext) IsFieldContext() {}

func NewFieldContext(parser antlr.Parser, parent antlr.ParserRuleContext, invokingState int) *FieldContext {
	var p = new(FieldContext)

	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, parent, invokingState)

	p.parser = parser
	p.RuleIndex = EQLParserRULE_field

	return p
}

func (s *FieldContext) GetParser() antlr.Parser { return s.parser }

func (s *FieldContext) ID() antlr.TerminalNode {
	return s.GetToken(EQLParserID, 0)
}

func (s *FieldContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *FieldContext) ToStringTree(ruleNames []string, recog antlr.Recognizer) string {
	return antlr.TreesStringTree(s, ruleNames, recog)
}

func (s *FieldContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(EQLListener); ok {
		listenerT.EnterField(s)
	}
}

func (s *FieldContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(EQLListener); ok {
		listenerT.ExitField(s)
	}
}

func (s *FieldContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case EQLVisitor:
		return t.VisitField(s)

	default:
		return t.VisitChildren(s)
	}
}

func (p *EQLParser) Field() (localctx IFieldContext) {
	localctx = NewFieldContext(p, p.GetParserRuleContext(), p.GetState())
	p.EnterRule(localctx, 12, EQLParserRULE_field)
	p.SetState(120)
	p.GetErrorHandler().Sync(p)
	if p.HasError() {
		goto errorExit
	}

	switch p.GetTokenStream().LA(1) {
	case EQLParserID:
		p.EnterOuterAlt(localctx, 1)
		{
			p.SetState(117)
			p.Match(EQLParserID)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}

	case EQLParserT__28:
		p.EnterOuterAlt(localctx, 2)
		{
			p.SetState(118)
			p.Match(EQLParserT__28)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(119)
			p.Match(EQLParserID)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}

	default:
		p.SetError(antlr.NewNoViableAltException(p, nil, nil, nil, nil, nil))
		goto errorExit
	}

errorExit:
	if p.HasError() {
		v := p.GetError()
		localctx.SetException(v)
		p.GetErrorHandler().ReportError(p, v)
		p.GetErrorHandler().Recover(p, v)
		p.SetError(nil)
	}
	p.ExitRule()
	if false {
		goto errorExit // Trick to prevent compiler error if the label is not used
	}
	return localctx
}

// IFieldListContext is an interface to support dynamic dispatch.
type IFieldListContext interface {
	antlr.ParserRuleContext

	// GetParser returns the parser.
	GetParser() antlr.Parser

	// Getter signatures
	AllField() []IFieldContext
	Field(i int) IFieldContext

	// IsFieldListContext differentiates from other interfaces.
	IsFieldListContext()
}

type FieldListContext struct {
	antlr.BaseParserRuleContext
	parser antlr.Parser
}

func NewEmptyFieldListContext() *FieldListContext {
	var p = new(FieldListContext)
	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, nil, -1)
	p.RuleIndex = EQLParserRULE_fieldList
	return p
}

func InitEmptyFieldListContext(p *FieldListContext) {
	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, nil, -1)
	p.RuleIndex = EQLParserRULE_fieldList
}

func (*FieldListContext) IsFieldListContext() {}

func NewFieldListContext(parser antlr.Parser, parent antlr.ParserRuleContext, invokingState int) *FieldListContext {
	var p = new(FieldListContext)

	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, parent, invokingState)

	p.parser = parser
	p.RuleIndex = EQLParserRULE_fieldList

	return p
}

func (s *FieldListContext) GetParser() antlr.Parser { return s.parser }

func (s *FieldListContext) AllField() []IFieldContext {
	children := s.GetChildren()
	len := 0
	for _, ctx := range children {
		if _, ok := ctx.(IFieldContext); ok {
			len++
		}
	}

	tst := make([]IFieldContext, len)
	i := 0
	for _, ctx := range children {
		if t, ok := ctx.(IFieldContext); ok {
			tst[i] = t.(IFieldContext)
			i++
		}
	}

	return tst
}

func (s *FieldListContext) Field(i int) IFieldContext {
	var t antlr.RuleContext
	j := 0
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IFieldContext); ok {
			if j == i {
				t = ctx.(antlr.RuleContext)
				break
			}
			j++
		}
	}

	if t == nil {
		return nil
	}

	return t.(IFieldContext)
}

func (s *FieldListContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *FieldListContext) ToStringTree(ruleNames []string, recog antlr.Recognizer) string {
	return antlr.TreesStringTree(s, ruleNames, recog)
}

func (s *FieldListContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(EQLListener); ok {
		listenerT.EnterFieldList(s)
	}
}

func (s *FieldListContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(EQLListener); ok {
		listenerT.ExitFieldList(s)
	}
}

func (s *FieldListContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case EQLVisitor:
		return t.VisitFieldList(s)

	default:
		return t.VisitChildren(s)
	}
}

func (p *EQLParser) FieldList() (localctx IFieldListContext) {
	localctx = NewFieldListContext(p, p.GetParserRuleContext(), p.GetState())
	p.EnterRule(localctx, 14, EQLParserRULE_fieldList)
	var _la int

	p.EnterOuterAlt(localctx, 1)
	{
		p.SetState(122)
		p.Field()
	}
	p.SetState(127)
	p.GetErrorHandler().Sync(p)
	if p.HasError() {
		goto errorExit
	}
	_la = p.GetTokenStream().LA(1)

	for _la == EQLParserT__29 {
		{
			p.SetState(123)
			p.Match(EQLParserT__29)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(124)
			p.Field()
		}

		p.SetState(129)
		p.GetErrorHandler().Sync(p)
		if p.HasError() {
			goto errorExit
		}
		_la = p.GetTokenStream().LA(1)
	}

errorExit:
	if p.HasError() {
		v := p.GetError()
		localctx.SetException(v)
		p.GetErrorHandler().ReportError(p, v)
		p.GetErrorHandler().Recover(p, v)
		p.SetError(nil)
	}
	p.ExitRule()
	if false {
		goto errorExit // Trick to prevent compiler error if the label is not used
	}
	return localctx
}

// ILiteralContext is an interface to support dynamic dispatch.
type ILiteralContext interface {
	antlr.ParserRuleContext

	// GetParser returns the parser.
	GetParser() antlr.Parser

	// Getter signatures
	STRING() antlr.TerminalNode
	NUMBER() antlr.TerminalNode
	BOOLEAN() antlr.TerminalNode

	// IsLiteralContext differentiates from other interfaces.
	IsLiteralContext()
}

type LiteralContext struct {
	antlr.BaseParserRuleContext
	parser antlr.Parser
}

func NewEmptyLiteralContext() *LiteralContext {
	var p = new(LiteralContext)
	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, nil, -1)
	p.RuleIndex = EQLParserRULE_literal
	return p
}

func InitEmptyLiteralContext(p *LiteralContext) {
	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, nil, -1)
	p.RuleIndex = EQLParserRULE_literal
}

func (*LiteralContext) IsLiteralContext() {}

func NewLiteralContext(parser antlr.Parser, parent antlr.ParserRuleContext, invokingState int) *LiteralContext {
	var p = new(LiteralContext)

	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, parent, invokingState)

	p.parser = parser
	p.RuleIndex = EQLParserRULE_literal

	return p
}

func (s *LiteralContext) GetParser() antlr.Parser { return s.parser }

func (s *LiteralContext) STRING() antlr.TerminalNode {
	return s.GetToken(EQLParserSTRING, 0)
}

func (s *LiteralContext) NUMBER() antlr.TerminalNode {
	return s.GetToken(EQLParserNUMBER, 0)
}

func (s *LiteralContext) BOOLEAN() antlr.TerminalNode {
	return s.GetToken(EQLParserBOOLEAN, 0)
}

func (s *LiteralContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *LiteralContext) ToStringTree(ruleNames []string, recog antlr.Recognizer) string {
	return antlr.TreesStringTree(s, ruleNames, recog)
}

func (s *LiteralContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(EQLListener); ok {
		listenerT.EnterLiteral(s)
	}
}

func (s *LiteralContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(EQLListener); ok {
		listenerT.ExitLiteral(s)
	}
}

func (s *LiteralContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case EQLVisitor:
		return t.VisitLiteral(s)

	default:
		return t.VisitChildren(s)
	}
}

func (p *EQLParser) Literal() (localctx ILiteralContext) {
	localctx = NewLiteralContext(p, p.GetParserRuleContext(), p.GetState())
	p.EnterRule(localctx, 16, EQLParserRULE_literal)
	var _la int

	p.EnterOuterAlt(localctx, 1)
	{
		p.SetState(130)
		_la = p.GetTokenStream().LA(1)

		if !((int64((_la-66)) & ^0x3f) == 0 && ((int64(1)<<(_la-66))&21) != 0) {
			p.GetErrorHandler().RecoverInline(p)
		} else {
			p.GetErrorHandler().ReportMatch(p)
			p.Consume()
		}
	}

errorExit:
	if p.HasError() {
		v := p.GetError()
		localctx.SetException(v)
		p.GetErrorHandler().ReportError(p, v)
		p.GetErrorHandler().Recover(p, v)
		p.SetError(nil)
	}
	p.ExitRule()
	if false {
		goto errorExit // Trick to prevent compiler error if the label is not used
	}
	return localctx
}

// ILiteralListContext is an interface to support dynamic dispatch.
type ILiteralListContext interface {
	antlr.ParserRuleContext

	// GetParser returns the parser.
	GetParser() antlr.Parser

	// Getter signatures
	AllLiteral() []ILiteralContext
	Literal(i int) ILiteralContext

	// IsLiteralListContext differentiates from other interfaces.
	IsLiteralListContext()
}

type LiteralListContext struct {
	antlr.BaseParserRuleContext
	parser antlr.Parser
}

func NewEmptyLiteralListContext() *LiteralListContext {
	var p = new(LiteralListContext)
	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, nil, -1)
	p.RuleIndex = EQLParserRULE_literalList
	return p
}

func InitEmptyLiteralListContext(p *LiteralListContext) {
	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, nil, -1)
	p.RuleIndex = EQLParserRULE_literalList
}

func (*LiteralListContext) IsLiteralListContext() {}

func NewLiteralListContext(parser antlr.Parser, parent antlr.ParserRuleContext, invokingState int) *LiteralListContext {
	var p = new(LiteralListContext)

	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, parent, invokingState)

	p.parser = parser
	p.RuleIndex = EQLParserRULE_literalList

	return p
}

func (s *LiteralListContext) GetParser() antlr.Parser { return s.parser }

func (s *LiteralListContext) AllLiteral() []ILiteralContext {
	children := s.GetChildren()
	len := 0
	for _, ctx := range children {
		if _, ok := ctx.(ILiteralContext); ok {
			len++
		}
	}

	tst := make([]ILiteralContext, len)
	i := 0
	for _, ctx := range children {
		if t, ok := ctx.(ILiteralContext); ok {
			tst[i] = t.(ILiteralContext)
			i++
		}
	}

	return tst
}

func (s *LiteralListContext) Literal(i int) ILiteralContext {
	var t antlr.RuleContext
	j := 0
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(ILiteralContext); ok {
			if j == i {
				t = ctx.(antlr.RuleContext)
				break
			}
			j++
		}
	}

	if t == nil {
		return nil
	}

	return t.(ILiteralContext)
}

func (s *LiteralListContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *LiteralListContext) ToStringTree(ruleNames []string, recog antlr.Recognizer) string {
	return antlr.TreesStringTree(s, ruleNames, recog)
}

func (s *LiteralListContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(EQLListener); ok {
		listenerT.EnterLiteralList(s)
	}
}

func (s *LiteralListContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(EQLListener); ok {
		listenerT.ExitLiteralList(s)
	}
}

func (s *LiteralListContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case EQLVisitor:
		return t.VisitLiteralList(s)

	default:
		return t.VisitChildren(s)
	}
}

func (p *EQLParser) LiteralList() (localctx ILiteralListContext) {
	localctx = NewLiteralListContext(p, p.GetParserRuleContext(), p.GetState())
	p.EnterRule(localctx, 18, EQLParserRULE_literalList)
	var _la int

	p.EnterOuterAlt(localctx, 1)
	{
		p.SetState(132)
		p.Match(EQLParserT__11)
		if p.HasError() {
			// Recognition error - abort rule
			goto errorExit
		}
	}
	{
		p.SetState(133)
		p.Literal()
	}
	p.SetState(138)
	p.GetErrorHandler().Sync(p)
	if p.HasError() {
		goto errorExit
	}
	_la = p.GetTokenStream().LA(1)

	for _la == EQLParserT__29 {
		{
			p.SetState(134)
			p.Match(EQLParserT__29)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(135)
			p.Literal()
		}

		p.SetState(140)
		p.GetErrorHandler().Sync(p)
		if p.HasError() {
			goto errorExit
		}
		_la = p.GetTokenStream().LA(1)
	}
	{
		p.SetState(141)
		p.Match(EQLParserT__12)
		if p.HasError() {
			// Recognition error - abort rule
			goto errorExit
		}
	}

errorExit:
	if p.HasError() {
		v := p.GetError()
		localctx.SetException(v)
		p.GetErrorHandler().ReportError(p, v)
		p.GetErrorHandler().Recover(p, v)
		p.SetError(nil)
	}
	p.ExitRule()
	if false {
		goto errorExit // Trick to prevent compiler error if the label is not used
	}
	return localctx
}

// IValueContext is an interface to support dynamic dispatch.
type IValueContext interface {
	antlr.ParserRuleContext

	// GetParser returns the parser.
	GetParser() antlr.Parser
	// IsValueContext differentiates from other interfaces.
	IsValueContext()
}

type ValueContext struct {
	antlr.BaseParserRuleContext
	parser antlr.Parser
}

func NewEmptyValueContext() *ValueContext {
	var p = new(ValueContext)
	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, nil, -1)
	p.RuleIndex = EQLParserRULE_value
	return p
}

func InitEmptyValueContext(p *ValueContext) {
	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, nil, -1)
	p.RuleIndex = EQLParserRULE_value
}

func (*ValueContext) IsValueContext() {}

func NewValueContext(parser antlr.Parser, parent antlr.ParserRuleContext, invokingState int) *ValueContext {
	var p = new(ValueContext)

	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, parent, invokingState)

	p.parser = parser
	p.RuleIndex = EQLParserRULE_value

	return p
}

func (s *ValueContext) GetParser() antlr.Parser { return s.parser }

func (s *ValueContext) CopyAll(ctx *ValueContext) {
	s.CopyFrom(&ctx.BaseParserRuleContext)
}

func (s *ValueContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *ValueContext) ToStringTree(ruleNames []string, recog antlr.Recognizer) string {
	return antlr.TreesStringTree(s, ruleNames, recog)
}

type ValueAddSubContext struct {
	ValueContext
	left  IValueContext
	op    antlr.Token
	right IValueContext
}

func NewValueAddSubContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *ValueAddSubContext {
	var p = new(ValueAddSubContext)

	InitEmptyValueContext(&p.ValueContext)
	p.parser = parser
	p.CopyAll(ctx.(*ValueContext))

	return p
}

func (s *ValueAddSubContext) GetOp() antlr.Token { return s.op }

func (s *ValueAddSubContext) SetOp(v antlr.Token) { s.op = v }

func (s *ValueAddSubContext) GetLeft() IValueContext { return s.left }

func (s *ValueAddSubContext) GetRight() IValueContext { return s.right }

func (s *ValueAddSubContext) SetLeft(v IValueContext) { s.left = v }

func (s *ValueAddSubContext) SetRight(v IValueContext) { s.right = v }

func (s *ValueAddSubContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *ValueAddSubContext) AllValue() []IValueContext {
	children := s.GetChildren()
	len := 0
	for _, ctx := range children {
		if _, ok := ctx.(IValueContext); ok {
			len++
		}
	}

	tst := make([]IValueContext, len)
	i := 0
	for _, ctx := range children {
		if t, ok := ctx.(IValueContext); ok {
			tst[i] = t.(IValueContext)
			i++
		}
	}

	return tst
}

func (s *ValueAddSubContext) Value(i int) IValueContext {
	var t antlr.RuleContext
	j := 0
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IValueContext); ok {
			if j == i {
				t = ctx.(antlr.RuleContext)
				break
			}
			j++
		}
	}

	if t == nil {
		return nil
	}

	return t.(IValueContext)
}

func (s *ValueAddSubContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(EQLListener); ok {
		listenerT.EnterValueAddSub(s)
	}
}

func (s *ValueAddSubContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(EQLListener); ok {
		listenerT.ExitValueAddSub(s)
	}
}

func (s *ValueAddSubContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case EQLVisitor:
		return t.VisitValueAddSub(s)

	default:
		return t.VisitChildren(s)
	}
}

type ValueNullContext struct {
	ValueContext
}

func NewValueNullContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *ValueNullContext {
	var p = new(ValueNullContext)

	InitEmptyValueContext(&p.ValueContext)
	p.parser = parser
	p.CopyAll(ctx.(*ValueContext))

	return p
}

func (s *ValueNullContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *ValueNullContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(EQLListener); ok {
		listenerT.EnterValueNull(s)
	}
}

func (s *ValueNullContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(EQLListener); ok {
		listenerT.ExitValueNull(s)
	}
}

func (s *ValueNullContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case EQLVisitor:
		return t.VisitValueNull(s)

	default:
		return t.VisitChildren(s)
	}
}

type ValueMulDivContext struct {
	ValueContext
	left  IValueContext
	op    antlr.Token
	right IValueContext
}

func NewValueMulDivContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *ValueMulDivContext {
	var p = new(ValueMulDivContext)

	InitEmptyValueContext(&p.ValueContext)
	p.parser = parser
	p.CopyAll(ctx.(*ValueContext))

	return p
}

func (s *ValueMulDivContext) GetOp() antlr.Token { return s.op }

func (s *ValueMulDivContext) SetOp(v antlr.Token) { s.op = v }

func (s *ValueMulDivContext) GetLeft() IValueContext { return s.left }

func (s *ValueMulDivContext) GetRight() IValueContext { return s.right }

func (s *ValueMulDivContext) SetLeft(v IValueContext) { s.left = v }

func (s *ValueMulDivContext) SetRight(v IValueContext) { s.right = v }

func (s *ValueMulDivContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *ValueMulDivContext) AllValue() []IValueContext {
	children := s.GetChildren()
	len := 0
	for _, ctx := range children {
		if _, ok := ctx.(IValueContext); ok {
			len++
		}
	}

	tst := make([]IValueContext, len)
	i := 0
	for _, ctx := range children {
		if t, ok := ctx.(IValueContext); ok {
			tst[i] = t.(IValueContext)
			i++
		}
	}

	return tst
}

func (s *ValueMulDivContext) Value(i int) IValueContext {
	var t antlr.RuleContext
	j := 0
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IValueContext); ok {
			if j == i {
				t = ctx.(antlr.RuleContext)
				break
			}
			j++
		}
	}

	if t == nil {
		return nil
	}

	return t.(IValueContext)
}

func (s *ValueMulDivContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(EQLListener); ok {
		listenerT.EnterValueMulDiv(s)
	}
}

func (s *ValueMulDivContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(EQLListener); ok {
		listenerT.ExitValueMulDiv(s)
	}
}

func (s *ValueMulDivContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case EQLVisitor:
		return t.VisitValueMulDiv(s)

	default:
		return t.VisitChildren(s)
	}
}

type ValueGroupContext struct {
	ValueContext
}

func NewValueGroupContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *ValueGroupContext {
	var p = new(ValueGroupContext)

	InitEmptyValueContext(&p.ValueContext)
	p.parser = parser
	p.CopyAll(ctx.(*ValueContext))

	return p
}

func (s *ValueGroupContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *ValueGroupContext) Value() IValueContext {
	var t antlr.RuleContext
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IValueContext); ok {
			t = ctx.(antlr.RuleContext)
			break
		}
	}

	if t == nil {
		return nil
	}

	return t.(IValueContext)
}

func (s *ValueGroupContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(EQLListener); ok {
		listenerT.EnterValueGroup(s)
	}
}

func (s *ValueGroupContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(EQLListener); ok {
		listenerT.ExitValueGroup(s)
	}
}

func (s *ValueGroupContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case EQLVisitor:
		return t.VisitValueGroup(s)

	default:
		return t.VisitChildren(s)
	}
}

type ValueLiteralContext struct {
	ValueContext
}

func NewValueLiteralContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *ValueLiteralContext {
	var p = new(ValueLiteralContext)

	InitEmptyValueContext(&p.ValueContext)
	p.parser = parser
	p.CopyAll(ctx.(*ValueContext))

	return p
}

func (s *ValueLiteralContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *ValueLiteralContext) Literal() ILiteralContext {
	var t antlr.RuleContext
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(ILiteralContext); ok {
			t = ctx.(antlr.RuleContext)
			break
		}
	}

	if t == nil {
		return nil
	}

	return t.(ILiteralContext)
}

func (s *ValueLiteralContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(EQLListener); ok {
		listenerT.EnterValueLiteral(s)
	}
}

func (s *ValueLiteralContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(EQLListener); ok {
		listenerT.ExitValueLiteral(s)
	}
}

func (s *ValueLiteralContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case EQLVisitor:
		return t.VisitValueLiteral(s)

	default:
		return t.VisitChildren(s)
	}
}

type ValueFuncallContext struct {
	ValueContext
}

func NewValueFuncallContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *ValueFuncallContext {
	var p = new(ValueFuncallContext)

	InitEmptyValueContext(&p.ValueContext)
	p.parser = parser
	p.CopyAll(ctx.(*ValueContext))

	return p
}

func (s *ValueFuncallContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *ValueFuncallContext) Funcall() IFuncallContext {
	var t antlr.RuleContext
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IFuncallContext); ok {
			t = ctx.(antlr.RuleContext)
			break
		}
	}

	if t == nil {
		return nil
	}

	return t.(IFuncallContext)
}

func (s *ValueFuncallContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(EQLListener); ok {
		listenerT.EnterValueFuncall(s)
	}
}

func (s *ValueFuncallContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(EQLListener); ok {
		listenerT.ExitValueFuncall(s)
	}
}

func (s *ValueFuncallContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case EQLVisitor:
		return t.VisitValueFuncall(s)

	default:
		return t.VisitChildren(s)
	}
}

type ValueFieldContext struct {
	ValueContext
}

func NewValueFieldContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *ValueFieldContext {
	var p = new(ValueFieldContext)

	InitEmptyValueContext(&p.ValueContext)
	p.parser = parser
	p.CopyAll(ctx.(*ValueContext))

	return p
}

func (s *ValueFieldContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *ValueFieldContext) Field() IFieldContext {
	var t antlr.RuleContext
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IFieldContext); ok {
			t = ctx.(antlr.RuleContext)
			break
		}
	}

	if t == nil {
		return nil
	}

	return t.(IFieldContext)
}

func (s *ValueFieldContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(EQLListener); ok {
		listenerT.EnterValueField(s)
	}
}

func (s *ValueFieldContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(EQLListener); ok {
		listenerT.ExitValueField(s)
	}
}

func (s *ValueFieldContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case EQLVisitor:
		return t.VisitValueField(s)

	default:
		return t.VisitChildren(s)
	}
}

func (p *EQLParser) Value() (localctx IValueContext) {
	return p.value(0)
}

func (p *EQLParser) value(_p int) (localctx IValueContext) {
	var _parentctx antlr.ParserRuleContext = p.GetParserRuleContext()

	_parentState := p.GetState()
	localctx = NewValueContext(p, p.GetParserRuleContext(), _parentState)
	var _prevctx IValueContext = localctx
	var _ antlr.ParserRuleContext = _prevctx // TODO: To prevent unused variable warning.
	_startState := 20
	p.EnterRecursionRule(localctx, 20, EQLParserRULE_value, _p)
	var _la int

	var _alt int

	p.EnterOuterAlt(localctx, 1)
	p.SetState(152)
	p.GetErrorHandler().Sync(p)
	if p.HasError() {
		goto errorExit
	}

	switch p.GetTokenStream().LA(1) {
	case EQLParserT__30:
		localctx = NewValueNullContext(p, localctx)
		p.SetParserRuleContext(localctx)
		_prevctx = localctx

		{
			p.SetState(144)
			p.Match(EQLParserT__30)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}

	case EQLParserBOOLEAN, EQLParserNUMBER, EQLParserSTRING:
		localctx = NewValueLiteralContext(p, localctx)
		p.SetParserRuleContext(localctx)
		_prevctx = localctx
		{
			p.SetState(145)
			p.Literal()
		}

	case EQLParserT__28, EQLParserID:
		localctx = NewValueFieldContext(p, localctx)
		p.SetParserRuleContext(localctx)
		_prevctx = localctx
		{
			p.SetState(146)
			p.Field()
		}

	case EQLParserT__42, EQLParserT__43, EQLParserT__44, EQLParserT__45, EQLParserT__46, EQLParserT__47, EQLParserT__48, EQLParserT__49, EQLParserT__50, EQLParserT__51, EQLParserT__52, EQLParserT__53, EQLParserT__54, EQLParserT__55, EQLParserT__56, EQLParserT__57, EQLParserT__58, EQLParserT__59, EQLParserT__60, EQLParserT__61:
		localctx = NewValueFuncallContext(p, localctx)
		p.SetParserRuleContext(localctx)
		_prevctx = localctx
		{
			p.SetState(147)
			p.Funcall()
		}

	case EQLParserT__11:
		localctx = NewValueGroupContext(p, localctx)
		p.SetParserRuleContext(localctx)
		_prevctx = localctx
		{
			p.SetState(148)
			p.Match(EQLParserT__11)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(149)
			p.value(0)
		}
		{
			p.SetState(150)
			p.Match(EQLParserT__12)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}

	default:
		p.SetError(antlr.NewNoViableAltException(p, nil, nil, nil, nil, nil))
		goto errorExit
	}
	p.GetParserRuleContext().SetStop(p.GetTokenStream().LT(-1))
	p.SetState(162)
	p.GetErrorHandler().Sync(p)
	if p.HasError() {
		goto errorExit
	}
	_alt = p.GetInterpreter().AdaptivePredict(p.BaseParser, p.GetTokenStream(), 14, p.GetParserRuleContext())
	if p.HasError() {
		goto errorExit
	}
	for _alt != 2 && _alt != antlr.ATNInvalidAltNumber {
		if _alt == 1 {
			if p.GetParseListeners() != nil {
				p.TriggerExitRuleEvent()
			}
			_prevctx = localctx
			p.SetState(160)
			p.GetErrorHandler().Sync(p)
			if p.HasError() {
				goto errorExit
			}

			switch p.GetInterpreter().AdaptivePredict(p.BaseParser, p.GetTokenStream(), 13, p.GetParserRuleContext()) {
			case 1:
				localctx = NewValueMulDivContext(p, NewValueContext(p, _parentctx, _parentState))
				localctx.(*ValueMulDivContext).left = _prevctx

				p.PushNewRecursionContext(localctx, _startState, EQLParserRULE_value)
				p.SetState(154)

				if !(p.Precpred(p.GetParserRuleContext(), 2)) {
					p.SetError(antlr.NewFailedPredicateException(p, "p.Precpred(p.GetParserRuleContext(), 2)", ""))
					goto errorExit
				}
				{
					p.SetState(155)

					var _lt = p.GetTokenStream().LT(1)

					localctx.(*ValueMulDivContext).op = _lt

					_la = p.GetTokenStream().LA(1)

					if !((int64(_la) & ^0x3f) == 0 && ((int64(1)<<_la)&30064771072) != 0) {
						var _ri = p.GetErrorHandler().RecoverInline(p)

						localctx.(*ValueMulDivContext).op = _ri
					} else {
						p.GetErrorHandler().ReportMatch(p)
						p.Consume()
					}
				}
				{
					p.SetState(156)

					var _x = p.value(3)

					localctx.(*ValueMulDivContext).right = _x
				}

			case 2:
				localctx = NewValueAddSubContext(p, NewValueContext(p, _parentctx, _parentState))
				localctx.(*ValueAddSubContext).left = _prevctx

				p.PushNewRecursionContext(localctx, _startState, EQLParserRULE_value)
				p.SetState(157)

				if !(p.Precpred(p.GetParserRuleContext(), 1)) {
					p.SetError(antlr.NewFailedPredicateException(p, "p.Precpred(p.GetParserRuleContext(), 1)", ""))
					goto errorExit
				}
				{
					p.SetState(158)

					var _lt = p.GetTokenStream().LT(1)

					localctx.(*ValueAddSubContext).op = _lt

					_la = p.GetTokenStream().LA(1)

					if !(_la == EQLParserT__34 || _la == EQLParserT__35) {
						var _ri = p.GetErrorHandler().RecoverInline(p)

						localctx.(*ValueAddSubContext).op = _ri
					} else {
						p.GetErrorHandler().ReportMatch(p)
						p.Consume()
					}
				}
				{
					p.SetState(159)

					var _x = p.value(2)

					localctx.(*ValueAddSubContext).right = _x
				}

			case antlr.ATNInvalidAltNumber:
				goto errorExit
			}

		}
		p.SetState(164)
		p.GetErrorHandler().Sync(p)
		if p.HasError() {
			goto errorExit
		}
		_alt = p.GetInterpreter().AdaptivePredict(p.BaseParser, p.GetTokenStream(), 14, p.GetParserRuleContext())
		if p.HasError() {
			goto errorExit
		}
	}

errorExit:
	if p.HasError() {
		v := p.GetError()
		localctx.SetException(v)
		p.GetErrorHandler().ReportError(p, v)
		p.GetErrorHandler().Recover(p, v)
		p.SetError(nil)
	}
	p.UnrollRecursionContexts(_parentctx)
	if false {
		goto errorExit // Trick to prevent compiler error if the label is not used
	}
	return localctx
}

// IPipeContext is an interface to support dynamic dispatch.
type IPipeContext interface {
	antlr.ParserRuleContext

	// GetParser returns the parser.
	GetParser() antlr.Parser
	// IsPipeContext differentiates from other interfaces.
	IsPipeContext()
}

type PipeContext struct {
	antlr.BaseParserRuleContext
	parser antlr.Parser
}

func NewEmptyPipeContext() *PipeContext {
	var p = new(PipeContext)
	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, nil, -1)
	p.RuleIndex = EQLParserRULE_pipe
	return p
}

func InitEmptyPipeContext(p *PipeContext) {
	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, nil, -1)
	p.RuleIndex = EQLParserRULE_pipe
}

func (*PipeContext) IsPipeContext() {}

func NewPipeContext(parser antlr.Parser, parent antlr.ParserRuleContext, invokingState int) *PipeContext {
	var p = new(PipeContext)

	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, parent, invokingState)

	p.parser = parser
	p.RuleIndex = EQLParserRULE_pipe

	return p
}

func (s *PipeContext) GetParser() antlr.Parser { return s.parser }

func (s *PipeContext) CopyAll(ctx *PipeContext) {
	s.CopyFrom(&ctx.BaseParserRuleContext)
}

func (s *PipeContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *PipeContext) ToStringTree(ruleNames []string, recog antlr.Recognizer) string {
	return antlr.TreesStringTree(s, ruleNames, recog)
}

type PipeHeadContext struct {
	PipeContext
}

func NewPipeHeadContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *PipeHeadContext {
	var p = new(PipeHeadContext)

	InitEmptyPipeContext(&p.PipeContext)
	p.parser = parser
	p.CopyAll(ctx.(*PipeContext))

	return p
}

func (s *PipeHeadContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *PipeHeadContext) NUMBER() antlr.TerminalNode {
	return s.GetToken(EQLParserNUMBER, 0)
}

func (s *PipeHeadContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(EQLListener); ok {
		listenerT.EnterPipeHead(s)
	}
}

func (s *PipeHeadContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(EQLListener); ok {
		listenerT.ExitPipeHead(s)
	}
}

func (s *PipeHeadContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case EQLVisitor:
		return t.VisitPipeHead(s)

	default:
		return t.VisitChildren(s)
	}
}

type PipeCountContext struct {
	PipeContext
}

func NewPipeCountContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *PipeCountContext {
	var p = new(PipeCountContext)

	InitEmptyPipeContext(&p.PipeContext)
	p.parser = parser
	p.CopyAll(ctx.(*PipeContext))

	return p
}

func (s *PipeCountContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *PipeCountContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(EQLListener); ok {
		listenerT.EnterPipeCount(s)
	}
}

func (s *PipeCountContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(EQLListener); ok {
		listenerT.ExitPipeCount(s)
	}
}

func (s *PipeCountContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case EQLVisitor:
		return t.VisitPipeCount(s)

	default:
		return t.VisitChildren(s)
	}
}

type PipeUniqueContext struct {
	PipeContext
}

func NewPipeUniqueContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *PipeUniqueContext {
	var p = new(PipeUniqueContext)

	InitEmptyPipeContext(&p.PipeContext)
	p.parser = parser
	p.CopyAll(ctx.(*PipeContext))

	return p
}

func (s *PipeUniqueContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *PipeUniqueContext) FieldList() IFieldListContext {
	var t antlr.RuleContext
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IFieldListContext); ok {
			t = ctx.(antlr.RuleContext)
			break
		}
	}

	if t == nil {
		return nil
	}

	return t.(IFieldListContext)
}

func (s *PipeUniqueContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(EQLListener); ok {
		listenerT.EnterPipeUnique(s)
	}
}

func (s *PipeUniqueContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(EQLListener); ok {
		listenerT.ExitPipeUnique(s)
	}
}

func (s *PipeUniqueContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case EQLVisitor:
		return t.VisitPipeUnique(s)

	default:
		return t.VisitChildren(s)
	}
}

type PipeTailContext struct {
	PipeContext
}

func NewPipeTailContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *PipeTailContext {
	var p = new(PipeTailContext)

	InitEmptyPipeContext(&p.PipeContext)
	p.parser = parser
	p.CopyAll(ctx.(*PipeContext))

	return p
}

func (s *PipeTailContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *PipeTailContext) NUMBER() antlr.TerminalNode {
	return s.GetToken(EQLParserNUMBER, 0)
}

func (s *PipeTailContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(EQLListener); ok {
		listenerT.EnterPipeTail(s)
	}
}

func (s *PipeTailContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(EQLListener); ok {
		listenerT.ExitPipeTail(s)
	}
}

func (s *PipeTailContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case EQLVisitor:
		return t.VisitPipeTail(s)

	default:
		return t.VisitChildren(s)
	}
}

type PipeFilterContext struct {
	PipeContext
}

func NewPipeFilterContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *PipeFilterContext {
	var p = new(PipeFilterContext)

	InitEmptyPipeContext(&p.PipeContext)
	p.parser = parser
	p.CopyAll(ctx.(*PipeContext))

	return p
}

func (s *PipeFilterContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *PipeFilterContext) Condition() IConditionContext {
	var t antlr.RuleContext
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IConditionContext); ok {
			t = ctx.(antlr.RuleContext)
			break
		}
	}

	if t == nil {
		return nil
	}

	return t.(IConditionContext)
}

func (s *PipeFilterContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(EQLListener); ok {
		listenerT.EnterPipeFilter(s)
	}
}

func (s *PipeFilterContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(EQLListener); ok {
		listenerT.ExitPipeFilter(s)
	}
}

func (s *PipeFilterContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case EQLVisitor:
		return t.VisitPipeFilter(s)

	default:
		return t.VisitChildren(s)
	}
}

type PipeSortContext struct {
	PipeContext
}

func NewPipeSortContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *PipeSortContext {
	var p = new(PipeSortContext)

	InitEmptyPipeContext(&p.PipeContext)
	p.parser = parser
	p.CopyAll(ctx.(*PipeContext))

	return p
}

func (s *PipeSortContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *PipeSortContext) FieldList() IFieldListContext {
	var t antlr.RuleContext
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IFieldListContext); ok {
			t = ctx.(antlr.RuleContext)
			break
		}
	}

	if t == nil {
		return nil
	}

	return t.(IFieldListContext)
}

func (s *PipeSortContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(EQLListener); ok {
		listenerT.EnterPipeSort(s)
	}
}

func (s *PipeSortContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(EQLListener); ok {
		listenerT.ExitPipeSort(s)
	}
}

func (s *PipeSortContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case EQLVisitor:
		return t.VisitPipeSort(s)

	default:
		return t.VisitChildren(s)
	}
}

func (p *EQLParser) Pipe() (localctx IPipeContext) {
	localctx = NewPipeContext(p, p.GetParserRuleContext(), p.GetState())
	p.EnterRule(localctx, 22, EQLParserRULE_pipe)
	p.SetState(176)
	p.GetErrorHandler().Sync(p)
	if p.HasError() {
		goto errorExit
	}

	switch p.GetTokenStream().LA(1) {
	case EQLParserT__36:
		localctx = NewPipeHeadContext(p, localctx)
		p.EnterOuterAlt(localctx, 1)
		{
			p.SetState(165)
			p.Match(EQLParserT__36)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(166)
			p.Match(EQLParserNUMBER)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}

	case EQLParserT__37:
		localctx = NewPipeTailContext(p, localctx)
		p.EnterOuterAlt(localctx, 2)
		{
			p.SetState(167)
			p.Match(EQLParserT__37)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(168)
			p.Match(EQLParserNUMBER)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}

	case EQLParserT__38:
		localctx = NewPipeCountContext(p, localctx)
		p.EnterOuterAlt(localctx, 3)
		{
			p.SetState(169)
			p.Match(EQLParserT__38)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}

	case EQLParserT__39:
		localctx = NewPipeUniqueContext(p, localctx)
		p.EnterOuterAlt(localctx, 4)
		{
			p.SetState(170)
			p.Match(EQLParserT__39)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(171)
			p.FieldList()
		}

	case EQLParserT__40:
		localctx = NewPipeFilterContext(p, localctx)
		p.EnterOuterAlt(localctx, 5)
		{
			p.SetState(172)
			p.Match(EQLParserT__40)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(173)
			p.condition(0)
		}

	case EQLParserT__41:
		localctx = NewPipeSortContext(p, localctx)
		p.EnterOuterAlt(localctx, 6)
		{
			p.SetState(174)
			p.Match(EQLParserT__41)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(175)
			p.FieldList()
		}

	default:
		p.SetError(antlr.NewNoViableAltException(p, nil, nil, nil, nil, nil))
		goto errorExit
	}

errorExit:
	if p.HasError() {
		v := p.GetError()
		localctx.SetException(v)
		p.GetErrorHandler().ReportError(p, v)
		p.GetErrorHandler().Recover(p, v)
		p.SetError(nil)
	}
	p.ExitRule()
	if false {
		goto errorExit // Trick to prevent compiler error if the label is not used
	}
	return localctx
}

// IFuncallContext is an interface to support dynamic dispatch.
type IFuncallContext interface {
	antlr.ParserRuleContext

	// GetParser returns the parser.
	GetParser() antlr.Parser

	// Getter signatures
	FuncName() IFuncNameContext
	AllValue() []IValueContext
	Value(i int) IValueContext

	// IsFuncallContext differentiates from other interfaces.
	IsFuncallContext()
}

type FuncallContext struct {
	antlr.BaseParserRuleContext
	parser antlr.Parser
}

func NewEmptyFuncallContext() *FuncallContext {
	var p = new(FuncallContext)
	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, nil, -1)
	p.RuleIndex = EQLParserRULE_funcall
	return p
}

func InitEmptyFuncallContext(p *FuncallContext) {
	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, nil, -1)
	p.RuleIndex = EQLParserRULE_funcall
}

func (*FuncallContext) IsFuncallContext() {}

func NewFuncallContext(parser antlr.Parser, parent antlr.ParserRuleContext, invokingState int) *FuncallContext {
	var p = new(FuncallContext)

	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, parent, invokingState)

	p.parser = parser
	p.RuleIndex = EQLParserRULE_funcall

	return p
}

func (s *FuncallContext) GetParser() antlr.Parser { return s.parser }

func (s *FuncallContext) FuncName() IFuncNameContext {
	var t antlr.RuleContext
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IFuncNameContext); ok {
			t = ctx.(antlr.RuleContext)
			break
		}
	}

	if t == nil {
		return nil
	}

	return t.(IFuncNameContext)
}

func (s *FuncallContext) AllValue() []IValueContext {
	children := s.GetChildren()
	len := 0
	for _, ctx := range children {
		if _, ok := ctx.(IValueContext); ok {
			len++
		}
	}

	tst := make([]IValueContext, len)
	i := 0
	for _, ctx := range children {
		if t, ok := ctx.(IValueContext); ok {
			tst[i] = t.(IValueContext)
			i++
		}
	}

	return tst
}

func (s *FuncallContext) Value(i int) IValueContext {
	var t antlr.RuleContext
	j := 0
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IValueContext); ok {
			if j == i {
				t = ctx.(antlr.RuleContext)
				break
			}
			j++
		}
	}

	if t == nil {
		return nil
	}

	return t.(IValueContext)
}

func (s *FuncallContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *FuncallContext) ToStringTree(ruleNames []string, recog antlr.Recognizer) string {
	return antlr.TreesStringTree(s, ruleNames, recog)
}

func (s *FuncallContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(EQLListener); ok {
		listenerT.EnterFuncall(s)
	}
}

func (s *FuncallContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(EQLListener); ok {
		listenerT.ExitFuncall(s)
	}
}

func (s *FuncallContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case EQLVisitor:
		return t.VisitFuncall(s)

	default:
		return t.VisitChildren(s)
	}
}

func (p *EQLParser) Funcall() (localctx IFuncallContext) {
	localctx = NewFuncallContext(p, p.GetParserRuleContext(), p.GetState())
	p.EnterRule(localctx, 24, EQLParserRULE_funcall)
	var _la int

	p.EnterOuterAlt(localctx, 1)
	{
		p.SetState(178)
		p.FuncName()
	}
	{
		p.SetState(179)
		p.Match(EQLParserT__11)
		if p.HasError() {
			// Recognition error - abort rule
			goto errorExit
		}
	}
	{
		p.SetState(180)
		p.value(0)
	}
	p.SetState(185)
	p.GetErrorHandler().Sync(p)
	if p.HasError() {
		goto errorExit
	}
	_la = p.GetTokenStream().LA(1)

	for _la == EQLParserT__29 {
		{
			p.SetState(181)
			p.Match(EQLParserT__29)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(182)
			p.value(0)
		}

		p.SetState(187)
		p.GetErrorHandler().Sync(p)
		if p.HasError() {
			goto errorExit
		}
		_la = p.GetTokenStream().LA(1)
	}
	{
		p.SetState(188)
		p.Match(EQLParserT__12)
		if p.HasError() {
			// Recognition error - abort rule
			goto errorExit
		}
	}

errorExit:
	if p.HasError() {
		v := p.GetError()
		localctx.SetException(v)
		p.GetErrorHandler().ReportError(p, v)
		p.GetErrorHandler().Recover(p, v)
		p.SetError(nil)
	}
	p.ExitRule()
	if false {
		goto errorExit // Trick to prevent compiler error if the label is not used
	}
	return localctx
}

// IFuncNameContext is an interface to support dynamic dispatch.
type IFuncNameContext interface {
	antlr.ParserRuleContext

	// GetParser returns the parser.
	GetParser() antlr.Parser
	// IsFuncNameContext differentiates from other interfaces.
	IsFuncNameContext()
}

type FuncNameContext struct {
	antlr.BaseParserRuleContext
	parser antlr.Parser
}

func NewEmptyFuncNameContext() *FuncNameContext {
	var p = new(FuncNameContext)
	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, nil, -1)
	p.RuleIndex = EQLParserRULE_funcName
	return p
}

func InitEmptyFuncNameContext(p *FuncNameContext) {
	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, nil, -1)
	p.RuleIndex = EQLParserRULE_funcName
}

func (*FuncNameContext) IsFuncNameContext() {}

func NewFuncNameContext(parser antlr.Parser, parent antlr.ParserRuleContext, invokingState int) *FuncNameContext {
	var p = new(FuncNameContext)

	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, parent, invokingState)

	p.parser = parser
	p.RuleIndex = EQLParserRULE_funcName

	return p
}

func (s *FuncNameContext) GetParser() antlr.Parser { return s.parser }
func (s *FuncNameContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *FuncNameContext) ToStringTree(ruleNames []string, recog antlr.Recognizer) string {
	return antlr.TreesStringTree(s, ruleNames, recog)
}

func (s *FuncNameContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(EQLListener); ok {
		listenerT.EnterFuncName(s)
	}
}

func (s *FuncNameContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(EQLListener); ok {
		listenerT.ExitFuncName(s)
	}
}

func (s *FuncNameContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case EQLVisitor:
		return t.VisitFuncName(s)

	default:
		return t.VisitChildren(s)
	}
}

func (p *EQLParser) FuncName() (localctx IFuncNameContext) {
	localctx = NewFuncNameContext(p, p.GetParserRuleContext(), p.GetState())
	p.EnterRule(localctx, 26, EQLParserRULE_funcName)
	var _la int

	p.EnterOuterAlt(localctx, 1)
	{
		p.SetState(190)
		_la = p.GetTokenStream().LA(1)

		if !((int64(_la) & ^0x3f) == 0 && ((int64(1)<<_la)&9223363240761753600) != 0) {
			p.GetErrorHandler().RecoverInline(p)
		} else {
			p.GetErrorHandler().ReportMatch(p)
			p.Consume()
		}
	}

errorExit:
	if p.HasError() {
		v := p.GetError()
		localctx.SetException(v)
		p.GetErrorHandler().ReportError(p, v)
		p.GetErrorHandler().Recover(p, v)
		p.SetError(nil)
	}
	p.ExitRule()
	if false {
		goto errorExit // Trick to prevent compiler error if the label is not used
	}
	return localctx
}

// IIntervalContext is an interface to support dynamic dispatch.
type IIntervalContext interface {
	antlr.ParserRuleContext

	// GetParser returns the parser.
	GetParser() antlr.Parser

	// Getter signatures
	INTERVAL() antlr.TerminalNode

	// IsIntervalContext differentiates from other interfaces.
	IsIntervalContext()
}

type IntervalContext struct {
	antlr.BaseParserRuleContext
	parser antlr.Parser
}

func NewEmptyIntervalContext() *IntervalContext {
	var p = new(IntervalContext)
	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, nil, -1)
	p.RuleIndex = EQLParserRULE_interval
	return p
}

func InitEmptyIntervalContext(p *IntervalContext) {
	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, nil, -1)
	p.RuleIndex = EQLParserRULE_interval
}

func (*IntervalContext) IsIntervalContext() {}

func NewIntervalContext(parser antlr.Parser, parent antlr.ParserRuleContext, invokingState int) *IntervalContext {
	var p = new(IntervalContext)

	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, parent, invokingState)

	p.parser = parser
	p.RuleIndex = EQLParserRULE_interval

	return p
}

func (s *IntervalContext) GetParser() antlr.Parser { return s.parser }

func (s *IntervalContext) INTERVAL() antlr.TerminalNode {
	return s.GetToken(EQLParserINTERVAL, 0)
}

func (s *IntervalContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *IntervalContext) ToStringTree(ruleNames []string, recog antlr.Recognizer) string {
	return antlr.TreesStringTree(s, ruleNames, recog)
}

func (s *IntervalContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(EQLListener); ok {
		listenerT.EnterInterval(s)
	}
}

func (s *IntervalContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(EQLListener); ok {
		listenerT.ExitInterval(s)
	}
}

func (s *IntervalContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case EQLVisitor:
		return t.VisitInterval(s)

	default:
		return t.VisitChildren(s)
	}
}

func (p *EQLParser) Interval() (localctx IIntervalContext) {
	localctx = NewIntervalContext(p, p.GetParserRuleContext(), p.GetState())
	p.EnterRule(localctx, 28, EQLParserRULE_interval)
	p.EnterOuterAlt(localctx, 1)
	{
		p.SetState(192)
		p.Match(EQLParserINTERVAL)
		if p.HasError() {
			// Recognition error - abort rule
			goto errorExit
		}
	}

errorExit:
	if p.HasError() {
		v := p.GetError()
		localctx.SetException(v)
		p.GetErrorHandler().ReportError(p, v)
		p.GetErrorHandler().Recover(p, v)
		p.SetError(nil)
	}
	p.ExitRule()
	if false {
		goto errorExit // Trick to prevent compiler error if the label is not used
	}
	return localctx
}

func (p *EQLParser) Sempred(localctx antlr.RuleContext, ruleIndex, predIndex int) bool {
	switch ruleIndex {
	case 4:
		var t *ConditionContext = nil
		if localctx != nil {
			t = localctx.(*ConditionContext)
		}
		return p.Condition_Sempred(t, predIndex)

	case 10:
		var t *ValueContext = nil
		if localctx != nil {
			t = localctx.(*ValueContext)
		}
		return p.Value_Sempred(t, predIndex)

	default:
		panic("No predicate with index: " + fmt.Sprint(ruleIndex))
	}
}

func (p *EQLParser) Condition_Sempred(localctx antlr.RuleContext, predIndex int) bool {
	switch predIndex {
	case 0:
		return p.Precpred(p.GetParserRuleContext(), 3)

	default:
		panic("No predicate with index: " + fmt.Sprint(predIndex))
	}
}

func (p *EQLParser) Value_Sempred(localctx antlr.RuleContext, predIndex int) bool {
	switch predIndex {
	case 1:
		return p.Precpred(p.GetParserRuleContext(), 2)

	case 2:
		return p.Precpred(p.GetParserRuleContext(), 1)

	default:
		panic("No predicate with index: " + fmt.Sprint(predIndex))
	}
}
