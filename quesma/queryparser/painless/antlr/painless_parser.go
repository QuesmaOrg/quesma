// Code generated from quesma/queryparser/painless/antlr/PainlessParser.g4 by ANTLR 4.13.2. DO NOT EDIT.

package parser // PainlessParser
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

type PainlessParser struct {
	*antlr.BaseParser
}

var PainlessParserParserStaticData struct {
	once                   sync.Once
	serializedATN          []int32
	LiteralNames           []string
	SymbolicNames          []string
	RuleNames              []string
	PredictionContextCache *antlr.PredictionContextCache
	atn                    *antlr.ATN
	decisionToDFA          []*antlr.DFA
}

func painlessparserParserInit() {
	staticData := &PainlessParserParserStaticData
	staticData.LiteralNames = []string{
		"", "", "", "'{'", "'}'", "'['", "']'", "'('", "')'", "'.'", "'?.'",
		"','", "';'", "'if'", "'in'", "'else'", "'while'", "'do'", "'for'",
		"'continue'", "'break'", "'return'", "'new'", "'try'", "'catch'", "'throw'",
		"'this'", "'instanceof'", "'!'", "'~'", "'*'", "'/'", "'%'", "'+'",
		"'-'", "'<<'", "'>>'", "'>>>'", "'<'", "'<='", "'>'", "'>='", "'=='",
		"'==='", "'!='", "'!=='", "'&'", "'^'", "'|'", "'&&'", "'||'", "'?'",
		"':'", "'?:'", "'::'", "'->'", "'=~'", "'==~'", "'++'", "'--'", "'='",
		"'+='", "'-='", "'*='", "'/='", "'%='", "'&='", "'^='", "'|='", "'<<='",
		"'>>='", "'>>>='", "", "", "", "", "", "", "'true'", "'false'", "'null'",
		"", "'def'",
	}
	staticData.SymbolicNames = []string{
		"", "WS", "COMMENT", "LBRACK", "RBRACK", "LBRACE", "RBRACE", "LP", "RP",
		"DOT", "NSDOT", "COMMA", "SEMICOLON", "IF", "IN", "ELSE", "WHILE", "DO",
		"FOR", "CONTINUE", "BREAK", "RETURN", "NEW", "TRY", "CATCH", "THROW",
		"THIS", "INSTANCEOF", "BOOLNOT", "BWNOT", "MUL", "DIV", "REM", "ADD",
		"SUB", "LSH", "RSH", "USH", "LT", "LTE", "GT", "GTE", "EQ", "EQR", "NE",
		"NER", "BWAND", "XOR", "BWOR", "BOOLAND", "BOOLOR", "COND", "COLON",
		"ELVIS", "REF", "ARROW", "FIND", "MATCH", "INCR", "DECR", "ASSIGN",
		"AADD", "ASUB", "AMUL", "ADIV", "AREM", "AAND", "AXOR", "AOR", "ALSH",
		"ARSH", "AUSH", "OCTAL", "HEX", "INTEGER", "DECIMAL", "STRING", "REGEX",
		"TRUE", "FALSE", "NULL", "PRIMITIVE", "DEF", "ID", "DOTINTEGER", "DOTID",
	}
	staticData.RuleNames = []string{
		"source", "function", "parameters", "statement", "rstatement", "dstatement",
		"trailer", "block", "empty", "initializer", "afterthought", "declaration",
		"decltype", "type", "declvar", "trap", "noncondexpression", "expression",
		"unary", "unarynotaddsub", "castexpression", "primordefcasttype", "refcasttype",
		"chain", "primary", "postfix", "postdot", "callinvoke", "fieldaccess",
		"braceaccess", "arrayinitializer", "listinitializer", "mapinitializer",
		"maptoken", "arguments", "argument", "lambda", "lamtype", "funcref",
	}
	staticData.PredictionContextCache = antlr.NewPredictionContextCache()
	staticData.serializedATN = []int32{
		4, 1, 85, 570, 2, 0, 7, 0, 2, 1, 7, 1, 2, 2, 7, 2, 2, 3, 7, 3, 2, 4, 7,
		4, 2, 5, 7, 5, 2, 6, 7, 6, 2, 7, 7, 7, 2, 8, 7, 8, 2, 9, 7, 9, 2, 10, 7,
		10, 2, 11, 7, 11, 2, 12, 7, 12, 2, 13, 7, 13, 2, 14, 7, 14, 2, 15, 7, 15,
		2, 16, 7, 16, 2, 17, 7, 17, 2, 18, 7, 18, 2, 19, 7, 19, 2, 20, 7, 20, 2,
		21, 7, 21, 2, 22, 7, 22, 2, 23, 7, 23, 2, 24, 7, 24, 2, 25, 7, 25, 2, 26,
		7, 26, 2, 27, 7, 27, 2, 28, 7, 28, 2, 29, 7, 29, 2, 30, 7, 30, 2, 31, 7,
		31, 2, 32, 7, 32, 2, 33, 7, 33, 2, 34, 7, 34, 2, 35, 7, 35, 2, 36, 7, 36,
		2, 37, 7, 37, 2, 38, 7, 38, 1, 0, 5, 0, 80, 8, 0, 10, 0, 12, 0, 83, 9,
		0, 1, 0, 5, 0, 86, 8, 0, 10, 0, 12, 0, 89, 9, 0, 1, 0, 1, 0, 1, 1, 1, 1,
		1, 1, 1, 1, 1, 1, 1, 2, 1, 2, 1, 2, 1, 2, 1, 2, 1, 2, 1, 2, 5, 2, 105,
		8, 2, 10, 2, 12, 2, 108, 9, 2, 3, 2, 110, 8, 2, 1, 2, 1, 2, 1, 3, 1, 3,
		1, 3, 1, 3, 3, 3, 118, 8, 3, 1, 4, 1, 4, 1, 4, 1, 4, 1, 4, 1, 4, 1, 4,
		1, 4, 1, 4, 1, 4, 1, 4, 1, 4, 1, 4, 1, 4, 3, 4, 134, 8, 4, 1, 4, 1, 4,
		1, 4, 3, 4, 139, 8, 4, 1, 4, 1, 4, 3, 4, 143, 8, 4, 1, 4, 1, 4, 3, 4, 147,
		8, 4, 1, 4, 1, 4, 1, 4, 3, 4, 152, 8, 4, 1, 4, 1, 4, 1, 4, 1, 4, 1, 4,
		1, 4, 1, 4, 1, 4, 1, 4, 1, 4, 1, 4, 1, 4, 1, 4, 1, 4, 1, 4, 1, 4, 1, 4,
		1, 4, 1, 4, 1, 4, 4, 4, 174, 8, 4, 11, 4, 12, 4, 175, 3, 4, 178, 8, 4,
		1, 5, 1, 5, 1, 5, 1, 5, 1, 5, 1, 5, 1, 5, 1, 5, 1, 5, 1, 5, 1, 5, 1, 5,
		3, 5, 192, 8, 5, 1, 5, 1, 5, 1, 5, 3, 5, 197, 8, 5, 1, 6, 1, 6, 3, 6, 201,
		8, 6, 1, 7, 1, 7, 5, 7, 205, 8, 7, 10, 7, 12, 7, 208, 9, 7, 1, 7, 3, 7,
		211, 8, 7, 1, 7, 1, 7, 1, 8, 1, 8, 1, 9, 1, 9, 3, 9, 219, 8, 9, 1, 10,
		1, 10, 1, 11, 1, 11, 1, 11, 1, 11, 5, 11, 227, 8, 11, 10, 11, 12, 11, 230,
		9, 11, 1, 12, 1, 12, 1, 12, 5, 12, 235, 8, 12, 10, 12, 12, 12, 238, 9,
		12, 1, 13, 1, 13, 1, 13, 1, 13, 1, 13, 5, 13, 245, 8, 13, 10, 13, 12, 13,
		248, 9, 13, 3, 13, 250, 8, 13, 1, 14, 1, 14, 1, 14, 3, 14, 255, 8, 14,
		1, 15, 1, 15, 1, 15, 1, 15, 1, 15, 1, 15, 1, 15, 1, 16, 1, 16, 1, 16, 1,
		16, 1, 16, 1, 16, 1, 16, 1, 16, 1, 16, 1, 16, 1, 16, 1, 16, 1, 16, 1, 16,
		1, 16, 1, 16, 1, 16, 1, 16, 1, 16, 1, 16, 1, 16, 1, 16, 1, 16, 1, 16, 1,
		16, 1, 16, 1, 16, 1, 16, 1, 16, 1, 16, 1, 16, 1, 16, 1, 16, 1, 16, 1, 16,
		1, 16, 1, 16, 1, 16, 1, 16, 1, 16, 1, 16, 1, 16, 5, 16, 306, 8, 16, 10,
		16, 12, 16, 309, 9, 16, 1, 17, 1, 17, 1, 17, 1, 17, 1, 17, 1, 17, 1, 17,
		1, 17, 1, 17, 1, 17, 1, 17, 3, 17, 322, 8, 17, 1, 18, 1, 18, 1, 18, 1,
		18, 1, 18, 3, 18, 329, 8, 18, 1, 19, 1, 19, 1, 19, 1, 19, 1, 19, 1, 19,
		1, 19, 3, 19, 338, 8, 19, 1, 20, 1, 20, 1, 20, 1, 20, 1, 20, 1, 20, 1,
		20, 1, 20, 1, 20, 1, 20, 3, 20, 350, 8, 20, 1, 21, 1, 21, 1, 22, 1, 22,
		1, 22, 4, 22, 357, 8, 22, 11, 22, 12, 22, 358, 1, 22, 1, 22, 1, 22, 4,
		22, 364, 8, 22, 11, 22, 12, 22, 365, 1, 22, 1, 22, 1, 22, 5, 22, 371, 8,
		22, 10, 22, 12, 22, 374, 9, 22, 1, 22, 1, 22, 5, 22, 378, 8, 22, 10, 22,
		12, 22, 381, 9, 22, 3, 22, 383, 8, 22, 1, 23, 1, 23, 5, 23, 387, 8, 23,
		10, 23, 12, 23, 390, 9, 23, 1, 23, 3, 23, 393, 8, 23, 1, 24, 1, 24, 1,
		24, 1, 24, 1, 24, 1, 24, 1, 24, 1, 24, 1, 24, 1, 24, 1, 24, 1, 24, 1, 24,
		1, 24, 1, 24, 1, 24, 1, 24, 1, 24, 1, 24, 3, 24, 414, 8, 24, 1, 25, 1,
		25, 1, 25, 3, 25, 419, 8, 25, 1, 26, 1, 26, 3, 26, 423, 8, 26, 1, 27, 1,
		27, 1, 27, 1, 27, 1, 28, 1, 28, 1, 28, 1, 29, 1, 29, 1, 29, 1, 29, 1, 30,
		1, 30, 1, 30, 1, 30, 1, 30, 1, 30, 4, 30, 442, 8, 30, 11, 30, 12, 30, 443,
		1, 30, 1, 30, 5, 30, 448, 8, 30, 10, 30, 12, 30, 451, 9, 30, 3, 30, 453,
		8, 30, 1, 30, 1, 30, 1, 30, 1, 30, 1, 30, 1, 30, 1, 30, 1, 30, 5, 30, 463,
		8, 30, 10, 30, 12, 30, 466, 9, 30, 3, 30, 468, 8, 30, 1, 30, 1, 30, 5,
		30, 472, 8, 30, 10, 30, 12, 30, 475, 9, 30, 3, 30, 477, 8, 30, 1, 31, 1,
		31, 1, 31, 1, 31, 5, 31, 483, 8, 31, 10, 31, 12, 31, 486, 9, 31, 1, 31,
		1, 31, 1, 31, 1, 31, 3, 31, 492, 8, 31, 1, 32, 1, 32, 1, 32, 1, 32, 5,
		32, 498, 8, 32, 10, 32, 12, 32, 501, 9, 32, 1, 32, 1, 32, 1, 32, 1, 32,
		1, 32, 3, 32, 508, 8, 32, 1, 33, 1, 33, 1, 33, 1, 33, 1, 34, 1, 34, 1,
		34, 1, 34, 5, 34, 518, 8, 34, 10, 34, 12, 34, 521, 9, 34, 3, 34, 523, 8,
		34, 1, 34, 1, 34, 1, 35, 1, 35, 1, 35, 3, 35, 530, 8, 35, 1, 36, 1, 36,
		1, 36, 1, 36, 1, 36, 5, 36, 537, 8, 36, 10, 36, 12, 36, 540, 9, 36, 3,
		36, 542, 8, 36, 1, 36, 3, 36, 545, 8, 36, 1, 36, 1, 36, 1, 36, 3, 36, 550,
		8, 36, 1, 37, 3, 37, 553, 8, 37, 1, 37, 1, 37, 1, 38, 1, 38, 1, 38, 1,
		38, 1, 38, 1, 38, 1, 38, 1, 38, 1, 38, 1, 38, 1, 38, 3, 38, 568, 8, 38,
		1, 38, 0, 1, 32, 39, 0, 2, 4, 6, 8, 10, 12, 14, 16, 18, 20, 22, 24, 26,
		28, 30, 32, 34, 36, 38, 40, 42, 44, 46, 48, 50, 52, 54, 56, 58, 60, 62,
		64, 66, 68, 70, 72, 74, 76, 0, 14, 1, 1, 12, 12, 1, 0, 30, 32, 1, 0, 33,
		34, 1, 0, 56, 57, 1, 0, 35, 37, 1, 0, 38, 41, 1, 0, 42, 45, 1, 0, 60, 71,
		1, 0, 58, 59, 1, 0, 28, 29, 1, 0, 81, 82, 1, 0, 72, 75, 1, 0, 9, 10, 1,
		0, 84, 85, 628, 0, 81, 1, 0, 0, 0, 2, 92, 1, 0, 0, 0, 4, 97, 1, 0, 0, 0,
		6, 117, 1, 0, 0, 0, 8, 177, 1, 0, 0, 0, 10, 196, 1, 0, 0, 0, 12, 200, 1,
		0, 0, 0, 14, 202, 1, 0, 0, 0, 16, 214, 1, 0, 0, 0, 18, 218, 1, 0, 0, 0,
		20, 220, 1, 0, 0, 0, 22, 222, 1, 0, 0, 0, 24, 231, 1, 0, 0, 0, 26, 249,
		1, 0, 0, 0, 28, 251, 1, 0, 0, 0, 30, 256, 1, 0, 0, 0, 32, 263, 1, 0, 0,
		0, 34, 321, 1, 0, 0, 0, 36, 328, 1, 0, 0, 0, 38, 337, 1, 0, 0, 0, 40, 349,
		1, 0, 0, 0, 42, 351, 1, 0, 0, 0, 44, 382, 1, 0, 0, 0, 46, 392, 1, 0, 0,
		0, 48, 413, 1, 0, 0, 0, 50, 418, 1, 0, 0, 0, 52, 422, 1, 0, 0, 0, 54, 424,
		1, 0, 0, 0, 56, 428, 1, 0, 0, 0, 58, 431, 1, 0, 0, 0, 60, 476, 1, 0, 0,
		0, 62, 491, 1, 0, 0, 0, 64, 507, 1, 0, 0, 0, 66, 509, 1, 0, 0, 0, 68, 513,
		1, 0, 0, 0, 70, 529, 1, 0, 0, 0, 72, 544, 1, 0, 0, 0, 74, 552, 1, 0, 0,
		0, 76, 567, 1, 0, 0, 0, 78, 80, 3, 2, 1, 0, 79, 78, 1, 0, 0, 0, 80, 83,
		1, 0, 0, 0, 81, 79, 1, 0, 0, 0, 81, 82, 1, 0, 0, 0, 82, 87, 1, 0, 0, 0,
		83, 81, 1, 0, 0, 0, 84, 86, 3, 6, 3, 0, 85, 84, 1, 0, 0, 0, 86, 89, 1,
		0, 0, 0, 87, 85, 1, 0, 0, 0, 87, 88, 1, 0, 0, 0, 88, 90, 1, 0, 0, 0, 89,
		87, 1, 0, 0, 0, 90, 91, 5, 0, 0, 1, 91, 1, 1, 0, 0, 0, 92, 93, 3, 24, 12,
		0, 93, 94, 5, 83, 0, 0, 94, 95, 3, 4, 2, 0, 95, 96, 3, 14, 7, 0, 96, 3,
		1, 0, 0, 0, 97, 109, 5, 7, 0, 0, 98, 99, 3, 24, 12, 0, 99, 106, 5, 83,
		0, 0, 100, 101, 5, 11, 0, 0, 101, 102, 3, 24, 12, 0, 102, 103, 5, 83, 0,
		0, 103, 105, 1, 0, 0, 0, 104, 100, 1, 0, 0, 0, 105, 108, 1, 0, 0, 0, 106,
		104, 1, 0, 0, 0, 106, 107, 1, 0, 0, 0, 107, 110, 1, 0, 0, 0, 108, 106,
		1, 0, 0, 0, 109, 98, 1, 0, 0, 0, 109, 110, 1, 0, 0, 0, 110, 111, 1, 0,
		0, 0, 111, 112, 5, 8, 0, 0, 112, 5, 1, 0, 0, 0, 113, 118, 3, 8, 4, 0, 114,
		115, 3, 10, 5, 0, 115, 116, 7, 0, 0, 0, 116, 118, 1, 0, 0, 0, 117, 113,
		1, 0, 0, 0, 117, 114, 1, 0, 0, 0, 118, 7, 1, 0, 0, 0, 119, 120, 5, 13,
		0, 0, 120, 121, 5, 7, 0, 0, 121, 122, 3, 34, 17, 0, 122, 123, 5, 8, 0,
		0, 123, 124, 3, 12, 6, 0, 124, 125, 5, 15, 0, 0, 125, 126, 3, 12, 6, 0,
		126, 178, 1, 0, 0, 0, 127, 128, 5, 16, 0, 0, 128, 129, 5, 7, 0, 0, 129,
		130, 3, 34, 17, 0, 130, 133, 5, 8, 0, 0, 131, 134, 3, 12, 6, 0, 132, 134,
		3, 16, 8, 0, 133, 131, 1, 0, 0, 0, 133, 132, 1, 0, 0, 0, 134, 178, 1, 0,
		0, 0, 135, 136, 5, 18, 0, 0, 136, 138, 5, 7, 0, 0, 137, 139, 3, 18, 9,
		0, 138, 137, 1, 0, 0, 0, 138, 139, 1, 0, 0, 0, 139, 140, 1, 0, 0, 0, 140,
		142, 5, 12, 0, 0, 141, 143, 3, 34, 17, 0, 142, 141, 1, 0, 0, 0, 142, 143,
		1, 0, 0, 0, 143, 144, 1, 0, 0, 0, 144, 146, 5, 12, 0, 0, 145, 147, 3, 20,
		10, 0, 146, 145, 1, 0, 0, 0, 146, 147, 1, 0, 0, 0, 147, 148, 1, 0, 0, 0,
		148, 151, 5, 8, 0, 0, 149, 152, 3, 12, 6, 0, 150, 152, 3, 16, 8, 0, 151,
		149, 1, 0, 0, 0, 151, 150, 1, 0, 0, 0, 152, 178, 1, 0, 0, 0, 153, 154,
		5, 18, 0, 0, 154, 155, 5, 7, 0, 0, 155, 156, 3, 24, 12, 0, 156, 157, 5,
		83, 0, 0, 157, 158, 5, 52, 0, 0, 158, 159, 3, 34, 17, 0, 159, 160, 5, 8,
		0, 0, 160, 161, 3, 12, 6, 0, 161, 178, 1, 0, 0, 0, 162, 163, 5, 18, 0,
		0, 163, 164, 5, 7, 0, 0, 164, 165, 5, 83, 0, 0, 165, 166, 5, 14, 0, 0,
		166, 167, 3, 34, 17, 0, 167, 168, 5, 8, 0, 0, 168, 169, 3, 12, 6, 0, 169,
		178, 1, 0, 0, 0, 170, 171, 5, 23, 0, 0, 171, 173, 3, 14, 7, 0, 172, 174,
		3, 30, 15, 0, 173, 172, 1, 0, 0, 0, 174, 175, 1, 0, 0, 0, 175, 173, 1,
		0, 0, 0, 175, 176, 1, 0, 0, 0, 176, 178, 1, 0, 0, 0, 177, 119, 1, 0, 0,
		0, 177, 127, 1, 0, 0, 0, 177, 135, 1, 0, 0, 0, 177, 153, 1, 0, 0, 0, 177,
		162, 1, 0, 0, 0, 177, 170, 1, 0, 0, 0, 178, 9, 1, 0, 0, 0, 179, 180, 5,
		17, 0, 0, 180, 181, 3, 14, 7, 0, 181, 182, 5, 16, 0, 0, 182, 183, 5, 7,
		0, 0, 183, 184, 3, 34, 17, 0, 184, 185, 5, 8, 0, 0, 185, 197, 1, 0, 0,
		0, 186, 197, 3, 22, 11, 0, 187, 197, 5, 19, 0, 0, 188, 197, 5, 20, 0, 0,
		189, 191, 5, 21, 0, 0, 190, 192, 3, 34, 17, 0, 191, 190, 1, 0, 0, 0, 191,
		192, 1, 0, 0, 0, 192, 197, 1, 0, 0, 0, 193, 194, 5, 25, 0, 0, 194, 197,
		3, 34, 17, 0, 195, 197, 3, 34, 17, 0, 196, 179, 1, 0, 0, 0, 196, 186, 1,
		0, 0, 0, 196, 187, 1, 0, 0, 0, 196, 188, 1, 0, 0, 0, 196, 189, 1, 0, 0,
		0, 196, 193, 1, 0, 0, 0, 196, 195, 1, 0, 0, 0, 197, 11, 1, 0, 0, 0, 198,
		201, 3, 14, 7, 0, 199, 201, 3, 6, 3, 0, 200, 198, 1, 0, 0, 0, 200, 199,
		1, 0, 0, 0, 201, 13, 1, 0, 0, 0, 202, 206, 5, 3, 0, 0, 203, 205, 3, 6,
		3, 0, 204, 203, 1, 0, 0, 0, 205, 208, 1, 0, 0, 0, 206, 204, 1, 0, 0, 0,
		206, 207, 1, 0, 0, 0, 207, 210, 1, 0, 0, 0, 208, 206, 1, 0, 0, 0, 209,
		211, 3, 10, 5, 0, 210, 209, 1, 0, 0, 0, 210, 211, 1, 0, 0, 0, 211, 212,
		1, 0, 0, 0, 212, 213, 5, 4, 0, 0, 213, 15, 1, 0, 0, 0, 214, 215, 5, 12,
		0, 0, 215, 17, 1, 0, 0, 0, 216, 219, 3, 22, 11, 0, 217, 219, 3, 34, 17,
		0, 218, 216, 1, 0, 0, 0, 218, 217, 1, 0, 0, 0, 219, 19, 1, 0, 0, 0, 220,
		221, 3, 34, 17, 0, 221, 21, 1, 0, 0, 0, 222, 223, 3, 24, 12, 0, 223, 228,
		3, 28, 14, 0, 224, 225, 5, 11, 0, 0, 225, 227, 3, 28, 14, 0, 226, 224,
		1, 0, 0, 0, 227, 230, 1, 0, 0, 0, 228, 226, 1, 0, 0, 0, 228, 229, 1, 0,
		0, 0, 229, 23, 1, 0, 0, 0, 230, 228, 1, 0, 0, 0, 231, 236, 3, 26, 13, 0,
		232, 233, 5, 5, 0, 0, 233, 235, 5, 6, 0, 0, 234, 232, 1, 0, 0, 0, 235,
		238, 1, 0, 0, 0, 236, 234, 1, 0, 0, 0, 236, 237, 1, 0, 0, 0, 237, 25, 1,
		0, 0, 0, 238, 236, 1, 0, 0, 0, 239, 250, 5, 82, 0, 0, 240, 250, 5, 81,
		0, 0, 241, 246, 5, 83, 0, 0, 242, 243, 5, 9, 0, 0, 243, 245, 5, 85, 0,
		0, 244, 242, 1, 0, 0, 0, 245, 248, 1, 0, 0, 0, 246, 244, 1, 0, 0, 0, 246,
		247, 1, 0, 0, 0, 247, 250, 1, 0, 0, 0, 248, 246, 1, 0, 0, 0, 249, 239,
		1, 0, 0, 0, 249, 240, 1, 0, 0, 0, 249, 241, 1, 0, 0, 0, 250, 27, 1, 0,
		0, 0, 251, 254, 5, 83, 0, 0, 252, 253, 5, 60, 0, 0, 253, 255, 3, 34, 17,
		0, 254, 252, 1, 0, 0, 0, 254, 255, 1, 0, 0, 0, 255, 29, 1, 0, 0, 0, 256,
		257, 5, 24, 0, 0, 257, 258, 5, 7, 0, 0, 258, 259, 3, 26, 13, 0, 259, 260,
		5, 83, 0, 0, 260, 261, 5, 8, 0, 0, 261, 262, 3, 14, 7, 0, 262, 31, 1, 0,
		0, 0, 263, 264, 6, 16, -1, 0, 264, 265, 3, 36, 18, 0, 265, 307, 1, 0, 0,
		0, 266, 267, 10, 13, 0, 0, 267, 268, 7, 1, 0, 0, 268, 306, 3, 32, 16, 14,
		269, 270, 10, 12, 0, 0, 270, 271, 7, 2, 0, 0, 271, 306, 3, 32, 16, 13,
		272, 273, 10, 11, 0, 0, 273, 274, 7, 3, 0, 0, 274, 306, 3, 32, 16, 12,
		275, 276, 10, 10, 0, 0, 276, 277, 7, 4, 0, 0, 277, 306, 3, 32, 16, 11,
		278, 279, 10, 9, 0, 0, 279, 280, 7, 5, 0, 0, 280, 306, 3, 32, 16, 10, 281,
		282, 10, 7, 0, 0, 282, 283, 7, 6, 0, 0, 283, 306, 3, 32, 16, 8, 284, 285,
		10, 6, 0, 0, 285, 286, 5, 46, 0, 0, 286, 306, 3, 32, 16, 7, 287, 288, 10,
		5, 0, 0, 288, 289, 5, 47, 0, 0, 289, 306, 3, 32, 16, 6, 290, 291, 10, 4,
		0, 0, 291, 292, 5, 48, 0, 0, 292, 306, 3, 32, 16, 5, 293, 294, 10, 3, 0,
		0, 294, 295, 5, 49, 0, 0, 295, 306, 3, 32, 16, 4, 296, 297, 10, 2, 0, 0,
		297, 298, 5, 50, 0, 0, 298, 306, 3, 32, 16, 3, 299, 300, 10, 1, 0, 0, 300,
		301, 5, 53, 0, 0, 301, 306, 3, 32, 16, 1, 302, 303, 10, 8, 0, 0, 303, 304,
		5, 27, 0, 0, 304, 306, 3, 24, 12, 0, 305, 266, 1, 0, 0, 0, 305, 269, 1,
		0, 0, 0, 305, 272, 1, 0, 0, 0, 305, 275, 1, 0, 0, 0, 305, 278, 1, 0, 0,
		0, 305, 281, 1, 0, 0, 0, 305, 284, 1, 0, 0, 0, 305, 287, 1, 0, 0, 0, 305,
		290, 1, 0, 0, 0, 305, 293, 1, 0, 0, 0, 305, 296, 1, 0, 0, 0, 305, 299,
		1, 0, 0, 0, 305, 302, 1, 0, 0, 0, 306, 309, 1, 0, 0, 0, 307, 305, 1, 0,
		0, 0, 307, 308, 1, 0, 0, 0, 308, 33, 1, 0, 0, 0, 309, 307, 1, 0, 0, 0,
		310, 322, 3, 32, 16, 0, 311, 312, 3, 32, 16, 0, 312, 313, 5, 51, 0, 0,
		313, 314, 3, 34, 17, 0, 314, 315, 5, 52, 0, 0, 315, 316, 3, 34, 17, 0,
		316, 322, 1, 0, 0, 0, 317, 318, 3, 32, 16, 0, 318, 319, 7, 7, 0, 0, 319,
		320, 3, 34, 17, 0, 320, 322, 1, 0, 0, 0, 321, 310, 1, 0, 0, 0, 321, 311,
		1, 0, 0, 0, 321, 317, 1, 0, 0, 0, 322, 35, 1, 0, 0, 0, 323, 324, 7, 8,
		0, 0, 324, 329, 3, 46, 23, 0, 325, 326, 7, 2, 0, 0, 326, 329, 3, 36, 18,
		0, 327, 329, 3, 38, 19, 0, 328, 323, 1, 0, 0, 0, 328, 325, 1, 0, 0, 0,
		328, 327, 1, 0, 0, 0, 329, 37, 1, 0, 0, 0, 330, 338, 3, 46, 23, 0, 331,
		332, 3, 46, 23, 0, 332, 333, 7, 8, 0, 0, 333, 338, 1, 0, 0, 0, 334, 335,
		7, 9, 0, 0, 335, 338, 3, 36, 18, 0, 336, 338, 3, 40, 20, 0, 337, 330, 1,
		0, 0, 0, 337, 331, 1, 0, 0, 0, 337, 334, 1, 0, 0, 0, 337, 336, 1, 0, 0,
		0, 338, 39, 1, 0, 0, 0, 339, 340, 5, 7, 0, 0, 340, 341, 3, 42, 21, 0, 341,
		342, 5, 8, 0, 0, 342, 343, 3, 36, 18, 0, 343, 350, 1, 0, 0, 0, 344, 345,
		5, 7, 0, 0, 345, 346, 3, 44, 22, 0, 346, 347, 5, 8, 0, 0, 347, 348, 3,
		38, 19, 0, 348, 350, 1, 0, 0, 0, 349, 339, 1, 0, 0, 0, 349, 344, 1, 0,
		0, 0, 350, 41, 1, 0, 0, 0, 351, 352, 7, 10, 0, 0, 352, 43, 1, 0, 0, 0,
		353, 356, 5, 82, 0, 0, 354, 355, 5, 5, 0, 0, 355, 357, 5, 6, 0, 0, 356,
		354, 1, 0, 0, 0, 357, 358, 1, 0, 0, 0, 358, 356, 1, 0, 0, 0, 358, 359,
		1, 0, 0, 0, 359, 383, 1, 0, 0, 0, 360, 363, 5, 81, 0, 0, 361, 362, 5, 5,
		0, 0, 362, 364, 5, 6, 0, 0, 363, 361, 1, 0, 0, 0, 364, 365, 1, 0, 0, 0,
		365, 363, 1, 0, 0, 0, 365, 366, 1, 0, 0, 0, 366, 383, 1, 0, 0, 0, 367,
		372, 5, 83, 0, 0, 368, 369, 5, 9, 0, 0, 369, 371, 5, 85, 0, 0, 370, 368,
		1, 0, 0, 0, 371, 374, 1, 0, 0, 0, 372, 370, 1, 0, 0, 0, 372, 373, 1, 0,
		0, 0, 373, 379, 1, 0, 0, 0, 374, 372, 1, 0, 0, 0, 375, 376, 5, 5, 0, 0,
		376, 378, 5, 6, 0, 0, 377, 375, 1, 0, 0, 0, 378, 381, 1, 0, 0, 0, 379,
		377, 1, 0, 0, 0, 379, 380, 1, 0, 0, 0, 380, 383, 1, 0, 0, 0, 381, 379,
		1, 0, 0, 0, 382, 353, 1, 0, 0, 0, 382, 360, 1, 0, 0, 0, 382, 367, 1, 0,
		0, 0, 383, 45, 1, 0, 0, 0, 384, 388, 3, 48, 24, 0, 385, 387, 3, 50, 25,
		0, 386, 385, 1, 0, 0, 0, 387, 390, 1, 0, 0, 0, 388, 386, 1, 0, 0, 0, 388,
		389, 1, 0, 0, 0, 389, 393, 1, 0, 0, 0, 390, 388, 1, 0, 0, 0, 391, 393,
		3, 60, 30, 0, 392, 384, 1, 0, 0, 0, 392, 391, 1, 0, 0, 0, 393, 47, 1, 0,
		0, 0, 394, 395, 5, 7, 0, 0, 395, 396, 3, 34, 17, 0, 396, 397, 5, 8, 0,
		0, 397, 414, 1, 0, 0, 0, 398, 414, 7, 11, 0, 0, 399, 414, 5, 78, 0, 0,
		400, 414, 5, 79, 0, 0, 401, 414, 5, 80, 0, 0, 402, 414, 5, 76, 0, 0, 403,
		414, 5, 77, 0, 0, 404, 414, 3, 62, 31, 0, 405, 414, 3, 64, 32, 0, 406,
		414, 5, 83, 0, 0, 407, 408, 5, 83, 0, 0, 408, 414, 3, 68, 34, 0, 409, 410,
		5, 22, 0, 0, 410, 411, 3, 26, 13, 0, 411, 412, 3, 68, 34, 0, 412, 414,
		1, 0, 0, 0, 413, 394, 1, 0, 0, 0, 413, 398, 1, 0, 0, 0, 413, 399, 1, 0,
		0, 0, 413, 400, 1, 0, 0, 0, 413, 401, 1, 0, 0, 0, 413, 402, 1, 0, 0, 0,
		413, 403, 1, 0, 0, 0, 413, 404, 1, 0, 0, 0, 413, 405, 1, 0, 0, 0, 413,
		406, 1, 0, 0, 0, 413, 407, 1, 0, 0, 0, 413, 409, 1, 0, 0, 0, 414, 49, 1,
		0, 0, 0, 415, 419, 3, 54, 27, 0, 416, 419, 3, 56, 28, 0, 417, 419, 3, 58,
		29, 0, 418, 415, 1, 0, 0, 0, 418, 416, 1, 0, 0, 0, 418, 417, 1, 0, 0, 0,
		419, 51, 1, 0, 0, 0, 420, 423, 3, 54, 27, 0, 421, 423, 3, 56, 28, 0, 422,
		420, 1, 0, 0, 0, 422, 421, 1, 0, 0, 0, 423, 53, 1, 0, 0, 0, 424, 425, 7,
		12, 0, 0, 425, 426, 5, 85, 0, 0, 426, 427, 3, 68, 34, 0, 427, 55, 1, 0,
		0, 0, 428, 429, 7, 12, 0, 0, 429, 430, 7, 13, 0, 0, 430, 57, 1, 0, 0, 0,
		431, 432, 5, 5, 0, 0, 432, 433, 3, 34, 17, 0, 433, 434, 5, 6, 0, 0, 434,
		59, 1, 0, 0, 0, 435, 436, 5, 22, 0, 0, 436, 441, 3, 26, 13, 0, 437, 438,
		5, 5, 0, 0, 438, 439, 3, 34, 17, 0, 439, 440, 5, 6, 0, 0, 440, 442, 1,
		0, 0, 0, 441, 437, 1, 0, 0, 0, 442, 443, 1, 0, 0, 0, 443, 441, 1, 0, 0,
		0, 443, 444, 1, 0, 0, 0, 444, 452, 1, 0, 0, 0, 445, 449, 3, 52, 26, 0,
		446, 448, 3, 50, 25, 0, 447, 446, 1, 0, 0, 0, 448, 451, 1, 0, 0, 0, 449,
		447, 1, 0, 0, 0, 449, 450, 1, 0, 0, 0, 450, 453, 1, 0, 0, 0, 451, 449,
		1, 0, 0, 0, 452, 445, 1, 0, 0, 0, 452, 453, 1, 0, 0, 0, 453, 477, 1, 0,
		0, 0, 454, 455, 5, 22, 0, 0, 455, 456, 3, 26, 13, 0, 456, 457, 5, 5, 0,
		0, 457, 458, 5, 6, 0, 0, 458, 467, 5, 3, 0, 0, 459, 464, 3, 34, 17, 0,
		460, 461, 5, 11, 0, 0, 461, 463, 3, 34, 17, 0, 462, 460, 1, 0, 0, 0, 463,
		466, 1, 0, 0, 0, 464, 462, 1, 0, 0, 0, 464, 465, 1, 0, 0, 0, 465, 468,
		1, 0, 0, 0, 466, 464, 1, 0, 0, 0, 467, 459, 1, 0, 0, 0, 467, 468, 1, 0,
		0, 0, 468, 469, 1, 0, 0, 0, 469, 473, 5, 4, 0, 0, 470, 472, 3, 50, 25,
		0, 471, 470, 1, 0, 0, 0, 472, 475, 1, 0, 0, 0, 473, 471, 1, 0, 0, 0, 473,
		474, 1, 0, 0, 0, 474, 477, 1, 0, 0, 0, 475, 473, 1, 0, 0, 0, 476, 435,
		1, 0, 0, 0, 476, 454, 1, 0, 0, 0, 477, 61, 1, 0, 0, 0, 478, 479, 5, 5,
		0, 0, 479, 484, 3, 34, 17, 0, 480, 481, 5, 11, 0, 0, 481, 483, 3, 34, 17,
		0, 482, 480, 1, 0, 0, 0, 483, 486, 1, 0, 0, 0, 484, 482, 1, 0, 0, 0, 484,
		485, 1, 0, 0, 0, 485, 487, 1, 0, 0, 0, 486, 484, 1, 0, 0, 0, 487, 488,
		5, 6, 0, 0, 488, 492, 1, 0, 0, 0, 489, 490, 5, 5, 0, 0, 490, 492, 5, 6,
		0, 0, 491, 478, 1, 0, 0, 0, 491, 489, 1, 0, 0, 0, 492, 63, 1, 0, 0, 0,
		493, 494, 5, 5, 0, 0, 494, 499, 3, 66, 33, 0, 495, 496, 5, 11, 0, 0, 496,
		498, 3, 66, 33, 0, 497, 495, 1, 0, 0, 0, 498, 501, 1, 0, 0, 0, 499, 497,
		1, 0, 0, 0, 499, 500, 1, 0, 0, 0, 500, 502, 1, 0, 0, 0, 501, 499, 1, 0,
		0, 0, 502, 503, 5, 6, 0, 0, 503, 508, 1, 0, 0, 0, 504, 505, 5, 5, 0, 0,
		505, 506, 5, 52, 0, 0, 506, 508, 5, 6, 0, 0, 507, 493, 1, 0, 0, 0, 507,
		504, 1, 0, 0, 0, 508, 65, 1, 0, 0, 0, 509, 510, 3, 34, 17, 0, 510, 511,
		5, 52, 0, 0, 511, 512, 3, 34, 17, 0, 512, 67, 1, 0, 0, 0, 513, 522, 5,
		7, 0, 0, 514, 519, 3, 70, 35, 0, 515, 516, 5, 11, 0, 0, 516, 518, 3, 70,
		35, 0, 517, 515, 1, 0, 0, 0, 518, 521, 1, 0, 0, 0, 519, 517, 1, 0, 0, 0,
		519, 520, 1, 0, 0, 0, 520, 523, 1, 0, 0, 0, 521, 519, 1, 0, 0, 0, 522,
		514, 1, 0, 0, 0, 522, 523, 1, 0, 0, 0, 523, 524, 1, 0, 0, 0, 524, 525,
		5, 8, 0, 0, 525, 69, 1, 0, 0, 0, 526, 530, 3, 34, 17, 0, 527, 530, 3, 72,
		36, 0, 528, 530, 3, 76, 38, 0, 529, 526, 1, 0, 0, 0, 529, 527, 1, 0, 0,
		0, 529, 528, 1, 0, 0, 0, 530, 71, 1, 0, 0, 0, 531, 545, 3, 74, 37, 0, 532,
		541, 5, 7, 0, 0, 533, 538, 3, 74, 37, 0, 534, 535, 5, 11, 0, 0, 535, 537,
		3, 74, 37, 0, 536, 534, 1, 0, 0, 0, 537, 540, 1, 0, 0, 0, 538, 536, 1,
		0, 0, 0, 538, 539, 1, 0, 0, 0, 539, 542, 1, 0, 0, 0, 540, 538, 1, 0, 0,
		0, 541, 533, 1, 0, 0, 0, 541, 542, 1, 0, 0, 0, 542, 543, 1, 0, 0, 0, 543,
		545, 5, 8, 0, 0, 544, 531, 1, 0, 0, 0, 544, 532, 1, 0, 0, 0, 545, 546,
		1, 0, 0, 0, 546, 549, 5, 55, 0, 0, 547, 550, 3, 14, 7, 0, 548, 550, 3,
		34, 17, 0, 549, 547, 1, 0, 0, 0, 549, 548, 1, 0, 0, 0, 550, 73, 1, 0, 0,
		0, 551, 553, 3, 24, 12, 0, 552, 551, 1, 0, 0, 0, 552, 553, 1, 0, 0, 0,
		553, 554, 1, 0, 0, 0, 554, 555, 5, 83, 0, 0, 555, 75, 1, 0, 0, 0, 556,
		557, 3, 24, 12, 0, 557, 558, 5, 54, 0, 0, 558, 559, 5, 83, 0, 0, 559, 568,
		1, 0, 0, 0, 560, 561, 3, 24, 12, 0, 561, 562, 5, 54, 0, 0, 562, 563, 5,
		22, 0, 0, 563, 568, 1, 0, 0, 0, 564, 565, 5, 26, 0, 0, 565, 566, 5, 54,
		0, 0, 566, 568, 5, 83, 0, 0, 567, 556, 1, 0, 0, 0, 567, 560, 1, 0, 0, 0,
		567, 564, 1, 0, 0, 0, 568, 77, 1, 0, 0, 0, 59, 81, 87, 106, 109, 117, 133,
		138, 142, 146, 151, 175, 177, 191, 196, 200, 206, 210, 218, 228, 236, 246,
		249, 254, 305, 307, 321, 328, 337, 349, 358, 365, 372, 379, 382, 388, 392,
		413, 418, 422, 443, 449, 452, 464, 467, 473, 476, 484, 491, 499, 507, 519,
		522, 529, 538, 541, 544, 549, 552, 567,
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

// PainlessParserInit initializes any static state used to implement PainlessParser. By default the
// static state used to implement the parser is lazily initialized during the first call to
// NewPainlessParser(). You can call this function if you wish to initialize the static state ahead
// of time.
func PainlessParserInit() {
	staticData := &PainlessParserParserStaticData
	staticData.once.Do(painlessparserParserInit)
}

// NewPainlessParser produces a new parser instance for the optional input antlr.TokenStream.
func NewPainlessParser(input antlr.TokenStream) *PainlessParser {
	PainlessParserInit()
	this := new(PainlessParser)
	this.BaseParser = antlr.NewBaseParser(input)
	staticData := &PainlessParserParserStaticData
	this.Interpreter = antlr.NewParserATNSimulator(this, staticData.atn, staticData.decisionToDFA, staticData.PredictionContextCache)
	this.RuleNames = staticData.RuleNames
	this.LiteralNames = staticData.LiteralNames
	this.SymbolicNames = staticData.SymbolicNames
	this.GrammarFileName = "PainlessParser.g4"

	return this
}

// PainlessParser tokens.
const (
	PainlessParserEOF        = antlr.TokenEOF
	PainlessParserWS         = 1
	PainlessParserCOMMENT    = 2
	PainlessParserLBRACK     = 3
	PainlessParserRBRACK     = 4
	PainlessParserLBRACE     = 5
	PainlessParserRBRACE     = 6
	PainlessParserLP         = 7
	PainlessParserRP         = 8
	PainlessParserDOT        = 9
	PainlessParserNSDOT      = 10
	PainlessParserCOMMA      = 11
	PainlessParserSEMICOLON  = 12
	PainlessParserIF         = 13
	PainlessParserIN         = 14
	PainlessParserELSE       = 15
	PainlessParserWHILE      = 16
	PainlessParserDO         = 17
	PainlessParserFOR        = 18
	PainlessParserCONTINUE   = 19
	PainlessParserBREAK      = 20
	PainlessParserRETURN     = 21
	PainlessParserNEW        = 22
	PainlessParserTRY        = 23
	PainlessParserCATCH      = 24
	PainlessParserTHROW      = 25
	PainlessParserTHIS       = 26
	PainlessParserINSTANCEOF = 27
	PainlessParserBOOLNOT    = 28
	PainlessParserBWNOT      = 29
	PainlessParserMUL        = 30
	PainlessParserDIV        = 31
	PainlessParserREM        = 32
	PainlessParserADD        = 33
	PainlessParserSUB        = 34
	PainlessParserLSH        = 35
	PainlessParserRSH        = 36
	PainlessParserUSH        = 37
	PainlessParserLT         = 38
	PainlessParserLTE        = 39
	PainlessParserGT         = 40
	PainlessParserGTE        = 41
	PainlessParserEQ         = 42
	PainlessParserEQR        = 43
	PainlessParserNE         = 44
	PainlessParserNER        = 45
	PainlessParserBWAND      = 46
	PainlessParserXOR        = 47
	PainlessParserBWOR       = 48
	PainlessParserBOOLAND    = 49
	PainlessParserBOOLOR     = 50
	PainlessParserCOND       = 51
	PainlessParserCOLON      = 52
	PainlessParserELVIS      = 53
	PainlessParserREF        = 54
	PainlessParserARROW      = 55
	PainlessParserFIND       = 56
	PainlessParserMATCH      = 57
	PainlessParserINCR       = 58
	PainlessParserDECR       = 59
	PainlessParserASSIGN     = 60
	PainlessParserAADD       = 61
	PainlessParserASUB       = 62
	PainlessParserAMUL       = 63
	PainlessParserADIV       = 64
	PainlessParserAREM       = 65
	PainlessParserAAND       = 66
	PainlessParserAXOR       = 67
	PainlessParserAOR        = 68
	PainlessParserALSH       = 69
	PainlessParserARSH       = 70
	PainlessParserAUSH       = 71
	PainlessParserOCTAL      = 72
	PainlessParserHEX        = 73
	PainlessParserINTEGER    = 74
	PainlessParserDECIMAL    = 75
	PainlessParserSTRING     = 76
	PainlessParserREGEX      = 77
	PainlessParserTRUE       = 78
	PainlessParserFALSE      = 79
	PainlessParserNULL       = 80
	PainlessParserPRIMITIVE  = 81
	PainlessParserDEF        = 82
	PainlessParserID         = 83
	PainlessParserDOTINTEGER = 84
	PainlessParserDOTID      = 85
)

// PainlessParser rules.
const (
	PainlessParserRULE_source            = 0
	PainlessParserRULE_function          = 1
	PainlessParserRULE_parameters        = 2
	PainlessParserRULE_statement         = 3
	PainlessParserRULE_rstatement        = 4
	PainlessParserRULE_dstatement        = 5
	PainlessParserRULE_trailer           = 6
	PainlessParserRULE_block             = 7
	PainlessParserRULE_empty             = 8
	PainlessParserRULE_initializer       = 9
	PainlessParserRULE_afterthought      = 10
	PainlessParserRULE_declaration       = 11
	PainlessParserRULE_decltype          = 12
	PainlessParserRULE_type              = 13
	PainlessParserRULE_declvar           = 14
	PainlessParserRULE_trap              = 15
	PainlessParserRULE_noncondexpression = 16
	PainlessParserRULE_expression        = 17
	PainlessParserRULE_unary             = 18
	PainlessParserRULE_unarynotaddsub    = 19
	PainlessParserRULE_castexpression    = 20
	PainlessParserRULE_primordefcasttype = 21
	PainlessParserRULE_refcasttype       = 22
	PainlessParserRULE_chain             = 23
	PainlessParserRULE_primary           = 24
	PainlessParserRULE_postfix           = 25
	PainlessParserRULE_postdot           = 26
	PainlessParserRULE_callinvoke        = 27
	PainlessParserRULE_fieldaccess       = 28
	PainlessParserRULE_braceaccess       = 29
	PainlessParserRULE_arrayinitializer  = 30
	PainlessParserRULE_listinitializer   = 31
	PainlessParserRULE_mapinitializer    = 32
	PainlessParserRULE_maptoken          = 33
	PainlessParserRULE_arguments         = 34
	PainlessParserRULE_argument          = 35
	PainlessParserRULE_lambda            = 36
	PainlessParserRULE_lamtype           = 37
	PainlessParserRULE_funcref           = 38
)

// ISourceContext is an interface to support dynamic dispatch.
type ISourceContext interface {
	antlr.ParserRuleContext

	// GetParser returns the parser.
	GetParser() antlr.Parser

	// Getter signatures
	EOF() antlr.TerminalNode
	AllFunction() []IFunctionContext
	Function(i int) IFunctionContext
	AllStatement() []IStatementContext
	Statement(i int) IStatementContext

	// IsSourceContext differentiates from other interfaces.
	IsSourceContext()
}

type SourceContext struct {
	antlr.BaseParserRuleContext
	parser antlr.Parser
}

func NewEmptySourceContext() *SourceContext {
	var p = new(SourceContext)
	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, nil, -1)
	p.RuleIndex = PainlessParserRULE_source
	return p
}

func InitEmptySourceContext(p *SourceContext) {
	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, nil, -1)
	p.RuleIndex = PainlessParserRULE_source
}

func (*SourceContext) IsSourceContext() {}

func NewSourceContext(parser antlr.Parser, parent antlr.ParserRuleContext, invokingState int) *SourceContext {
	var p = new(SourceContext)

	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, parent, invokingState)

	p.parser = parser
	p.RuleIndex = PainlessParserRULE_source

	return p
}

func (s *SourceContext) GetParser() antlr.Parser { return s.parser }

func (s *SourceContext) EOF() antlr.TerminalNode {
	return s.GetToken(PainlessParserEOF, 0)
}

func (s *SourceContext) AllFunction() []IFunctionContext {
	children := s.GetChildren()
	len := 0
	for _, ctx := range children {
		if _, ok := ctx.(IFunctionContext); ok {
			len++
		}
	}

	tst := make([]IFunctionContext, len)
	i := 0
	for _, ctx := range children {
		if t, ok := ctx.(IFunctionContext); ok {
			tst[i] = t.(IFunctionContext)
			i++
		}
	}

	return tst
}

func (s *SourceContext) Function(i int) IFunctionContext {
	var t antlr.RuleContext
	j := 0
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IFunctionContext); ok {
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

	return t.(IFunctionContext)
}

func (s *SourceContext) AllStatement() []IStatementContext {
	children := s.GetChildren()
	len := 0
	for _, ctx := range children {
		if _, ok := ctx.(IStatementContext); ok {
			len++
		}
	}

	tst := make([]IStatementContext, len)
	i := 0
	for _, ctx := range children {
		if t, ok := ctx.(IStatementContext); ok {
			tst[i] = t.(IStatementContext)
			i++
		}
	}

	return tst
}

func (s *SourceContext) Statement(i int) IStatementContext {
	var t antlr.RuleContext
	j := 0
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IStatementContext); ok {
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

	return t.(IStatementContext)
}

func (s *SourceContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *SourceContext) ToStringTree(ruleNames []string, recog antlr.Recognizer) string {
	return antlr.TreesStringTree(s, ruleNames, recog)
}

func (s *SourceContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PainlessParserListener); ok {
		listenerT.EnterSource(s)
	}
}

func (s *SourceContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PainlessParserListener); ok {
		listenerT.ExitSource(s)
	}
}

func (s *SourceContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PainlessParserVisitor:
		return t.VisitSource(s)

	default:
		return t.VisitChildren(s)
	}
}

func (p *PainlessParser) Source() (localctx ISourceContext) {
	localctx = NewSourceContext(p, p.GetParserRuleContext(), p.GetState())
	p.EnterRule(localctx, 0, PainlessParserRULE_source)
	var _la int

	var _alt int

	p.EnterOuterAlt(localctx, 1)
	p.SetState(81)
	p.GetErrorHandler().Sync(p)
	if p.HasError() {
		goto errorExit
	}
	_alt = p.GetInterpreter().AdaptivePredict(p.BaseParser, p.GetTokenStream(), 0, p.GetParserRuleContext())
	if p.HasError() {
		goto errorExit
	}
	for _alt != 2 && _alt != antlr.ATNInvalidAltNumber {
		if _alt == 1 {
			{
				p.SetState(78)
				p.Function()
			}

		}
		p.SetState(83)
		p.GetErrorHandler().Sync(p)
		if p.HasError() {
			goto errorExit
		}
		_alt = p.GetInterpreter().AdaptivePredict(p.BaseParser, p.GetTokenStream(), 0, p.GetParserRuleContext())
		if p.HasError() {
			goto errorExit
		}
	}
	p.SetState(87)
	p.GetErrorHandler().Sync(p)
	if p.HasError() {
		goto errorExit
	}
	_la = p.GetTokenStream().LA(1)

	for ((int64(_la) & ^0x3f) == 0 && ((int64(1)<<_la)&864691155080519840) != 0) || ((int64((_la-72)) & ^0x3f) == 0 && ((int64(1)<<(_la-72))&4095) != 0) {
		{
			p.SetState(84)
			p.Statement()
		}

		p.SetState(89)
		p.GetErrorHandler().Sync(p)
		if p.HasError() {
			goto errorExit
		}
		_la = p.GetTokenStream().LA(1)
	}
	{
		p.SetState(90)
		p.Match(PainlessParserEOF)
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
	return localctx
	goto errorExit // Trick to prevent compiler error if the label is not used
}

// IFunctionContext is an interface to support dynamic dispatch.
type IFunctionContext interface {
	antlr.ParserRuleContext

	// GetParser returns the parser.
	GetParser() antlr.Parser

	// Getter signatures
	Decltype() IDecltypeContext
	ID() antlr.TerminalNode
	Parameters() IParametersContext
	Block() IBlockContext

	// IsFunctionContext differentiates from other interfaces.
	IsFunctionContext()
}

type FunctionContext struct {
	antlr.BaseParserRuleContext
	parser antlr.Parser
}

func NewEmptyFunctionContext() *FunctionContext {
	var p = new(FunctionContext)
	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, nil, -1)
	p.RuleIndex = PainlessParserRULE_function
	return p
}

func InitEmptyFunctionContext(p *FunctionContext) {
	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, nil, -1)
	p.RuleIndex = PainlessParserRULE_function
}

func (*FunctionContext) IsFunctionContext() {}

func NewFunctionContext(parser antlr.Parser, parent antlr.ParserRuleContext, invokingState int) *FunctionContext {
	var p = new(FunctionContext)

	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, parent, invokingState)

	p.parser = parser
	p.RuleIndex = PainlessParserRULE_function

	return p
}

func (s *FunctionContext) GetParser() antlr.Parser { return s.parser }

func (s *FunctionContext) Decltype() IDecltypeContext {
	var t antlr.RuleContext
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IDecltypeContext); ok {
			t = ctx.(antlr.RuleContext)
			break
		}
	}

	if t == nil {
		return nil
	}

	return t.(IDecltypeContext)
}

func (s *FunctionContext) ID() antlr.TerminalNode {
	return s.GetToken(PainlessParserID, 0)
}

func (s *FunctionContext) Parameters() IParametersContext {
	var t antlr.RuleContext
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IParametersContext); ok {
			t = ctx.(antlr.RuleContext)
			break
		}
	}

	if t == nil {
		return nil
	}

	return t.(IParametersContext)
}

func (s *FunctionContext) Block() IBlockContext {
	var t antlr.RuleContext
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IBlockContext); ok {
			t = ctx.(antlr.RuleContext)
			break
		}
	}

	if t == nil {
		return nil
	}

	return t.(IBlockContext)
}

func (s *FunctionContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *FunctionContext) ToStringTree(ruleNames []string, recog antlr.Recognizer) string {
	return antlr.TreesStringTree(s, ruleNames, recog)
}

func (s *FunctionContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PainlessParserListener); ok {
		listenerT.EnterFunction(s)
	}
}

func (s *FunctionContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PainlessParserListener); ok {
		listenerT.ExitFunction(s)
	}
}

func (s *FunctionContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PainlessParserVisitor:
		return t.VisitFunction(s)

	default:
		return t.VisitChildren(s)
	}
}

func (p *PainlessParser) Function() (localctx IFunctionContext) {
	localctx = NewFunctionContext(p, p.GetParserRuleContext(), p.GetState())
	p.EnterRule(localctx, 2, PainlessParserRULE_function)
	p.EnterOuterAlt(localctx, 1)
	{
		p.SetState(92)
		p.Decltype()
	}
	{
		p.SetState(93)
		p.Match(PainlessParserID)
		if p.HasError() {
			// Recognition error - abort rule
			goto errorExit
		}
	}
	{
		p.SetState(94)
		p.Parameters()
	}
	{
		p.SetState(95)
		p.Block()
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
	return localctx
	goto errorExit // Trick to prevent compiler error if the label is not used
}

// IParametersContext is an interface to support dynamic dispatch.
type IParametersContext interface {
	antlr.ParserRuleContext

	// GetParser returns the parser.
	GetParser() antlr.Parser

	// Getter signatures
	LP() antlr.TerminalNode
	RP() antlr.TerminalNode
	AllDecltype() []IDecltypeContext
	Decltype(i int) IDecltypeContext
	AllID() []antlr.TerminalNode
	ID(i int) antlr.TerminalNode
	AllCOMMA() []antlr.TerminalNode
	COMMA(i int) antlr.TerminalNode

	// IsParametersContext differentiates from other interfaces.
	IsParametersContext()
}

type ParametersContext struct {
	antlr.BaseParserRuleContext
	parser antlr.Parser
}

func NewEmptyParametersContext() *ParametersContext {
	var p = new(ParametersContext)
	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, nil, -1)
	p.RuleIndex = PainlessParserRULE_parameters
	return p
}

func InitEmptyParametersContext(p *ParametersContext) {
	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, nil, -1)
	p.RuleIndex = PainlessParserRULE_parameters
}

func (*ParametersContext) IsParametersContext() {}

func NewParametersContext(parser antlr.Parser, parent antlr.ParserRuleContext, invokingState int) *ParametersContext {
	var p = new(ParametersContext)

	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, parent, invokingState)

	p.parser = parser
	p.RuleIndex = PainlessParserRULE_parameters

	return p
}

func (s *ParametersContext) GetParser() antlr.Parser { return s.parser }

func (s *ParametersContext) LP() antlr.TerminalNode {
	return s.GetToken(PainlessParserLP, 0)
}

func (s *ParametersContext) RP() antlr.TerminalNode {
	return s.GetToken(PainlessParserRP, 0)
}

func (s *ParametersContext) AllDecltype() []IDecltypeContext {
	children := s.GetChildren()
	len := 0
	for _, ctx := range children {
		if _, ok := ctx.(IDecltypeContext); ok {
			len++
		}
	}

	tst := make([]IDecltypeContext, len)
	i := 0
	for _, ctx := range children {
		if t, ok := ctx.(IDecltypeContext); ok {
			tst[i] = t.(IDecltypeContext)
			i++
		}
	}

	return tst
}

func (s *ParametersContext) Decltype(i int) IDecltypeContext {
	var t antlr.RuleContext
	j := 0
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IDecltypeContext); ok {
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

	return t.(IDecltypeContext)
}

func (s *ParametersContext) AllID() []antlr.TerminalNode {
	return s.GetTokens(PainlessParserID)
}

func (s *ParametersContext) ID(i int) antlr.TerminalNode {
	return s.GetToken(PainlessParserID, i)
}

func (s *ParametersContext) AllCOMMA() []antlr.TerminalNode {
	return s.GetTokens(PainlessParserCOMMA)
}

func (s *ParametersContext) COMMA(i int) antlr.TerminalNode {
	return s.GetToken(PainlessParserCOMMA, i)
}

func (s *ParametersContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *ParametersContext) ToStringTree(ruleNames []string, recog antlr.Recognizer) string {
	return antlr.TreesStringTree(s, ruleNames, recog)
}

func (s *ParametersContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PainlessParserListener); ok {
		listenerT.EnterParameters(s)
	}
}

func (s *ParametersContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PainlessParserListener); ok {
		listenerT.ExitParameters(s)
	}
}

func (s *ParametersContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PainlessParserVisitor:
		return t.VisitParameters(s)

	default:
		return t.VisitChildren(s)
	}
}

func (p *PainlessParser) Parameters() (localctx IParametersContext) {
	localctx = NewParametersContext(p, p.GetParserRuleContext(), p.GetState())
	p.EnterRule(localctx, 4, PainlessParserRULE_parameters)
	var _la int

	p.EnterOuterAlt(localctx, 1)
	{
		p.SetState(97)
		p.Match(PainlessParserLP)
		if p.HasError() {
			// Recognition error - abort rule
			goto errorExit
		}
	}
	p.SetState(109)
	p.GetErrorHandler().Sync(p)
	if p.HasError() {
		goto errorExit
	}
	_la = p.GetTokenStream().LA(1)

	if (int64((_la-81)) & ^0x3f) == 0 && ((int64(1)<<(_la-81))&7) != 0 {
		{
			p.SetState(98)
			p.Decltype()
		}
		{
			p.SetState(99)
			p.Match(PainlessParserID)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		p.SetState(106)
		p.GetErrorHandler().Sync(p)
		if p.HasError() {
			goto errorExit
		}
		_la = p.GetTokenStream().LA(1)

		for _la == PainlessParserCOMMA {
			{
				p.SetState(100)
				p.Match(PainlessParserCOMMA)
				if p.HasError() {
					// Recognition error - abort rule
					goto errorExit
				}
			}
			{
				p.SetState(101)
				p.Decltype()
			}
			{
				p.SetState(102)
				p.Match(PainlessParserID)
				if p.HasError() {
					// Recognition error - abort rule
					goto errorExit
				}
			}

			p.SetState(108)
			p.GetErrorHandler().Sync(p)
			if p.HasError() {
				goto errorExit
			}
			_la = p.GetTokenStream().LA(1)
		}

	}
	{
		p.SetState(111)
		p.Match(PainlessParserRP)
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
	return localctx
	goto errorExit // Trick to prevent compiler error if the label is not used
}

// IStatementContext is an interface to support dynamic dispatch.
type IStatementContext interface {
	antlr.ParserRuleContext

	// GetParser returns the parser.
	GetParser() antlr.Parser

	// Getter signatures
	Rstatement() IRstatementContext
	Dstatement() IDstatementContext
	SEMICOLON() antlr.TerminalNode
	EOF() antlr.TerminalNode

	// IsStatementContext differentiates from other interfaces.
	IsStatementContext()
}

type StatementContext struct {
	antlr.BaseParserRuleContext
	parser antlr.Parser
}

func NewEmptyStatementContext() *StatementContext {
	var p = new(StatementContext)
	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, nil, -1)
	p.RuleIndex = PainlessParserRULE_statement
	return p
}

func InitEmptyStatementContext(p *StatementContext) {
	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, nil, -1)
	p.RuleIndex = PainlessParserRULE_statement
}

func (*StatementContext) IsStatementContext() {}

func NewStatementContext(parser antlr.Parser, parent antlr.ParserRuleContext, invokingState int) *StatementContext {
	var p = new(StatementContext)

	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, parent, invokingState)

	p.parser = parser
	p.RuleIndex = PainlessParserRULE_statement

	return p
}

func (s *StatementContext) GetParser() antlr.Parser { return s.parser }

func (s *StatementContext) Rstatement() IRstatementContext {
	var t antlr.RuleContext
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IRstatementContext); ok {
			t = ctx.(antlr.RuleContext)
			break
		}
	}

	if t == nil {
		return nil
	}

	return t.(IRstatementContext)
}

func (s *StatementContext) Dstatement() IDstatementContext {
	var t antlr.RuleContext
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IDstatementContext); ok {
			t = ctx.(antlr.RuleContext)
			break
		}
	}

	if t == nil {
		return nil
	}

	return t.(IDstatementContext)
}

func (s *StatementContext) SEMICOLON() antlr.TerminalNode {
	return s.GetToken(PainlessParserSEMICOLON, 0)
}

func (s *StatementContext) EOF() antlr.TerminalNode {
	return s.GetToken(PainlessParserEOF, 0)
}

func (s *StatementContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *StatementContext) ToStringTree(ruleNames []string, recog antlr.Recognizer) string {
	return antlr.TreesStringTree(s, ruleNames, recog)
}

func (s *StatementContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PainlessParserListener); ok {
		listenerT.EnterStatement(s)
	}
}

func (s *StatementContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PainlessParserListener); ok {
		listenerT.ExitStatement(s)
	}
}

func (s *StatementContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PainlessParserVisitor:
		return t.VisitStatement(s)

	default:
		return t.VisitChildren(s)
	}
}

func (p *PainlessParser) Statement() (localctx IStatementContext) {
	localctx = NewStatementContext(p, p.GetParserRuleContext(), p.GetState())
	p.EnterRule(localctx, 6, PainlessParserRULE_statement)
	var _la int

	p.SetState(117)
	p.GetErrorHandler().Sync(p)
	if p.HasError() {
		goto errorExit
	}

	switch p.GetTokenStream().LA(1) {
	case PainlessParserIF, PainlessParserWHILE, PainlessParserFOR, PainlessParserTRY:
		p.EnterOuterAlt(localctx, 1)
		{
			p.SetState(113)
			p.Rstatement()
		}

	case PainlessParserLBRACE, PainlessParserLP, PainlessParserDO, PainlessParserCONTINUE, PainlessParserBREAK, PainlessParserRETURN, PainlessParserNEW, PainlessParserTHROW, PainlessParserBOOLNOT, PainlessParserBWNOT, PainlessParserADD, PainlessParserSUB, PainlessParserINCR, PainlessParserDECR, PainlessParserOCTAL, PainlessParserHEX, PainlessParserINTEGER, PainlessParserDECIMAL, PainlessParserSTRING, PainlessParserREGEX, PainlessParserTRUE, PainlessParserFALSE, PainlessParserNULL, PainlessParserPRIMITIVE, PainlessParserDEF, PainlessParserID:
		p.EnterOuterAlt(localctx, 2)
		{
			p.SetState(114)
			p.Dstatement()
		}
		{
			p.SetState(115)
			_la = p.GetTokenStream().LA(1)

			if !(_la == PainlessParserEOF || _la == PainlessParserSEMICOLON) {
				p.GetErrorHandler().RecoverInline(p)
			} else {
				p.GetErrorHandler().ReportMatch(p)
				p.Consume()
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
	return localctx
	goto errorExit // Trick to prevent compiler error if the label is not used
}

// IRstatementContext is an interface to support dynamic dispatch.
type IRstatementContext interface {
	antlr.ParserRuleContext

	// GetParser returns the parser.
	GetParser() antlr.Parser
	// IsRstatementContext differentiates from other interfaces.
	IsRstatementContext()
}

type RstatementContext struct {
	antlr.BaseParserRuleContext
	parser antlr.Parser
}

func NewEmptyRstatementContext() *RstatementContext {
	var p = new(RstatementContext)
	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, nil, -1)
	p.RuleIndex = PainlessParserRULE_rstatement
	return p
}

func InitEmptyRstatementContext(p *RstatementContext) {
	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, nil, -1)
	p.RuleIndex = PainlessParserRULE_rstatement
}

func (*RstatementContext) IsRstatementContext() {}

func NewRstatementContext(parser antlr.Parser, parent antlr.ParserRuleContext, invokingState int) *RstatementContext {
	var p = new(RstatementContext)

	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, parent, invokingState)

	p.parser = parser
	p.RuleIndex = PainlessParserRULE_rstatement

	return p
}

func (s *RstatementContext) GetParser() antlr.Parser { return s.parser }

func (s *RstatementContext) CopyAll(ctx *RstatementContext) {
	s.CopyFrom(&ctx.BaseParserRuleContext)
}

func (s *RstatementContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *RstatementContext) ToStringTree(ruleNames []string, recog antlr.Recognizer) string {
	return antlr.TreesStringTree(s, ruleNames, recog)
}

type ForContext struct {
	RstatementContext
}

func NewForContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *ForContext {
	var p = new(ForContext)

	InitEmptyRstatementContext(&p.RstatementContext)
	p.parser = parser
	p.CopyAll(ctx.(*RstatementContext))

	return p
}

func (s *ForContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *ForContext) FOR() antlr.TerminalNode {
	return s.GetToken(PainlessParserFOR, 0)
}

func (s *ForContext) LP() antlr.TerminalNode {
	return s.GetToken(PainlessParserLP, 0)
}

func (s *ForContext) AllSEMICOLON() []antlr.TerminalNode {
	return s.GetTokens(PainlessParserSEMICOLON)
}

func (s *ForContext) SEMICOLON(i int) antlr.TerminalNode {
	return s.GetToken(PainlessParserSEMICOLON, i)
}

func (s *ForContext) RP() antlr.TerminalNode {
	return s.GetToken(PainlessParserRP, 0)
}

func (s *ForContext) Trailer() ITrailerContext {
	var t antlr.RuleContext
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(ITrailerContext); ok {
			t = ctx.(antlr.RuleContext)
			break
		}
	}

	if t == nil {
		return nil
	}

	return t.(ITrailerContext)
}

func (s *ForContext) Empty() IEmptyContext {
	var t antlr.RuleContext
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IEmptyContext); ok {
			t = ctx.(antlr.RuleContext)
			break
		}
	}

	if t == nil {
		return nil
	}

	return t.(IEmptyContext)
}

func (s *ForContext) Initializer() IInitializerContext {
	var t antlr.RuleContext
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IInitializerContext); ok {
			t = ctx.(antlr.RuleContext)
			break
		}
	}

	if t == nil {
		return nil
	}

	return t.(IInitializerContext)
}

func (s *ForContext) Expression() IExpressionContext {
	var t antlr.RuleContext
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IExpressionContext); ok {
			t = ctx.(antlr.RuleContext)
			break
		}
	}

	if t == nil {
		return nil
	}

	return t.(IExpressionContext)
}

func (s *ForContext) Afterthought() IAfterthoughtContext {
	var t antlr.RuleContext
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IAfterthoughtContext); ok {
			t = ctx.(antlr.RuleContext)
			break
		}
	}

	if t == nil {
		return nil
	}

	return t.(IAfterthoughtContext)
}

func (s *ForContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PainlessParserListener); ok {
		listenerT.EnterFor(s)
	}
}

func (s *ForContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PainlessParserListener); ok {
		listenerT.ExitFor(s)
	}
}

func (s *ForContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PainlessParserVisitor:
		return t.VisitFor(s)

	default:
		return t.VisitChildren(s)
	}
}

type TryContext struct {
	RstatementContext
}

func NewTryContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *TryContext {
	var p = new(TryContext)

	InitEmptyRstatementContext(&p.RstatementContext)
	p.parser = parser
	p.CopyAll(ctx.(*RstatementContext))

	return p
}

func (s *TryContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *TryContext) TRY() antlr.TerminalNode {
	return s.GetToken(PainlessParserTRY, 0)
}

func (s *TryContext) Block() IBlockContext {
	var t antlr.RuleContext
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IBlockContext); ok {
			t = ctx.(antlr.RuleContext)
			break
		}
	}

	if t == nil {
		return nil
	}

	return t.(IBlockContext)
}

func (s *TryContext) AllTrap() []ITrapContext {
	children := s.GetChildren()
	len := 0
	for _, ctx := range children {
		if _, ok := ctx.(ITrapContext); ok {
			len++
		}
	}

	tst := make([]ITrapContext, len)
	i := 0
	for _, ctx := range children {
		if t, ok := ctx.(ITrapContext); ok {
			tst[i] = t.(ITrapContext)
			i++
		}
	}

	return tst
}

func (s *TryContext) Trap(i int) ITrapContext {
	var t antlr.RuleContext
	j := 0
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(ITrapContext); ok {
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

	return t.(ITrapContext)
}

func (s *TryContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PainlessParserListener); ok {
		listenerT.EnterTry(s)
	}
}

func (s *TryContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PainlessParserListener); ok {
		listenerT.ExitTry(s)
	}
}

func (s *TryContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PainlessParserVisitor:
		return t.VisitTry(s)

	default:
		return t.VisitChildren(s)
	}
}

type WhileContext struct {
	RstatementContext
}

func NewWhileContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *WhileContext {
	var p = new(WhileContext)

	InitEmptyRstatementContext(&p.RstatementContext)
	p.parser = parser
	p.CopyAll(ctx.(*RstatementContext))

	return p
}

func (s *WhileContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *WhileContext) WHILE() antlr.TerminalNode {
	return s.GetToken(PainlessParserWHILE, 0)
}

func (s *WhileContext) LP() antlr.TerminalNode {
	return s.GetToken(PainlessParserLP, 0)
}

func (s *WhileContext) Expression() IExpressionContext {
	var t antlr.RuleContext
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IExpressionContext); ok {
			t = ctx.(antlr.RuleContext)
			break
		}
	}

	if t == nil {
		return nil
	}

	return t.(IExpressionContext)
}

func (s *WhileContext) RP() antlr.TerminalNode {
	return s.GetToken(PainlessParserRP, 0)
}

func (s *WhileContext) Trailer() ITrailerContext {
	var t antlr.RuleContext
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(ITrailerContext); ok {
			t = ctx.(antlr.RuleContext)
			break
		}
	}

	if t == nil {
		return nil
	}

	return t.(ITrailerContext)
}

func (s *WhileContext) Empty() IEmptyContext {
	var t antlr.RuleContext
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IEmptyContext); ok {
			t = ctx.(antlr.RuleContext)
			break
		}
	}

	if t == nil {
		return nil
	}

	return t.(IEmptyContext)
}

func (s *WhileContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PainlessParserListener); ok {
		listenerT.EnterWhile(s)
	}
}

func (s *WhileContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PainlessParserListener); ok {
		listenerT.ExitWhile(s)
	}
}

func (s *WhileContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PainlessParserVisitor:
		return t.VisitWhile(s)

	default:
		return t.VisitChildren(s)
	}
}

type IneachContext struct {
	RstatementContext
}

func NewIneachContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *IneachContext {
	var p = new(IneachContext)

	InitEmptyRstatementContext(&p.RstatementContext)
	p.parser = parser
	p.CopyAll(ctx.(*RstatementContext))

	return p
}

func (s *IneachContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *IneachContext) FOR() antlr.TerminalNode {
	return s.GetToken(PainlessParserFOR, 0)
}

func (s *IneachContext) LP() antlr.TerminalNode {
	return s.GetToken(PainlessParserLP, 0)
}

func (s *IneachContext) ID() antlr.TerminalNode {
	return s.GetToken(PainlessParserID, 0)
}

func (s *IneachContext) IN() antlr.TerminalNode {
	return s.GetToken(PainlessParserIN, 0)
}

func (s *IneachContext) Expression() IExpressionContext {
	var t antlr.RuleContext
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IExpressionContext); ok {
			t = ctx.(antlr.RuleContext)
			break
		}
	}

	if t == nil {
		return nil
	}

	return t.(IExpressionContext)
}

func (s *IneachContext) RP() antlr.TerminalNode {
	return s.GetToken(PainlessParserRP, 0)
}

func (s *IneachContext) Trailer() ITrailerContext {
	var t antlr.RuleContext
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(ITrailerContext); ok {
			t = ctx.(antlr.RuleContext)
			break
		}
	}

	if t == nil {
		return nil
	}

	return t.(ITrailerContext)
}

func (s *IneachContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PainlessParserListener); ok {
		listenerT.EnterIneach(s)
	}
}

func (s *IneachContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PainlessParserListener); ok {
		listenerT.ExitIneach(s)
	}
}

func (s *IneachContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PainlessParserVisitor:
		return t.VisitIneach(s)

	default:
		return t.VisitChildren(s)
	}
}

type IfContext struct {
	RstatementContext
}

func NewIfContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *IfContext {
	var p = new(IfContext)

	InitEmptyRstatementContext(&p.RstatementContext)
	p.parser = parser
	p.CopyAll(ctx.(*RstatementContext))

	return p
}

func (s *IfContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *IfContext) IF() antlr.TerminalNode {
	return s.GetToken(PainlessParserIF, 0)
}

func (s *IfContext) LP() antlr.TerminalNode {
	return s.GetToken(PainlessParserLP, 0)
}

func (s *IfContext) Expression() IExpressionContext {
	var t antlr.RuleContext
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IExpressionContext); ok {
			t = ctx.(antlr.RuleContext)
			break
		}
	}

	if t == nil {
		return nil
	}

	return t.(IExpressionContext)
}

func (s *IfContext) RP() antlr.TerminalNode {
	return s.GetToken(PainlessParserRP, 0)
}

func (s *IfContext) AllTrailer() []ITrailerContext {
	children := s.GetChildren()
	len := 0
	for _, ctx := range children {
		if _, ok := ctx.(ITrailerContext); ok {
			len++
		}
	}

	tst := make([]ITrailerContext, len)
	i := 0
	for _, ctx := range children {
		if t, ok := ctx.(ITrailerContext); ok {
			tst[i] = t.(ITrailerContext)
			i++
		}
	}

	return tst
}

func (s *IfContext) Trailer(i int) ITrailerContext {
	var t antlr.RuleContext
	j := 0
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(ITrailerContext); ok {
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

	return t.(ITrailerContext)
}

func (s *IfContext) ELSE() antlr.TerminalNode {
	return s.GetToken(PainlessParserELSE, 0)
}

func (s *IfContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PainlessParserListener); ok {
		listenerT.EnterIf(s)
	}
}

func (s *IfContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PainlessParserListener); ok {
		listenerT.ExitIf(s)
	}
}

func (s *IfContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PainlessParserVisitor:
		return t.VisitIf(s)

	default:
		return t.VisitChildren(s)
	}
}

type EachContext struct {
	RstatementContext
}

func NewEachContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *EachContext {
	var p = new(EachContext)

	InitEmptyRstatementContext(&p.RstatementContext)
	p.parser = parser
	p.CopyAll(ctx.(*RstatementContext))

	return p
}

func (s *EachContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *EachContext) FOR() antlr.TerminalNode {
	return s.GetToken(PainlessParserFOR, 0)
}

func (s *EachContext) LP() antlr.TerminalNode {
	return s.GetToken(PainlessParserLP, 0)
}

func (s *EachContext) Decltype() IDecltypeContext {
	var t antlr.RuleContext
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IDecltypeContext); ok {
			t = ctx.(antlr.RuleContext)
			break
		}
	}

	if t == nil {
		return nil
	}

	return t.(IDecltypeContext)
}

func (s *EachContext) ID() antlr.TerminalNode {
	return s.GetToken(PainlessParserID, 0)
}

func (s *EachContext) COLON() antlr.TerminalNode {
	return s.GetToken(PainlessParserCOLON, 0)
}

func (s *EachContext) Expression() IExpressionContext {
	var t antlr.RuleContext
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IExpressionContext); ok {
			t = ctx.(antlr.RuleContext)
			break
		}
	}

	if t == nil {
		return nil
	}

	return t.(IExpressionContext)
}

func (s *EachContext) RP() antlr.TerminalNode {
	return s.GetToken(PainlessParserRP, 0)
}

func (s *EachContext) Trailer() ITrailerContext {
	var t antlr.RuleContext
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(ITrailerContext); ok {
			t = ctx.(antlr.RuleContext)
			break
		}
	}

	if t == nil {
		return nil
	}

	return t.(ITrailerContext)
}

func (s *EachContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PainlessParserListener); ok {
		listenerT.EnterEach(s)
	}
}

func (s *EachContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PainlessParserListener); ok {
		listenerT.ExitEach(s)
	}
}

func (s *EachContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PainlessParserVisitor:
		return t.VisitEach(s)

	default:
		return t.VisitChildren(s)
	}
}

func (p *PainlessParser) Rstatement() (localctx IRstatementContext) {
	localctx = NewRstatementContext(p, p.GetParserRuleContext(), p.GetState())
	p.EnterRule(localctx, 8, PainlessParserRULE_rstatement)
	var _la int

	p.SetState(177)
	p.GetErrorHandler().Sync(p)
	if p.HasError() {
		goto errorExit
	}

	switch p.GetInterpreter().AdaptivePredict(p.BaseParser, p.GetTokenStream(), 11, p.GetParserRuleContext()) {
	case 1:
		localctx = NewIfContext(p, localctx)
		p.EnterOuterAlt(localctx, 1)
		{
			p.SetState(119)
			p.Match(PainlessParserIF)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(120)
			p.Match(PainlessParserLP)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(121)
			p.Expression()
		}
		{
			p.SetState(122)
			p.Match(PainlessParserRP)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(123)
			p.Trailer()
		}

		{
			p.SetState(124)
			p.Match(PainlessParserELSE)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(125)
			p.Trailer()
		}

	case 2:
		localctx = NewWhileContext(p, localctx)
		p.EnterOuterAlt(localctx, 2)
		{
			p.SetState(127)
			p.Match(PainlessParserWHILE)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(128)
			p.Match(PainlessParserLP)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(129)
			p.Expression()
		}
		{
			p.SetState(130)
			p.Match(PainlessParserRP)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		p.SetState(133)
		p.GetErrorHandler().Sync(p)
		if p.HasError() {
			goto errorExit
		}

		switch p.GetTokenStream().LA(1) {
		case PainlessParserLBRACK, PainlessParserLBRACE, PainlessParserLP, PainlessParserIF, PainlessParserWHILE, PainlessParserDO, PainlessParserFOR, PainlessParserCONTINUE, PainlessParserBREAK, PainlessParserRETURN, PainlessParserNEW, PainlessParserTRY, PainlessParserTHROW, PainlessParserBOOLNOT, PainlessParserBWNOT, PainlessParserADD, PainlessParserSUB, PainlessParserINCR, PainlessParserDECR, PainlessParserOCTAL, PainlessParserHEX, PainlessParserINTEGER, PainlessParserDECIMAL, PainlessParserSTRING, PainlessParserREGEX, PainlessParserTRUE, PainlessParserFALSE, PainlessParserNULL, PainlessParserPRIMITIVE, PainlessParserDEF, PainlessParserID:
			{
				p.SetState(131)
				p.Trailer()
			}

		case PainlessParserSEMICOLON:
			{
				p.SetState(132)
				p.Empty()
			}

		default:
			p.SetError(antlr.NewNoViableAltException(p, nil, nil, nil, nil, nil))
			goto errorExit
		}

	case 3:
		localctx = NewForContext(p, localctx)
		p.EnterOuterAlt(localctx, 3)
		{
			p.SetState(135)
			p.Match(PainlessParserFOR)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(136)
			p.Match(PainlessParserLP)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		p.SetState(138)
		p.GetErrorHandler().Sync(p)
		if p.HasError() {
			goto errorExit
		}
		_la = p.GetTokenStream().LA(1)

		if ((int64(_la) & ^0x3f) == 0 && ((int64(1)<<_la)&864691155034439840) != 0) || ((int64((_la-72)) & ^0x3f) == 0 && ((int64(1)<<(_la-72))&4095) != 0) {
			{
				p.SetState(137)
				p.Initializer()
			}

		}
		{
			p.SetState(140)
			p.Match(PainlessParserSEMICOLON)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		p.SetState(142)
		p.GetErrorHandler().Sync(p)
		if p.HasError() {
			goto errorExit
		}
		_la = p.GetTokenStream().LA(1)

		if ((int64(_la) & ^0x3f) == 0 && ((int64(1)<<_la)&864691155034439840) != 0) || ((int64((_la-72)) & ^0x3f) == 0 && ((int64(1)<<(_la-72))&2559) != 0) {
			{
				p.SetState(141)
				p.Expression()
			}

		}
		{
			p.SetState(144)
			p.Match(PainlessParserSEMICOLON)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		p.SetState(146)
		p.GetErrorHandler().Sync(p)
		if p.HasError() {
			goto errorExit
		}
		_la = p.GetTokenStream().LA(1)

		if ((int64(_la) & ^0x3f) == 0 && ((int64(1)<<_la)&864691155034439840) != 0) || ((int64((_la-72)) & ^0x3f) == 0 && ((int64(1)<<(_la-72))&2559) != 0) {
			{
				p.SetState(145)
				p.Afterthought()
			}

		}
		{
			p.SetState(148)
			p.Match(PainlessParserRP)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		p.SetState(151)
		p.GetErrorHandler().Sync(p)
		if p.HasError() {
			goto errorExit
		}

		switch p.GetTokenStream().LA(1) {
		case PainlessParserLBRACK, PainlessParserLBRACE, PainlessParserLP, PainlessParserIF, PainlessParserWHILE, PainlessParserDO, PainlessParserFOR, PainlessParserCONTINUE, PainlessParserBREAK, PainlessParserRETURN, PainlessParserNEW, PainlessParserTRY, PainlessParserTHROW, PainlessParserBOOLNOT, PainlessParserBWNOT, PainlessParserADD, PainlessParserSUB, PainlessParserINCR, PainlessParserDECR, PainlessParserOCTAL, PainlessParserHEX, PainlessParserINTEGER, PainlessParserDECIMAL, PainlessParserSTRING, PainlessParserREGEX, PainlessParserTRUE, PainlessParserFALSE, PainlessParserNULL, PainlessParserPRIMITIVE, PainlessParserDEF, PainlessParserID:
			{
				p.SetState(149)
				p.Trailer()
			}

		case PainlessParserSEMICOLON:
			{
				p.SetState(150)
				p.Empty()
			}

		default:
			p.SetError(antlr.NewNoViableAltException(p, nil, nil, nil, nil, nil))
			goto errorExit
		}

	case 4:
		localctx = NewEachContext(p, localctx)
		p.EnterOuterAlt(localctx, 4)
		{
			p.SetState(153)
			p.Match(PainlessParserFOR)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(154)
			p.Match(PainlessParserLP)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(155)
			p.Decltype()
		}
		{
			p.SetState(156)
			p.Match(PainlessParserID)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(157)
			p.Match(PainlessParserCOLON)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(158)
			p.Expression()
		}
		{
			p.SetState(159)
			p.Match(PainlessParserRP)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(160)
			p.Trailer()
		}

	case 5:
		localctx = NewIneachContext(p, localctx)
		p.EnterOuterAlt(localctx, 5)
		{
			p.SetState(162)
			p.Match(PainlessParserFOR)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(163)
			p.Match(PainlessParserLP)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(164)
			p.Match(PainlessParserID)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(165)
			p.Match(PainlessParserIN)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(166)
			p.Expression()
		}
		{
			p.SetState(167)
			p.Match(PainlessParserRP)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(168)
			p.Trailer()
		}

	case 6:
		localctx = NewTryContext(p, localctx)
		p.EnterOuterAlt(localctx, 6)
		{
			p.SetState(170)
			p.Match(PainlessParserTRY)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(171)
			p.Block()
		}
		p.SetState(173)
		p.GetErrorHandler().Sync(p)
		if p.HasError() {
			goto errorExit
		}
		_la = p.GetTokenStream().LA(1)

		for ok := true; ok; ok = _la == PainlessParserCATCH {
			{
				p.SetState(172)
				p.Trap()
			}

			p.SetState(175)
			p.GetErrorHandler().Sync(p)
			if p.HasError() {
				goto errorExit
			}
			_la = p.GetTokenStream().LA(1)
		}

	case antlr.ATNInvalidAltNumber:
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
	return localctx
	goto errorExit // Trick to prevent compiler error if the label is not used
}

// IDstatementContext is an interface to support dynamic dispatch.
type IDstatementContext interface {
	antlr.ParserRuleContext

	// GetParser returns the parser.
	GetParser() antlr.Parser
	// IsDstatementContext differentiates from other interfaces.
	IsDstatementContext()
}

type DstatementContext struct {
	antlr.BaseParserRuleContext
	parser antlr.Parser
}

func NewEmptyDstatementContext() *DstatementContext {
	var p = new(DstatementContext)
	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, nil, -1)
	p.RuleIndex = PainlessParserRULE_dstatement
	return p
}

func InitEmptyDstatementContext(p *DstatementContext) {
	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, nil, -1)
	p.RuleIndex = PainlessParserRULE_dstatement
}

func (*DstatementContext) IsDstatementContext() {}

func NewDstatementContext(parser antlr.Parser, parent antlr.ParserRuleContext, invokingState int) *DstatementContext {
	var p = new(DstatementContext)

	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, parent, invokingState)

	p.parser = parser
	p.RuleIndex = PainlessParserRULE_dstatement

	return p
}

func (s *DstatementContext) GetParser() antlr.Parser { return s.parser }

func (s *DstatementContext) CopyAll(ctx *DstatementContext) {
	s.CopyFrom(&ctx.BaseParserRuleContext)
}

func (s *DstatementContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *DstatementContext) ToStringTree(ruleNames []string, recog antlr.Recognizer) string {
	return antlr.TreesStringTree(s, ruleNames, recog)
}

type DeclContext struct {
	DstatementContext
}

func NewDeclContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *DeclContext {
	var p = new(DeclContext)

	InitEmptyDstatementContext(&p.DstatementContext)
	p.parser = parser
	p.CopyAll(ctx.(*DstatementContext))

	return p
}

func (s *DeclContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *DeclContext) Declaration() IDeclarationContext {
	var t antlr.RuleContext
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IDeclarationContext); ok {
			t = ctx.(antlr.RuleContext)
			break
		}
	}

	if t == nil {
		return nil
	}

	return t.(IDeclarationContext)
}

func (s *DeclContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PainlessParserListener); ok {
		listenerT.EnterDecl(s)
	}
}

func (s *DeclContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PainlessParserListener); ok {
		listenerT.ExitDecl(s)
	}
}

func (s *DeclContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PainlessParserVisitor:
		return t.VisitDecl(s)

	default:
		return t.VisitChildren(s)
	}
}

type BreakContext struct {
	DstatementContext
}

func NewBreakContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *BreakContext {
	var p = new(BreakContext)

	InitEmptyDstatementContext(&p.DstatementContext)
	p.parser = parser
	p.CopyAll(ctx.(*DstatementContext))

	return p
}

func (s *BreakContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *BreakContext) BREAK() antlr.TerminalNode {
	return s.GetToken(PainlessParserBREAK, 0)
}

func (s *BreakContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PainlessParserListener); ok {
		listenerT.EnterBreak(s)
	}
}

func (s *BreakContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PainlessParserListener); ok {
		listenerT.ExitBreak(s)
	}
}

func (s *BreakContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PainlessParserVisitor:
		return t.VisitBreak(s)

	default:
		return t.VisitChildren(s)
	}
}

type ThrowContext struct {
	DstatementContext
}

func NewThrowContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *ThrowContext {
	var p = new(ThrowContext)

	InitEmptyDstatementContext(&p.DstatementContext)
	p.parser = parser
	p.CopyAll(ctx.(*DstatementContext))

	return p
}

func (s *ThrowContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *ThrowContext) THROW() antlr.TerminalNode {
	return s.GetToken(PainlessParserTHROW, 0)
}

func (s *ThrowContext) Expression() IExpressionContext {
	var t antlr.RuleContext
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IExpressionContext); ok {
			t = ctx.(antlr.RuleContext)
			break
		}
	}

	if t == nil {
		return nil
	}

	return t.(IExpressionContext)
}

func (s *ThrowContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PainlessParserListener); ok {
		listenerT.EnterThrow(s)
	}
}

func (s *ThrowContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PainlessParserListener); ok {
		listenerT.ExitThrow(s)
	}
}

func (s *ThrowContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PainlessParserVisitor:
		return t.VisitThrow(s)

	default:
		return t.VisitChildren(s)
	}
}

type ContinueContext struct {
	DstatementContext
}

func NewContinueContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *ContinueContext {
	var p = new(ContinueContext)

	InitEmptyDstatementContext(&p.DstatementContext)
	p.parser = parser
	p.CopyAll(ctx.(*DstatementContext))

	return p
}

func (s *ContinueContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *ContinueContext) CONTINUE() antlr.TerminalNode {
	return s.GetToken(PainlessParserCONTINUE, 0)
}

func (s *ContinueContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PainlessParserListener); ok {
		listenerT.EnterContinue(s)
	}
}

func (s *ContinueContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PainlessParserListener); ok {
		listenerT.ExitContinue(s)
	}
}

func (s *ContinueContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PainlessParserVisitor:
		return t.VisitContinue(s)

	default:
		return t.VisitChildren(s)
	}
}

type ExprContext struct {
	DstatementContext
}

func NewExprContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *ExprContext {
	var p = new(ExprContext)

	InitEmptyDstatementContext(&p.DstatementContext)
	p.parser = parser
	p.CopyAll(ctx.(*DstatementContext))

	return p
}

func (s *ExprContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *ExprContext) Expression() IExpressionContext {
	var t antlr.RuleContext
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IExpressionContext); ok {
			t = ctx.(antlr.RuleContext)
			break
		}
	}

	if t == nil {
		return nil
	}

	return t.(IExpressionContext)
}

func (s *ExprContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PainlessParserListener); ok {
		listenerT.EnterExpr(s)
	}
}

func (s *ExprContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PainlessParserListener); ok {
		listenerT.ExitExpr(s)
	}
}

func (s *ExprContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PainlessParserVisitor:
		return t.VisitExpr(s)

	default:
		return t.VisitChildren(s)
	}
}

type DoContext struct {
	DstatementContext
}

func NewDoContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *DoContext {
	var p = new(DoContext)

	InitEmptyDstatementContext(&p.DstatementContext)
	p.parser = parser
	p.CopyAll(ctx.(*DstatementContext))

	return p
}

func (s *DoContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *DoContext) DO() antlr.TerminalNode {
	return s.GetToken(PainlessParserDO, 0)
}

func (s *DoContext) Block() IBlockContext {
	var t antlr.RuleContext
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IBlockContext); ok {
			t = ctx.(antlr.RuleContext)
			break
		}
	}

	if t == nil {
		return nil
	}

	return t.(IBlockContext)
}

func (s *DoContext) WHILE() antlr.TerminalNode {
	return s.GetToken(PainlessParserWHILE, 0)
}

func (s *DoContext) LP() antlr.TerminalNode {
	return s.GetToken(PainlessParserLP, 0)
}

func (s *DoContext) Expression() IExpressionContext {
	var t antlr.RuleContext
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IExpressionContext); ok {
			t = ctx.(antlr.RuleContext)
			break
		}
	}

	if t == nil {
		return nil
	}

	return t.(IExpressionContext)
}

func (s *DoContext) RP() antlr.TerminalNode {
	return s.GetToken(PainlessParserRP, 0)
}

func (s *DoContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PainlessParserListener); ok {
		listenerT.EnterDo(s)
	}
}

func (s *DoContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PainlessParserListener); ok {
		listenerT.ExitDo(s)
	}
}

func (s *DoContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PainlessParserVisitor:
		return t.VisitDo(s)

	default:
		return t.VisitChildren(s)
	}
}

type ReturnContext struct {
	DstatementContext
}

func NewReturnContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *ReturnContext {
	var p = new(ReturnContext)

	InitEmptyDstatementContext(&p.DstatementContext)
	p.parser = parser
	p.CopyAll(ctx.(*DstatementContext))

	return p
}

func (s *ReturnContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *ReturnContext) RETURN() antlr.TerminalNode {
	return s.GetToken(PainlessParserRETURN, 0)
}

func (s *ReturnContext) Expression() IExpressionContext {
	var t antlr.RuleContext
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IExpressionContext); ok {
			t = ctx.(antlr.RuleContext)
			break
		}
	}

	if t == nil {
		return nil
	}

	return t.(IExpressionContext)
}

func (s *ReturnContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PainlessParserListener); ok {
		listenerT.EnterReturn(s)
	}
}

func (s *ReturnContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PainlessParserListener); ok {
		listenerT.ExitReturn(s)
	}
}

func (s *ReturnContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PainlessParserVisitor:
		return t.VisitReturn(s)

	default:
		return t.VisitChildren(s)
	}
}

func (p *PainlessParser) Dstatement() (localctx IDstatementContext) {
	localctx = NewDstatementContext(p, p.GetParserRuleContext(), p.GetState())
	p.EnterRule(localctx, 10, PainlessParserRULE_dstatement)
	var _la int

	p.SetState(196)
	p.GetErrorHandler().Sync(p)
	if p.HasError() {
		goto errorExit
	}

	switch p.GetInterpreter().AdaptivePredict(p.BaseParser, p.GetTokenStream(), 13, p.GetParserRuleContext()) {
	case 1:
		localctx = NewDoContext(p, localctx)
		p.EnterOuterAlt(localctx, 1)
		{
			p.SetState(179)
			p.Match(PainlessParserDO)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(180)
			p.Block()
		}
		{
			p.SetState(181)
			p.Match(PainlessParserWHILE)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(182)
			p.Match(PainlessParserLP)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(183)
			p.Expression()
		}
		{
			p.SetState(184)
			p.Match(PainlessParserRP)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}

	case 2:
		localctx = NewDeclContext(p, localctx)
		p.EnterOuterAlt(localctx, 2)
		{
			p.SetState(186)
			p.Declaration()
		}

	case 3:
		localctx = NewContinueContext(p, localctx)
		p.EnterOuterAlt(localctx, 3)
		{
			p.SetState(187)
			p.Match(PainlessParserCONTINUE)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}

	case 4:
		localctx = NewBreakContext(p, localctx)
		p.EnterOuterAlt(localctx, 4)
		{
			p.SetState(188)
			p.Match(PainlessParserBREAK)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}

	case 5:
		localctx = NewReturnContext(p, localctx)
		p.EnterOuterAlt(localctx, 5)
		{
			p.SetState(189)
			p.Match(PainlessParserRETURN)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		p.SetState(191)
		p.GetErrorHandler().Sync(p)
		if p.HasError() {
			goto errorExit
		}
		_la = p.GetTokenStream().LA(1)

		if ((int64(_la) & ^0x3f) == 0 && ((int64(1)<<_la)&864691155034439840) != 0) || ((int64((_la-72)) & ^0x3f) == 0 && ((int64(1)<<(_la-72))&2559) != 0) {
			{
				p.SetState(190)
				p.Expression()
			}

		}

	case 6:
		localctx = NewThrowContext(p, localctx)
		p.EnterOuterAlt(localctx, 6)
		{
			p.SetState(193)
			p.Match(PainlessParserTHROW)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(194)
			p.Expression()
		}

	case 7:
		localctx = NewExprContext(p, localctx)
		p.EnterOuterAlt(localctx, 7)
		{
			p.SetState(195)
			p.Expression()
		}

	case antlr.ATNInvalidAltNumber:
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
	return localctx
	goto errorExit // Trick to prevent compiler error if the label is not used
}

// ITrailerContext is an interface to support dynamic dispatch.
type ITrailerContext interface {
	antlr.ParserRuleContext

	// GetParser returns the parser.
	GetParser() antlr.Parser

	// Getter signatures
	Block() IBlockContext
	Statement() IStatementContext

	// IsTrailerContext differentiates from other interfaces.
	IsTrailerContext()
}

type TrailerContext struct {
	antlr.BaseParserRuleContext
	parser antlr.Parser
}

func NewEmptyTrailerContext() *TrailerContext {
	var p = new(TrailerContext)
	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, nil, -1)
	p.RuleIndex = PainlessParserRULE_trailer
	return p
}

func InitEmptyTrailerContext(p *TrailerContext) {
	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, nil, -1)
	p.RuleIndex = PainlessParserRULE_trailer
}

func (*TrailerContext) IsTrailerContext() {}

func NewTrailerContext(parser antlr.Parser, parent antlr.ParserRuleContext, invokingState int) *TrailerContext {
	var p = new(TrailerContext)

	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, parent, invokingState)

	p.parser = parser
	p.RuleIndex = PainlessParserRULE_trailer

	return p
}

func (s *TrailerContext) GetParser() antlr.Parser { return s.parser }

func (s *TrailerContext) Block() IBlockContext {
	var t antlr.RuleContext
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IBlockContext); ok {
			t = ctx.(antlr.RuleContext)
			break
		}
	}

	if t == nil {
		return nil
	}

	return t.(IBlockContext)
}

func (s *TrailerContext) Statement() IStatementContext {
	var t antlr.RuleContext
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IStatementContext); ok {
			t = ctx.(antlr.RuleContext)
			break
		}
	}

	if t == nil {
		return nil
	}

	return t.(IStatementContext)
}

func (s *TrailerContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *TrailerContext) ToStringTree(ruleNames []string, recog antlr.Recognizer) string {
	return antlr.TreesStringTree(s, ruleNames, recog)
}

func (s *TrailerContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PainlessParserListener); ok {
		listenerT.EnterTrailer(s)
	}
}

func (s *TrailerContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PainlessParserListener); ok {
		listenerT.ExitTrailer(s)
	}
}

func (s *TrailerContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PainlessParserVisitor:
		return t.VisitTrailer(s)

	default:
		return t.VisitChildren(s)
	}
}

func (p *PainlessParser) Trailer() (localctx ITrailerContext) {
	localctx = NewTrailerContext(p, p.GetParserRuleContext(), p.GetState())
	p.EnterRule(localctx, 12, PainlessParserRULE_trailer)
	p.SetState(200)
	p.GetErrorHandler().Sync(p)
	if p.HasError() {
		goto errorExit
	}

	switch p.GetTokenStream().LA(1) {
	case PainlessParserLBRACK:
		p.EnterOuterAlt(localctx, 1)
		{
			p.SetState(198)
			p.Block()
		}

	case PainlessParserLBRACE, PainlessParserLP, PainlessParserIF, PainlessParserWHILE, PainlessParserDO, PainlessParserFOR, PainlessParserCONTINUE, PainlessParserBREAK, PainlessParserRETURN, PainlessParserNEW, PainlessParserTRY, PainlessParserTHROW, PainlessParserBOOLNOT, PainlessParserBWNOT, PainlessParserADD, PainlessParserSUB, PainlessParserINCR, PainlessParserDECR, PainlessParserOCTAL, PainlessParserHEX, PainlessParserINTEGER, PainlessParserDECIMAL, PainlessParserSTRING, PainlessParserREGEX, PainlessParserTRUE, PainlessParserFALSE, PainlessParserNULL, PainlessParserPRIMITIVE, PainlessParserDEF, PainlessParserID:
		p.EnterOuterAlt(localctx, 2)
		{
			p.SetState(199)
			p.Statement()
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
	return localctx
	goto errorExit // Trick to prevent compiler error if the label is not used
}

// IBlockContext is an interface to support dynamic dispatch.
type IBlockContext interface {
	antlr.ParserRuleContext

	// GetParser returns the parser.
	GetParser() antlr.Parser

	// Getter signatures
	LBRACK() antlr.TerminalNode
	RBRACK() antlr.TerminalNode
	AllStatement() []IStatementContext
	Statement(i int) IStatementContext
	Dstatement() IDstatementContext

	// IsBlockContext differentiates from other interfaces.
	IsBlockContext()
}

type BlockContext struct {
	antlr.BaseParserRuleContext
	parser antlr.Parser
}

func NewEmptyBlockContext() *BlockContext {
	var p = new(BlockContext)
	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, nil, -1)
	p.RuleIndex = PainlessParserRULE_block
	return p
}

func InitEmptyBlockContext(p *BlockContext) {
	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, nil, -1)
	p.RuleIndex = PainlessParserRULE_block
}

func (*BlockContext) IsBlockContext() {}

func NewBlockContext(parser antlr.Parser, parent antlr.ParserRuleContext, invokingState int) *BlockContext {
	var p = new(BlockContext)

	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, parent, invokingState)

	p.parser = parser
	p.RuleIndex = PainlessParserRULE_block

	return p
}

func (s *BlockContext) GetParser() antlr.Parser { return s.parser }

func (s *BlockContext) LBRACK() antlr.TerminalNode {
	return s.GetToken(PainlessParserLBRACK, 0)
}

func (s *BlockContext) RBRACK() antlr.TerminalNode {
	return s.GetToken(PainlessParserRBRACK, 0)
}

func (s *BlockContext) AllStatement() []IStatementContext {
	children := s.GetChildren()
	len := 0
	for _, ctx := range children {
		if _, ok := ctx.(IStatementContext); ok {
			len++
		}
	}

	tst := make([]IStatementContext, len)
	i := 0
	for _, ctx := range children {
		if t, ok := ctx.(IStatementContext); ok {
			tst[i] = t.(IStatementContext)
			i++
		}
	}

	return tst
}

func (s *BlockContext) Statement(i int) IStatementContext {
	var t antlr.RuleContext
	j := 0
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IStatementContext); ok {
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

	return t.(IStatementContext)
}

func (s *BlockContext) Dstatement() IDstatementContext {
	var t antlr.RuleContext
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IDstatementContext); ok {
			t = ctx.(antlr.RuleContext)
			break
		}
	}

	if t == nil {
		return nil
	}

	return t.(IDstatementContext)
}

func (s *BlockContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *BlockContext) ToStringTree(ruleNames []string, recog antlr.Recognizer) string {
	return antlr.TreesStringTree(s, ruleNames, recog)
}

func (s *BlockContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PainlessParserListener); ok {
		listenerT.EnterBlock(s)
	}
}

func (s *BlockContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PainlessParserListener); ok {
		listenerT.ExitBlock(s)
	}
}

func (s *BlockContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PainlessParserVisitor:
		return t.VisitBlock(s)

	default:
		return t.VisitChildren(s)
	}
}

func (p *PainlessParser) Block() (localctx IBlockContext) {
	localctx = NewBlockContext(p, p.GetParserRuleContext(), p.GetState())
	p.EnterRule(localctx, 14, PainlessParserRULE_block)
	var _la int

	var _alt int

	p.EnterOuterAlt(localctx, 1)
	{
		p.SetState(202)
		p.Match(PainlessParserLBRACK)
		if p.HasError() {
			// Recognition error - abort rule
			goto errorExit
		}
	}
	p.SetState(206)
	p.GetErrorHandler().Sync(p)
	if p.HasError() {
		goto errorExit
	}
	_alt = p.GetInterpreter().AdaptivePredict(p.BaseParser, p.GetTokenStream(), 15, p.GetParserRuleContext())
	if p.HasError() {
		goto errorExit
	}
	for _alt != 2 && _alt != antlr.ATNInvalidAltNumber {
		if _alt == 1 {
			{
				p.SetState(203)
				p.Statement()
			}

		}
		p.SetState(208)
		p.GetErrorHandler().Sync(p)
		if p.HasError() {
			goto errorExit
		}
		_alt = p.GetInterpreter().AdaptivePredict(p.BaseParser, p.GetTokenStream(), 15, p.GetParserRuleContext())
		if p.HasError() {
			goto errorExit
		}
	}
	p.SetState(210)
	p.GetErrorHandler().Sync(p)
	if p.HasError() {
		goto errorExit
	}
	_la = p.GetTokenStream().LA(1)

	if ((int64(_la) & ^0x3f) == 0 && ((int64(1)<<_la)&864691155071795360) != 0) || ((int64((_la-72)) & ^0x3f) == 0 && ((int64(1)<<(_la-72))&4095) != 0) {
		{
			p.SetState(209)
			p.Dstatement()
		}

	}
	{
		p.SetState(212)
		p.Match(PainlessParserRBRACK)
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
	return localctx
	goto errorExit // Trick to prevent compiler error if the label is not used
}

// IEmptyContext is an interface to support dynamic dispatch.
type IEmptyContext interface {
	antlr.ParserRuleContext

	// GetParser returns the parser.
	GetParser() antlr.Parser

	// Getter signatures
	SEMICOLON() antlr.TerminalNode

	// IsEmptyContext differentiates from other interfaces.
	IsEmptyContext()
}

type EmptyContext struct {
	antlr.BaseParserRuleContext
	parser antlr.Parser
}

func NewEmptyEmptyContext() *EmptyContext {
	var p = new(EmptyContext)
	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, nil, -1)
	p.RuleIndex = PainlessParserRULE_empty
	return p
}

func InitEmptyEmptyContext(p *EmptyContext) {
	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, nil, -1)
	p.RuleIndex = PainlessParserRULE_empty
}

func (*EmptyContext) IsEmptyContext() {}

func NewEmptyContext(parser antlr.Parser, parent antlr.ParserRuleContext, invokingState int) *EmptyContext {
	var p = new(EmptyContext)

	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, parent, invokingState)

	p.parser = parser
	p.RuleIndex = PainlessParserRULE_empty

	return p
}

func (s *EmptyContext) GetParser() antlr.Parser { return s.parser }

func (s *EmptyContext) SEMICOLON() antlr.TerminalNode {
	return s.GetToken(PainlessParserSEMICOLON, 0)
}

func (s *EmptyContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *EmptyContext) ToStringTree(ruleNames []string, recog antlr.Recognizer) string {
	return antlr.TreesStringTree(s, ruleNames, recog)
}

func (s *EmptyContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PainlessParserListener); ok {
		listenerT.EnterEmpty(s)
	}
}

func (s *EmptyContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PainlessParserListener); ok {
		listenerT.ExitEmpty(s)
	}
}

func (s *EmptyContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PainlessParserVisitor:
		return t.VisitEmpty(s)

	default:
		return t.VisitChildren(s)
	}
}

func (p *PainlessParser) Empty() (localctx IEmptyContext) {
	localctx = NewEmptyContext(p, p.GetParserRuleContext(), p.GetState())
	p.EnterRule(localctx, 16, PainlessParserRULE_empty)
	p.EnterOuterAlt(localctx, 1)
	{
		p.SetState(214)
		p.Match(PainlessParserSEMICOLON)
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
	return localctx
	goto errorExit // Trick to prevent compiler error if the label is not used
}

// IInitializerContext is an interface to support dynamic dispatch.
type IInitializerContext interface {
	antlr.ParserRuleContext

	// GetParser returns the parser.
	GetParser() antlr.Parser

	// Getter signatures
	Declaration() IDeclarationContext
	Expression() IExpressionContext

	// IsInitializerContext differentiates from other interfaces.
	IsInitializerContext()
}

type InitializerContext struct {
	antlr.BaseParserRuleContext
	parser antlr.Parser
}

func NewEmptyInitializerContext() *InitializerContext {
	var p = new(InitializerContext)
	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, nil, -1)
	p.RuleIndex = PainlessParserRULE_initializer
	return p
}

func InitEmptyInitializerContext(p *InitializerContext) {
	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, nil, -1)
	p.RuleIndex = PainlessParserRULE_initializer
}

func (*InitializerContext) IsInitializerContext() {}

func NewInitializerContext(parser antlr.Parser, parent antlr.ParserRuleContext, invokingState int) *InitializerContext {
	var p = new(InitializerContext)

	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, parent, invokingState)

	p.parser = parser
	p.RuleIndex = PainlessParserRULE_initializer

	return p
}

func (s *InitializerContext) GetParser() antlr.Parser { return s.parser }

func (s *InitializerContext) Declaration() IDeclarationContext {
	var t antlr.RuleContext
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IDeclarationContext); ok {
			t = ctx.(antlr.RuleContext)
			break
		}
	}

	if t == nil {
		return nil
	}

	return t.(IDeclarationContext)
}

func (s *InitializerContext) Expression() IExpressionContext {
	var t antlr.RuleContext
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IExpressionContext); ok {
			t = ctx.(antlr.RuleContext)
			break
		}
	}

	if t == nil {
		return nil
	}

	return t.(IExpressionContext)
}

func (s *InitializerContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *InitializerContext) ToStringTree(ruleNames []string, recog antlr.Recognizer) string {
	return antlr.TreesStringTree(s, ruleNames, recog)
}

func (s *InitializerContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PainlessParserListener); ok {
		listenerT.EnterInitializer(s)
	}
}

func (s *InitializerContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PainlessParserListener); ok {
		listenerT.ExitInitializer(s)
	}
}

func (s *InitializerContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PainlessParserVisitor:
		return t.VisitInitializer(s)

	default:
		return t.VisitChildren(s)
	}
}

func (p *PainlessParser) Initializer() (localctx IInitializerContext) {
	localctx = NewInitializerContext(p, p.GetParserRuleContext(), p.GetState())
	p.EnterRule(localctx, 18, PainlessParserRULE_initializer)
	p.SetState(218)
	p.GetErrorHandler().Sync(p)
	if p.HasError() {
		goto errorExit
	}

	switch p.GetInterpreter().AdaptivePredict(p.BaseParser, p.GetTokenStream(), 17, p.GetParserRuleContext()) {
	case 1:
		p.EnterOuterAlt(localctx, 1)
		{
			p.SetState(216)
			p.Declaration()
		}

	case 2:
		p.EnterOuterAlt(localctx, 2)
		{
			p.SetState(217)
			p.Expression()
		}

	case antlr.ATNInvalidAltNumber:
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
	return localctx
	goto errorExit // Trick to prevent compiler error if the label is not used
}

// IAfterthoughtContext is an interface to support dynamic dispatch.
type IAfterthoughtContext interface {
	antlr.ParserRuleContext

	// GetParser returns the parser.
	GetParser() antlr.Parser

	// Getter signatures
	Expression() IExpressionContext

	// IsAfterthoughtContext differentiates from other interfaces.
	IsAfterthoughtContext()
}

type AfterthoughtContext struct {
	antlr.BaseParserRuleContext
	parser antlr.Parser
}

func NewEmptyAfterthoughtContext() *AfterthoughtContext {
	var p = new(AfterthoughtContext)
	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, nil, -1)
	p.RuleIndex = PainlessParserRULE_afterthought
	return p
}

func InitEmptyAfterthoughtContext(p *AfterthoughtContext) {
	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, nil, -1)
	p.RuleIndex = PainlessParserRULE_afterthought
}

func (*AfterthoughtContext) IsAfterthoughtContext() {}

func NewAfterthoughtContext(parser antlr.Parser, parent antlr.ParserRuleContext, invokingState int) *AfterthoughtContext {
	var p = new(AfterthoughtContext)

	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, parent, invokingState)

	p.parser = parser
	p.RuleIndex = PainlessParserRULE_afterthought

	return p
}

func (s *AfterthoughtContext) GetParser() antlr.Parser { return s.parser }

func (s *AfterthoughtContext) Expression() IExpressionContext {
	var t antlr.RuleContext
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IExpressionContext); ok {
			t = ctx.(antlr.RuleContext)
			break
		}
	}

	if t == nil {
		return nil
	}

	return t.(IExpressionContext)
}

func (s *AfterthoughtContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *AfterthoughtContext) ToStringTree(ruleNames []string, recog antlr.Recognizer) string {
	return antlr.TreesStringTree(s, ruleNames, recog)
}

func (s *AfterthoughtContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PainlessParserListener); ok {
		listenerT.EnterAfterthought(s)
	}
}

func (s *AfterthoughtContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PainlessParserListener); ok {
		listenerT.ExitAfterthought(s)
	}
}

func (s *AfterthoughtContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PainlessParserVisitor:
		return t.VisitAfterthought(s)

	default:
		return t.VisitChildren(s)
	}
}

func (p *PainlessParser) Afterthought() (localctx IAfterthoughtContext) {
	localctx = NewAfterthoughtContext(p, p.GetParserRuleContext(), p.GetState())
	p.EnterRule(localctx, 20, PainlessParserRULE_afterthought)
	p.EnterOuterAlt(localctx, 1)
	{
		p.SetState(220)
		p.Expression()
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
	return localctx
	goto errorExit // Trick to prevent compiler error if the label is not used
}

// IDeclarationContext is an interface to support dynamic dispatch.
type IDeclarationContext interface {
	antlr.ParserRuleContext

	// GetParser returns the parser.
	GetParser() antlr.Parser

	// Getter signatures
	Decltype() IDecltypeContext
	AllDeclvar() []IDeclvarContext
	Declvar(i int) IDeclvarContext
	AllCOMMA() []antlr.TerminalNode
	COMMA(i int) antlr.TerminalNode

	// IsDeclarationContext differentiates from other interfaces.
	IsDeclarationContext()
}

type DeclarationContext struct {
	antlr.BaseParserRuleContext
	parser antlr.Parser
}

func NewEmptyDeclarationContext() *DeclarationContext {
	var p = new(DeclarationContext)
	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, nil, -1)
	p.RuleIndex = PainlessParserRULE_declaration
	return p
}

func InitEmptyDeclarationContext(p *DeclarationContext) {
	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, nil, -1)
	p.RuleIndex = PainlessParserRULE_declaration
}

func (*DeclarationContext) IsDeclarationContext() {}

func NewDeclarationContext(parser antlr.Parser, parent antlr.ParserRuleContext, invokingState int) *DeclarationContext {
	var p = new(DeclarationContext)

	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, parent, invokingState)

	p.parser = parser
	p.RuleIndex = PainlessParserRULE_declaration

	return p
}

func (s *DeclarationContext) GetParser() antlr.Parser { return s.parser }

func (s *DeclarationContext) Decltype() IDecltypeContext {
	var t antlr.RuleContext
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IDecltypeContext); ok {
			t = ctx.(antlr.RuleContext)
			break
		}
	}

	if t == nil {
		return nil
	}

	return t.(IDecltypeContext)
}

func (s *DeclarationContext) AllDeclvar() []IDeclvarContext {
	children := s.GetChildren()
	len := 0
	for _, ctx := range children {
		if _, ok := ctx.(IDeclvarContext); ok {
			len++
		}
	}

	tst := make([]IDeclvarContext, len)
	i := 0
	for _, ctx := range children {
		if t, ok := ctx.(IDeclvarContext); ok {
			tst[i] = t.(IDeclvarContext)
			i++
		}
	}

	return tst
}

func (s *DeclarationContext) Declvar(i int) IDeclvarContext {
	var t antlr.RuleContext
	j := 0
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IDeclvarContext); ok {
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

	return t.(IDeclvarContext)
}

func (s *DeclarationContext) AllCOMMA() []antlr.TerminalNode {
	return s.GetTokens(PainlessParserCOMMA)
}

func (s *DeclarationContext) COMMA(i int) antlr.TerminalNode {
	return s.GetToken(PainlessParserCOMMA, i)
}

func (s *DeclarationContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *DeclarationContext) ToStringTree(ruleNames []string, recog antlr.Recognizer) string {
	return antlr.TreesStringTree(s, ruleNames, recog)
}

func (s *DeclarationContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PainlessParserListener); ok {
		listenerT.EnterDeclaration(s)
	}
}

func (s *DeclarationContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PainlessParserListener); ok {
		listenerT.ExitDeclaration(s)
	}
}

func (s *DeclarationContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PainlessParserVisitor:
		return t.VisitDeclaration(s)

	default:
		return t.VisitChildren(s)
	}
}

func (p *PainlessParser) Declaration() (localctx IDeclarationContext) {
	localctx = NewDeclarationContext(p, p.GetParserRuleContext(), p.GetState())
	p.EnterRule(localctx, 22, PainlessParserRULE_declaration)
	var _la int

	p.EnterOuterAlt(localctx, 1)
	{
		p.SetState(222)
		p.Decltype()
	}
	{
		p.SetState(223)
		p.Declvar()
	}
	p.SetState(228)
	p.GetErrorHandler().Sync(p)
	if p.HasError() {
		goto errorExit
	}
	_la = p.GetTokenStream().LA(1)

	for _la == PainlessParserCOMMA {
		{
			p.SetState(224)
			p.Match(PainlessParserCOMMA)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(225)
			p.Declvar()
		}

		p.SetState(230)
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
	return localctx
	goto errorExit // Trick to prevent compiler error if the label is not used
}

// IDecltypeContext is an interface to support dynamic dispatch.
type IDecltypeContext interface {
	antlr.ParserRuleContext

	// GetParser returns the parser.
	GetParser() antlr.Parser

	// Getter signatures
	Type_() ITypeContext
	AllLBRACE() []antlr.TerminalNode
	LBRACE(i int) antlr.TerminalNode
	AllRBRACE() []antlr.TerminalNode
	RBRACE(i int) antlr.TerminalNode

	// IsDecltypeContext differentiates from other interfaces.
	IsDecltypeContext()
}

type DecltypeContext struct {
	antlr.BaseParserRuleContext
	parser antlr.Parser
}

func NewEmptyDecltypeContext() *DecltypeContext {
	var p = new(DecltypeContext)
	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, nil, -1)
	p.RuleIndex = PainlessParserRULE_decltype
	return p
}

func InitEmptyDecltypeContext(p *DecltypeContext) {
	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, nil, -1)
	p.RuleIndex = PainlessParserRULE_decltype
}

func (*DecltypeContext) IsDecltypeContext() {}

func NewDecltypeContext(parser antlr.Parser, parent antlr.ParserRuleContext, invokingState int) *DecltypeContext {
	var p = new(DecltypeContext)

	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, parent, invokingState)

	p.parser = parser
	p.RuleIndex = PainlessParserRULE_decltype

	return p
}

func (s *DecltypeContext) GetParser() antlr.Parser { return s.parser }

func (s *DecltypeContext) Type_() ITypeContext {
	var t antlr.RuleContext
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(ITypeContext); ok {
			t = ctx.(antlr.RuleContext)
			break
		}
	}

	if t == nil {
		return nil
	}

	return t.(ITypeContext)
}

func (s *DecltypeContext) AllLBRACE() []antlr.TerminalNode {
	return s.GetTokens(PainlessParserLBRACE)
}

func (s *DecltypeContext) LBRACE(i int) antlr.TerminalNode {
	return s.GetToken(PainlessParserLBRACE, i)
}

func (s *DecltypeContext) AllRBRACE() []antlr.TerminalNode {
	return s.GetTokens(PainlessParserRBRACE)
}

func (s *DecltypeContext) RBRACE(i int) antlr.TerminalNode {
	return s.GetToken(PainlessParserRBRACE, i)
}

func (s *DecltypeContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *DecltypeContext) ToStringTree(ruleNames []string, recog antlr.Recognizer) string {
	return antlr.TreesStringTree(s, ruleNames, recog)
}

func (s *DecltypeContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PainlessParserListener); ok {
		listenerT.EnterDecltype(s)
	}
}

func (s *DecltypeContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PainlessParserListener); ok {
		listenerT.ExitDecltype(s)
	}
}

func (s *DecltypeContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PainlessParserVisitor:
		return t.VisitDecltype(s)

	default:
		return t.VisitChildren(s)
	}
}

func (p *PainlessParser) Decltype() (localctx IDecltypeContext) {
	localctx = NewDecltypeContext(p, p.GetParserRuleContext(), p.GetState())
	p.EnterRule(localctx, 24, PainlessParserRULE_decltype)
	var _alt int

	p.EnterOuterAlt(localctx, 1)
	{
		p.SetState(231)
		p.Type_()
	}
	p.SetState(236)
	p.GetErrorHandler().Sync(p)
	if p.HasError() {
		goto errorExit
	}
	_alt = p.GetInterpreter().AdaptivePredict(p.BaseParser, p.GetTokenStream(), 19, p.GetParserRuleContext())
	if p.HasError() {
		goto errorExit
	}
	for _alt != 2 && _alt != antlr.ATNInvalidAltNumber {
		if _alt == 1 {
			{
				p.SetState(232)
				p.Match(PainlessParserLBRACE)
				if p.HasError() {
					// Recognition error - abort rule
					goto errorExit
				}
			}
			{
				p.SetState(233)
				p.Match(PainlessParserRBRACE)
				if p.HasError() {
					// Recognition error - abort rule
					goto errorExit
				}
			}

		}
		p.SetState(238)
		p.GetErrorHandler().Sync(p)
		if p.HasError() {
			goto errorExit
		}
		_alt = p.GetInterpreter().AdaptivePredict(p.BaseParser, p.GetTokenStream(), 19, p.GetParserRuleContext())
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
	p.ExitRule()
	return localctx
	goto errorExit // Trick to prevent compiler error if the label is not used
}

// ITypeContext is an interface to support dynamic dispatch.
type ITypeContext interface {
	antlr.ParserRuleContext

	// GetParser returns the parser.
	GetParser() antlr.Parser

	// Getter signatures
	DEF() antlr.TerminalNode
	PRIMITIVE() antlr.TerminalNode
	ID() antlr.TerminalNode
	AllDOT() []antlr.TerminalNode
	DOT(i int) antlr.TerminalNode
	AllDOTID() []antlr.TerminalNode
	DOTID(i int) antlr.TerminalNode

	// IsTypeContext differentiates from other interfaces.
	IsTypeContext()
}

type TypeContext struct {
	antlr.BaseParserRuleContext
	parser antlr.Parser
}

func NewEmptyTypeContext() *TypeContext {
	var p = new(TypeContext)
	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, nil, -1)
	p.RuleIndex = PainlessParserRULE_type
	return p
}

func InitEmptyTypeContext(p *TypeContext) {
	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, nil, -1)
	p.RuleIndex = PainlessParserRULE_type
}

func (*TypeContext) IsTypeContext() {}

func NewTypeContext(parser antlr.Parser, parent antlr.ParserRuleContext, invokingState int) *TypeContext {
	var p = new(TypeContext)

	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, parent, invokingState)

	p.parser = parser
	p.RuleIndex = PainlessParserRULE_type

	return p
}

func (s *TypeContext) GetParser() antlr.Parser { return s.parser }

func (s *TypeContext) DEF() antlr.TerminalNode {
	return s.GetToken(PainlessParserDEF, 0)
}

func (s *TypeContext) PRIMITIVE() antlr.TerminalNode {
	return s.GetToken(PainlessParserPRIMITIVE, 0)
}

func (s *TypeContext) ID() antlr.TerminalNode {
	return s.GetToken(PainlessParserID, 0)
}

func (s *TypeContext) AllDOT() []antlr.TerminalNode {
	return s.GetTokens(PainlessParserDOT)
}

func (s *TypeContext) DOT(i int) antlr.TerminalNode {
	return s.GetToken(PainlessParserDOT, i)
}

func (s *TypeContext) AllDOTID() []antlr.TerminalNode {
	return s.GetTokens(PainlessParserDOTID)
}

func (s *TypeContext) DOTID(i int) antlr.TerminalNode {
	return s.GetToken(PainlessParserDOTID, i)
}

func (s *TypeContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *TypeContext) ToStringTree(ruleNames []string, recog antlr.Recognizer) string {
	return antlr.TreesStringTree(s, ruleNames, recog)
}

func (s *TypeContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PainlessParserListener); ok {
		listenerT.EnterType(s)
	}
}

func (s *TypeContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PainlessParserListener); ok {
		listenerT.ExitType(s)
	}
}

func (s *TypeContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PainlessParserVisitor:
		return t.VisitType(s)

	default:
		return t.VisitChildren(s)
	}
}

func (p *PainlessParser) Type_() (localctx ITypeContext) {
	localctx = NewTypeContext(p, p.GetParserRuleContext(), p.GetState())
	p.EnterRule(localctx, 26, PainlessParserRULE_type)
	var _alt int

	p.SetState(249)
	p.GetErrorHandler().Sync(p)
	if p.HasError() {
		goto errorExit
	}

	switch p.GetTokenStream().LA(1) {
	case PainlessParserDEF:
		p.EnterOuterAlt(localctx, 1)
		{
			p.SetState(239)
			p.Match(PainlessParserDEF)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}

	case PainlessParserPRIMITIVE:
		p.EnterOuterAlt(localctx, 2)
		{
			p.SetState(240)
			p.Match(PainlessParserPRIMITIVE)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}

	case PainlessParserID:
		p.EnterOuterAlt(localctx, 3)
		{
			p.SetState(241)
			p.Match(PainlessParserID)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		p.SetState(246)
		p.GetErrorHandler().Sync(p)
		if p.HasError() {
			goto errorExit
		}
		_alt = p.GetInterpreter().AdaptivePredict(p.BaseParser, p.GetTokenStream(), 20, p.GetParserRuleContext())
		if p.HasError() {
			goto errorExit
		}
		for _alt != 2 && _alt != antlr.ATNInvalidAltNumber {
			if _alt == 1 {
				{
					p.SetState(242)
					p.Match(PainlessParserDOT)
					if p.HasError() {
						// Recognition error - abort rule
						goto errorExit
					}
				}
				{
					p.SetState(243)
					p.Match(PainlessParserDOTID)
					if p.HasError() {
						// Recognition error - abort rule
						goto errorExit
					}
				}

			}
			p.SetState(248)
			p.GetErrorHandler().Sync(p)
			if p.HasError() {
				goto errorExit
			}
			_alt = p.GetInterpreter().AdaptivePredict(p.BaseParser, p.GetTokenStream(), 20, p.GetParserRuleContext())
			if p.HasError() {
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
	return localctx
	goto errorExit // Trick to prevent compiler error if the label is not used
}

// IDeclvarContext is an interface to support dynamic dispatch.
type IDeclvarContext interface {
	antlr.ParserRuleContext

	// GetParser returns the parser.
	GetParser() antlr.Parser

	// Getter signatures
	ID() antlr.TerminalNode
	ASSIGN() antlr.TerminalNode
	Expression() IExpressionContext

	// IsDeclvarContext differentiates from other interfaces.
	IsDeclvarContext()
}

type DeclvarContext struct {
	antlr.BaseParserRuleContext
	parser antlr.Parser
}

func NewEmptyDeclvarContext() *DeclvarContext {
	var p = new(DeclvarContext)
	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, nil, -1)
	p.RuleIndex = PainlessParserRULE_declvar
	return p
}

func InitEmptyDeclvarContext(p *DeclvarContext) {
	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, nil, -1)
	p.RuleIndex = PainlessParserRULE_declvar
}

func (*DeclvarContext) IsDeclvarContext() {}

func NewDeclvarContext(parser antlr.Parser, parent antlr.ParserRuleContext, invokingState int) *DeclvarContext {
	var p = new(DeclvarContext)

	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, parent, invokingState)

	p.parser = parser
	p.RuleIndex = PainlessParserRULE_declvar

	return p
}

func (s *DeclvarContext) GetParser() antlr.Parser { return s.parser }

func (s *DeclvarContext) ID() antlr.TerminalNode {
	return s.GetToken(PainlessParserID, 0)
}

func (s *DeclvarContext) ASSIGN() antlr.TerminalNode {
	return s.GetToken(PainlessParserASSIGN, 0)
}

func (s *DeclvarContext) Expression() IExpressionContext {
	var t antlr.RuleContext
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IExpressionContext); ok {
			t = ctx.(antlr.RuleContext)
			break
		}
	}

	if t == nil {
		return nil
	}

	return t.(IExpressionContext)
}

func (s *DeclvarContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *DeclvarContext) ToStringTree(ruleNames []string, recog antlr.Recognizer) string {
	return antlr.TreesStringTree(s, ruleNames, recog)
}

func (s *DeclvarContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PainlessParserListener); ok {
		listenerT.EnterDeclvar(s)
	}
}

func (s *DeclvarContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PainlessParserListener); ok {
		listenerT.ExitDeclvar(s)
	}
}

func (s *DeclvarContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PainlessParserVisitor:
		return t.VisitDeclvar(s)

	default:
		return t.VisitChildren(s)
	}
}

func (p *PainlessParser) Declvar() (localctx IDeclvarContext) {
	localctx = NewDeclvarContext(p, p.GetParserRuleContext(), p.GetState())
	p.EnterRule(localctx, 28, PainlessParserRULE_declvar)
	var _la int

	p.EnterOuterAlt(localctx, 1)
	{
		p.SetState(251)
		p.Match(PainlessParserID)
		if p.HasError() {
			// Recognition error - abort rule
			goto errorExit
		}
	}
	p.SetState(254)
	p.GetErrorHandler().Sync(p)
	if p.HasError() {
		goto errorExit
	}
	_la = p.GetTokenStream().LA(1)

	if _la == PainlessParserASSIGN {
		{
			p.SetState(252)
			p.Match(PainlessParserASSIGN)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(253)
			p.Expression()
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
	return localctx
	goto errorExit // Trick to prevent compiler error if the label is not used
}

// ITrapContext is an interface to support dynamic dispatch.
type ITrapContext interface {
	antlr.ParserRuleContext

	// GetParser returns the parser.
	GetParser() antlr.Parser

	// Getter signatures
	CATCH() antlr.TerminalNode
	LP() antlr.TerminalNode
	Type_() ITypeContext
	ID() antlr.TerminalNode
	RP() antlr.TerminalNode
	Block() IBlockContext

	// IsTrapContext differentiates from other interfaces.
	IsTrapContext()
}

type TrapContext struct {
	antlr.BaseParserRuleContext
	parser antlr.Parser
}

func NewEmptyTrapContext() *TrapContext {
	var p = new(TrapContext)
	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, nil, -1)
	p.RuleIndex = PainlessParserRULE_trap
	return p
}

func InitEmptyTrapContext(p *TrapContext) {
	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, nil, -1)
	p.RuleIndex = PainlessParserRULE_trap
}

func (*TrapContext) IsTrapContext() {}

func NewTrapContext(parser antlr.Parser, parent antlr.ParserRuleContext, invokingState int) *TrapContext {
	var p = new(TrapContext)

	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, parent, invokingState)

	p.parser = parser
	p.RuleIndex = PainlessParserRULE_trap

	return p
}

func (s *TrapContext) GetParser() antlr.Parser { return s.parser }

func (s *TrapContext) CATCH() antlr.TerminalNode {
	return s.GetToken(PainlessParserCATCH, 0)
}

func (s *TrapContext) LP() antlr.TerminalNode {
	return s.GetToken(PainlessParserLP, 0)
}

func (s *TrapContext) Type_() ITypeContext {
	var t antlr.RuleContext
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(ITypeContext); ok {
			t = ctx.(antlr.RuleContext)
			break
		}
	}

	if t == nil {
		return nil
	}

	return t.(ITypeContext)
}

func (s *TrapContext) ID() antlr.TerminalNode {
	return s.GetToken(PainlessParserID, 0)
}

func (s *TrapContext) RP() antlr.TerminalNode {
	return s.GetToken(PainlessParserRP, 0)
}

func (s *TrapContext) Block() IBlockContext {
	var t antlr.RuleContext
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IBlockContext); ok {
			t = ctx.(antlr.RuleContext)
			break
		}
	}

	if t == nil {
		return nil
	}

	return t.(IBlockContext)
}

func (s *TrapContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *TrapContext) ToStringTree(ruleNames []string, recog antlr.Recognizer) string {
	return antlr.TreesStringTree(s, ruleNames, recog)
}

func (s *TrapContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PainlessParserListener); ok {
		listenerT.EnterTrap(s)
	}
}

func (s *TrapContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PainlessParserListener); ok {
		listenerT.ExitTrap(s)
	}
}

func (s *TrapContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PainlessParserVisitor:
		return t.VisitTrap(s)

	default:
		return t.VisitChildren(s)
	}
}

func (p *PainlessParser) Trap() (localctx ITrapContext) {
	localctx = NewTrapContext(p, p.GetParserRuleContext(), p.GetState())
	p.EnterRule(localctx, 30, PainlessParserRULE_trap)
	p.EnterOuterAlt(localctx, 1)
	{
		p.SetState(256)
		p.Match(PainlessParserCATCH)
		if p.HasError() {
			// Recognition error - abort rule
			goto errorExit
		}
	}
	{
		p.SetState(257)
		p.Match(PainlessParserLP)
		if p.HasError() {
			// Recognition error - abort rule
			goto errorExit
		}
	}
	{
		p.SetState(258)
		p.Type_()
	}
	{
		p.SetState(259)
		p.Match(PainlessParserID)
		if p.HasError() {
			// Recognition error - abort rule
			goto errorExit
		}
	}
	{
		p.SetState(260)
		p.Match(PainlessParserRP)
		if p.HasError() {
			// Recognition error - abort rule
			goto errorExit
		}
	}
	{
		p.SetState(261)
		p.Block()
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
	return localctx
	goto errorExit // Trick to prevent compiler error if the label is not used
}

// INoncondexpressionContext is an interface to support dynamic dispatch.
type INoncondexpressionContext interface {
	antlr.ParserRuleContext

	// GetParser returns the parser.
	GetParser() antlr.Parser
	// IsNoncondexpressionContext differentiates from other interfaces.
	IsNoncondexpressionContext()
}

type NoncondexpressionContext struct {
	antlr.BaseParserRuleContext
	parser antlr.Parser
}

func NewEmptyNoncondexpressionContext() *NoncondexpressionContext {
	var p = new(NoncondexpressionContext)
	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, nil, -1)
	p.RuleIndex = PainlessParserRULE_noncondexpression
	return p
}

func InitEmptyNoncondexpressionContext(p *NoncondexpressionContext) {
	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, nil, -1)
	p.RuleIndex = PainlessParserRULE_noncondexpression
}

func (*NoncondexpressionContext) IsNoncondexpressionContext() {}

func NewNoncondexpressionContext(parser antlr.Parser, parent antlr.ParserRuleContext, invokingState int) *NoncondexpressionContext {
	var p = new(NoncondexpressionContext)

	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, parent, invokingState)

	p.parser = parser
	p.RuleIndex = PainlessParserRULE_noncondexpression

	return p
}

func (s *NoncondexpressionContext) GetParser() antlr.Parser { return s.parser }

func (s *NoncondexpressionContext) CopyAll(ctx *NoncondexpressionContext) {
	s.CopyFrom(&ctx.BaseParserRuleContext)
}

func (s *NoncondexpressionContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *NoncondexpressionContext) ToStringTree(ruleNames []string, recog antlr.Recognizer) string {
	return antlr.TreesStringTree(s, ruleNames, recog)
}

type SingleContext struct {
	NoncondexpressionContext
}

func NewSingleContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *SingleContext {
	var p = new(SingleContext)

	InitEmptyNoncondexpressionContext(&p.NoncondexpressionContext)
	p.parser = parser
	p.CopyAll(ctx.(*NoncondexpressionContext))

	return p
}

func (s *SingleContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *SingleContext) Unary() IUnaryContext {
	var t antlr.RuleContext
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IUnaryContext); ok {
			t = ctx.(antlr.RuleContext)
			break
		}
	}

	if t == nil {
		return nil
	}

	return t.(IUnaryContext)
}

func (s *SingleContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PainlessParserListener); ok {
		listenerT.EnterSingle(s)
	}
}

func (s *SingleContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PainlessParserListener); ok {
		listenerT.ExitSingle(s)
	}
}

func (s *SingleContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PainlessParserVisitor:
		return t.VisitSingle(s)

	default:
		return t.VisitChildren(s)
	}
}

type CompContext struct {
	NoncondexpressionContext
}

func NewCompContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *CompContext {
	var p = new(CompContext)

	InitEmptyNoncondexpressionContext(&p.NoncondexpressionContext)
	p.parser = parser
	p.CopyAll(ctx.(*NoncondexpressionContext))

	return p
}

func (s *CompContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *CompContext) AllNoncondexpression() []INoncondexpressionContext {
	children := s.GetChildren()
	len := 0
	for _, ctx := range children {
		if _, ok := ctx.(INoncondexpressionContext); ok {
			len++
		}
	}

	tst := make([]INoncondexpressionContext, len)
	i := 0
	for _, ctx := range children {
		if t, ok := ctx.(INoncondexpressionContext); ok {
			tst[i] = t.(INoncondexpressionContext)
			i++
		}
	}

	return tst
}

func (s *CompContext) Noncondexpression(i int) INoncondexpressionContext {
	var t antlr.RuleContext
	j := 0
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(INoncondexpressionContext); ok {
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

	return t.(INoncondexpressionContext)
}

func (s *CompContext) LT() antlr.TerminalNode {
	return s.GetToken(PainlessParserLT, 0)
}

func (s *CompContext) LTE() antlr.TerminalNode {
	return s.GetToken(PainlessParserLTE, 0)
}

func (s *CompContext) GT() antlr.TerminalNode {
	return s.GetToken(PainlessParserGT, 0)
}

func (s *CompContext) GTE() antlr.TerminalNode {
	return s.GetToken(PainlessParserGTE, 0)
}

func (s *CompContext) EQ() antlr.TerminalNode {
	return s.GetToken(PainlessParserEQ, 0)
}

func (s *CompContext) EQR() antlr.TerminalNode {
	return s.GetToken(PainlessParserEQR, 0)
}

func (s *CompContext) NE() antlr.TerminalNode {
	return s.GetToken(PainlessParserNE, 0)
}

func (s *CompContext) NER() antlr.TerminalNode {
	return s.GetToken(PainlessParserNER, 0)
}

func (s *CompContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PainlessParserListener); ok {
		listenerT.EnterComp(s)
	}
}

func (s *CompContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PainlessParserListener); ok {
		listenerT.ExitComp(s)
	}
}

func (s *CompContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PainlessParserVisitor:
		return t.VisitComp(s)

	default:
		return t.VisitChildren(s)
	}
}

type BoolContext struct {
	NoncondexpressionContext
}

func NewBoolContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *BoolContext {
	var p = new(BoolContext)

	InitEmptyNoncondexpressionContext(&p.NoncondexpressionContext)
	p.parser = parser
	p.CopyAll(ctx.(*NoncondexpressionContext))

	return p
}

func (s *BoolContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *BoolContext) AllNoncondexpression() []INoncondexpressionContext {
	children := s.GetChildren()
	len := 0
	for _, ctx := range children {
		if _, ok := ctx.(INoncondexpressionContext); ok {
			len++
		}
	}

	tst := make([]INoncondexpressionContext, len)
	i := 0
	for _, ctx := range children {
		if t, ok := ctx.(INoncondexpressionContext); ok {
			tst[i] = t.(INoncondexpressionContext)
			i++
		}
	}

	return tst
}

func (s *BoolContext) Noncondexpression(i int) INoncondexpressionContext {
	var t antlr.RuleContext
	j := 0
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(INoncondexpressionContext); ok {
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

	return t.(INoncondexpressionContext)
}

func (s *BoolContext) BOOLAND() antlr.TerminalNode {
	return s.GetToken(PainlessParserBOOLAND, 0)
}

func (s *BoolContext) BOOLOR() antlr.TerminalNode {
	return s.GetToken(PainlessParserBOOLOR, 0)
}

func (s *BoolContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PainlessParserListener); ok {
		listenerT.EnterBool(s)
	}
}

func (s *BoolContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PainlessParserListener); ok {
		listenerT.ExitBool(s)
	}
}

func (s *BoolContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PainlessParserVisitor:
		return t.VisitBool(s)

	default:
		return t.VisitChildren(s)
	}
}

type BinaryContext struct {
	NoncondexpressionContext
}

func NewBinaryContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *BinaryContext {
	var p = new(BinaryContext)

	InitEmptyNoncondexpressionContext(&p.NoncondexpressionContext)
	p.parser = parser
	p.CopyAll(ctx.(*NoncondexpressionContext))

	return p
}

func (s *BinaryContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *BinaryContext) AllNoncondexpression() []INoncondexpressionContext {
	children := s.GetChildren()
	len := 0
	for _, ctx := range children {
		if _, ok := ctx.(INoncondexpressionContext); ok {
			len++
		}
	}

	tst := make([]INoncondexpressionContext, len)
	i := 0
	for _, ctx := range children {
		if t, ok := ctx.(INoncondexpressionContext); ok {
			tst[i] = t.(INoncondexpressionContext)
			i++
		}
	}

	return tst
}

func (s *BinaryContext) Noncondexpression(i int) INoncondexpressionContext {
	var t antlr.RuleContext
	j := 0
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(INoncondexpressionContext); ok {
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

	return t.(INoncondexpressionContext)
}

func (s *BinaryContext) MUL() antlr.TerminalNode {
	return s.GetToken(PainlessParserMUL, 0)
}

func (s *BinaryContext) DIV() antlr.TerminalNode {
	return s.GetToken(PainlessParserDIV, 0)
}

func (s *BinaryContext) REM() antlr.TerminalNode {
	return s.GetToken(PainlessParserREM, 0)
}

func (s *BinaryContext) ADD() antlr.TerminalNode {
	return s.GetToken(PainlessParserADD, 0)
}

func (s *BinaryContext) SUB() antlr.TerminalNode {
	return s.GetToken(PainlessParserSUB, 0)
}

func (s *BinaryContext) FIND() antlr.TerminalNode {
	return s.GetToken(PainlessParserFIND, 0)
}

func (s *BinaryContext) MATCH() antlr.TerminalNode {
	return s.GetToken(PainlessParserMATCH, 0)
}

func (s *BinaryContext) LSH() antlr.TerminalNode {
	return s.GetToken(PainlessParserLSH, 0)
}

func (s *BinaryContext) RSH() antlr.TerminalNode {
	return s.GetToken(PainlessParserRSH, 0)
}

func (s *BinaryContext) USH() antlr.TerminalNode {
	return s.GetToken(PainlessParserUSH, 0)
}

func (s *BinaryContext) BWAND() antlr.TerminalNode {
	return s.GetToken(PainlessParserBWAND, 0)
}

func (s *BinaryContext) XOR() antlr.TerminalNode {
	return s.GetToken(PainlessParserXOR, 0)
}

func (s *BinaryContext) BWOR() antlr.TerminalNode {
	return s.GetToken(PainlessParserBWOR, 0)
}

func (s *BinaryContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PainlessParserListener); ok {
		listenerT.EnterBinary(s)
	}
}

func (s *BinaryContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PainlessParserListener); ok {
		listenerT.ExitBinary(s)
	}
}

func (s *BinaryContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PainlessParserVisitor:
		return t.VisitBinary(s)

	default:
		return t.VisitChildren(s)
	}
}

type ElvisContext struct {
	NoncondexpressionContext
}

func NewElvisContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *ElvisContext {
	var p = new(ElvisContext)

	InitEmptyNoncondexpressionContext(&p.NoncondexpressionContext)
	p.parser = parser
	p.CopyAll(ctx.(*NoncondexpressionContext))

	return p
}

func (s *ElvisContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *ElvisContext) AllNoncondexpression() []INoncondexpressionContext {
	children := s.GetChildren()
	len := 0
	for _, ctx := range children {
		if _, ok := ctx.(INoncondexpressionContext); ok {
			len++
		}
	}

	tst := make([]INoncondexpressionContext, len)
	i := 0
	for _, ctx := range children {
		if t, ok := ctx.(INoncondexpressionContext); ok {
			tst[i] = t.(INoncondexpressionContext)
			i++
		}
	}

	return tst
}

func (s *ElvisContext) Noncondexpression(i int) INoncondexpressionContext {
	var t antlr.RuleContext
	j := 0
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(INoncondexpressionContext); ok {
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

	return t.(INoncondexpressionContext)
}

func (s *ElvisContext) ELVIS() antlr.TerminalNode {
	return s.GetToken(PainlessParserELVIS, 0)
}

func (s *ElvisContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PainlessParserListener); ok {
		listenerT.EnterElvis(s)
	}
}

func (s *ElvisContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PainlessParserListener); ok {
		listenerT.ExitElvis(s)
	}
}

func (s *ElvisContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PainlessParserVisitor:
		return t.VisitElvis(s)

	default:
		return t.VisitChildren(s)
	}
}

type InstanceofContext struct {
	NoncondexpressionContext
}

func NewInstanceofContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *InstanceofContext {
	var p = new(InstanceofContext)

	InitEmptyNoncondexpressionContext(&p.NoncondexpressionContext)
	p.parser = parser
	p.CopyAll(ctx.(*NoncondexpressionContext))

	return p
}

func (s *InstanceofContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *InstanceofContext) Noncondexpression() INoncondexpressionContext {
	var t antlr.RuleContext
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(INoncondexpressionContext); ok {
			t = ctx.(antlr.RuleContext)
			break
		}
	}

	if t == nil {
		return nil
	}

	return t.(INoncondexpressionContext)
}

func (s *InstanceofContext) INSTANCEOF() antlr.TerminalNode {
	return s.GetToken(PainlessParserINSTANCEOF, 0)
}

func (s *InstanceofContext) Decltype() IDecltypeContext {
	var t antlr.RuleContext
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IDecltypeContext); ok {
			t = ctx.(antlr.RuleContext)
			break
		}
	}

	if t == nil {
		return nil
	}

	return t.(IDecltypeContext)
}

func (s *InstanceofContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PainlessParserListener); ok {
		listenerT.EnterInstanceof(s)
	}
}

func (s *InstanceofContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PainlessParserListener); ok {
		listenerT.ExitInstanceof(s)
	}
}

func (s *InstanceofContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PainlessParserVisitor:
		return t.VisitInstanceof(s)

	default:
		return t.VisitChildren(s)
	}
}

func (p *PainlessParser) Noncondexpression() (localctx INoncondexpressionContext) {
	return p.noncondexpression(0)
}

func (p *PainlessParser) noncondexpression(_p int) (localctx INoncondexpressionContext) {
	var _parentctx antlr.ParserRuleContext = p.GetParserRuleContext()

	_parentState := p.GetState()
	localctx = NewNoncondexpressionContext(p, p.GetParserRuleContext(), _parentState)
	var _prevctx INoncondexpressionContext = localctx
	var _ antlr.ParserRuleContext = _prevctx // TODO: To prevent unused variable warning.
	_startState := 32
	p.EnterRecursionRule(localctx, 32, PainlessParserRULE_noncondexpression, _p)
	var _la int

	var _alt int

	p.EnterOuterAlt(localctx, 1)
	localctx = NewSingleContext(p, localctx)
	p.SetParserRuleContext(localctx)
	_prevctx = localctx

	{
		p.SetState(264)
		p.Unary()
	}

	p.GetParserRuleContext().SetStop(p.GetTokenStream().LT(-1))
	p.SetState(307)
	p.GetErrorHandler().Sync(p)
	if p.HasError() {
		goto errorExit
	}
	_alt = p.GetInterpreter().AdaptivePredict(p.BaseParser, p.GetTokenStream(), 24, p.GetParserRuleContext())
	if p.HasError() {
		goto errorExit
	}
	for _alt != 2 && _alt != antlr.ATNInvalidAltNumber {
		if _alt == 1 {
			if p.GetParseListeners() != nil {
				p.TriggerExitRuleEvent()
			}
			_prevctx = localctx
			p.SetState(305)
			p.GetErrorHandler().Sync(p)
			if p.HasError() {
				goto errorExit
			}

			switch p.GetInterpreter().AdaptivePredict(p.BaseParser, p.GetTokenStream(), 23, p.GetParserRuleContext()) {
			case 1:
				localctx = NewBinaryContext(p, NewNoncondexpressionContext(p, _parentctx, _parentState))
				p.PushNewRecursionContext(localctx, _startState, PainlessParserRULE_noncondexpression)
				p.SetState(266)

				if !(p.Precpred(p.GetParserRuleContext(), 13)) {
					p.SetError(antlr.NewFailedPredicateException(p, "p.Precpred(p.GetParserRuleContext(), 13)", ""))
					goto errorExit
				}
				{
					p.SetState(267)
					_la = p.GetTokenStream().LA(1)

					if !((int64(_la) & ^0x3f) == 0 && ((int64(1)<<_la)&7516192768) != 0) {
						p.GetErrorHandler().RecoverInline(p)
					} else {
						p.GetErrorHandler().ReportMatch(p)
						p.Consume()
					}
				}
				{
					p.SetState(268)
					p.noncondexpression(14)
				}

			case 2:
				localctx = NewBinaryContext(p, NewNoncondexpressionContext(p, _parentctx, _parentState))
				p.PushNewRecursionContext(localctx, _startState, PainlessParserRULE_noncondexpression)
				p.SetState(269)

				if !(p.Precpred(p.GetParserRuleContext(), 12)) {
					p.SetError(antlr.NewFailedPredicateException(p, "p.Precpred(p.GetParserRuleContext(), 12)", ""))
					goto errorExit
				}
				{
					p.SetState(270)
					_la = p.GetTokenStream().LA(1)

					if !(_la == PainlessParserADD || _la == PainlessParserSUB) {
						p.GetErrorHandler().RecoverInline(p)
					} else {
						p.GetErrorHandler().ReportMatch(p)
						p.Consume()
					}
				}
				{
					p.SetState(271)
					p.noncondexpression(13)
				}

			case 3:
				localctx = NewBinaryContext(p, NewNoncondexpressionContext(p, _parentctx, _parentState))
				p.PushNewRecursionContext(localctx, _startState, PainlessParserRULE_noncondexpression)
				p.SetState(272)

				if !(p.Precpred(p.GetParserRuleContext(), 11)) {
					p.SetError(antlr.NewFailedPredicateException(p, "p.Precpred(p.GetParserRuleContext(), 11)", ""))
					goto errorExit
				}
				{
					p.SetState(273)
					_la = p.GetTokenStream().LA(1)

					if !(_la == PainlessParserFIND || _la == PainlessParserMATCH) {
						p.GetErrorHandler().RecoverInline(p)
					} else {
						p.GetErrorHandler().ReportMatch(p)
						p.Consume()
					}
				}
				{
					p.SetState(274)
					p.noncondexpression(12)
				}

			case 4:
				localctx = NewBinaryContext(p, NewNoncondexpressionContext(p, _parentctx, _parentState))
				p.PushNewRecursionContext(localctx, _startState, PainlessParserRULE_noncondexpression)
				p.SetState(275)

				if !(p.Precpred(p.GetParserRuleContext(), 10)) {
					p.SetError(antlr.NewFailedPredicateException(p, "p.Precpred(p.GetParserRuleContext(), 10)", ""))
					goto errorExit
				}
				{
					p.SetState(276)
					_la = p.GetTokenStream().LA(1)

					if !((int64(_la) & ^0x3f) == 0 && ((int64(1)<<_la)&240518168576) != 0) {
						p.GetErrorHandler().RecoverInline(p)
					} else {
						p.GetErrorHandler().ReportMatch(p)
						p.Consume()
					}
				}
				{
					p.SetState(277)
					p.noncondexpression(11)
				}

			case 5:
				localctx = NewCompContext(p, NewNoncondexpressionContext(p, _parentctx, _parentState))
				p.PushNewRecursionContext(localctx, _startState, PainlessParserRULE_noncondexpression)
				p.SetState(278)

				if !(p.Precpred(p.GetParserRuleContext(), 9)) {
					p.SetError(antlr.NewFailedPredicateException(p, "p.Precpred(p.GetParserRuleContext(), 9)", ""))
					goto errorExit
				}
				{
					p.SetState(279)
					_la = p.GetTokenStream().LA(1)

					if !((int64(_la) & ^0x3f) == 0 && ((int64(1)<<_la)&4123168604160) != 0) {
						p.GetErrorHandler().RecoverInline(p)
					} else {
						p.GetErrorHandler().ReportMatch(p)
						p.Consume()
					}
				}
				{
					p.SetState(280)
					p.noncondexpression(10)
				}

			case 6:
				localctx = NewCompContext(p, NewNoncondexpressionContext(p, _parentctx, _parentState))
				p.PushNewRecursionContext(localctx, _startState, PainlessParserRULE_noncondexpression)
				p.SetState(281)

				if !(p.Precpred(p.GetParserRuleContext(), 7)) {
					p.SetError(antlr.NewFailedPredicateException(p, "p.Precpred(p.GetParserRuleContext(), 7)", ""))
					goto errorExit
				}
				{
					p.SetState(282)
					_la = p.GetTokenStream().LA(1)

					if !((int64(_la) & ^0x3f) == 0 && ((int64(1)<<_la)&65970697666560) != 0) {
						p.GetErrorHandler().RecoverInline(p)
					} else {
						p.GetErrorHandler().ReportMatch(p)
						p.Consume()
					}
				}
				{
					p.SetState(283)
					p.noncondexpression(8)
				}

			case 7:
				localctx = NewBinaryContext(p, NewNoncondexpressionContext(p, _parentctx, _parentState))
				p.PushNewRecursionContext(localctx, _startState, PainlessParserRULE_noncondexpression)
				p.SetState(284)

				if !(p.Precpred(p.GetParserRuleContext(), 6)) {
					p.SetError(antlr.NewFailedPredicateException(p, "p.Precpred(p.GetParserRuleContext(), 6)", ""))
					goto errorExit
				}
				{
					p.SetState(285)
					p.Match(PainlessParserBWAND)
					if p.HasError() {
						// Recognition error - abort rule
						goto errorExit
					}
				}
				{
					p.SetState(286)
					p.noncondexpression(7)
				}

			case 8:
				localctx = NewBinaryContext(p, NewNoncondexpressionContext(p, _parentctx, _parentState))
				p.PushNewRecursionContext(localctx, _startState, PainlessParserRULE_noncondexpression)
				p.SetState(287)

				if !(p.Precpred(p.GetParserRuleContext(), 5)) {
					p.SetError(antlr.NewFailedPredicateException(p, "p.Precpred(p.GetParserRuleContext(), 5)", ""))
					goto errorExit
				}
				{
					p.SetState(288)
					p.Match(PainlessParserXOR)
					if p.HasError() {
						// Recognition error - abort rule
						goto errorExit
					}
				}
				{
					p.SetState(289)
					p.noncondexpression(6)
				}

			case 9:
				localctx = NewBinaryContext(p, NewNoncondexpressionContext(p, _parentctx, _parentState))
				p.PushNewRecursionContext(localctx, _startState, PainlessParserRULE_noncondexpression)
				p.SetState(290)

				if !(p.Precpred(p.GetParserRuleContext(), 4)) {
					p.SetError(antlr.NewFailedPredicateException(p, "p.Precpred(p.GetParserRuleContext(), 4)", ""))
					goto errorExit
				}
				{
					p.SetState(291)
					p.Match(PainlessParserBWOR)
					if p.HasError() {
						// Recognition error - abort rule
						goto errorExit
					}
				}
				{
					p.SetState(292)
					p.noncondexpression(5)
				}

			case 10:
				localctx = NewBoolContext(p, NewNoncondexpressionContext(p, _parentctx, _parentState))
				p.PushNewRecursionContext(localctx, _startState, PainlessParserRULE_noncondexpression)
				p.SetState(293)

				if !(p.Precpred(p.GetParserRuleContext(), 3)) {
					p.SetError(antlr.NewFailedPredicateException(p, "p.Precpred(p.GetParserRuleContext(), 3)", ""))
					goto errorExit
				}
				{
					p.SetState(294)
					p.Match(PainlessParserBOOLAND)
					if p.HasError() {
						// Recognition error - abort rule
						goto errorExit
					}
				}
				{
					p.SetState(295)
					p.noncondexpression(4)
				}

			case 11:
				localctx = NewBoolContext(p, NewNoncondexpressionContext(p, _parentctx, _parentState))
				p.PushNewRecursionContext(localctx, _startState, PainlessParserRULE_noncondexpression)
				p.SetState(296)

				if !(p.Precpred(p.GetParserRuleContext(), 2)) {
					p.SetError(antlr.NewFailedPredicateException(p, "p.Precpred(p.GetParserRuleContext(), 2)", ""))
					goto errorExit
				}
				{
					p.SetState(297)
					p.Match(PainlessParserBOOLOR)
					if p.HasError() {
						// Recognition error - abort rule
						goto errorExit
					}
				}
				{
					p.SetState(298)
					p.noncondexpression(3)
				}

			case 12:
				localctx = NewElvisContext(p, NewNoncondexpressionContext(p, _parentctx, _parentState))
				p.PushNewRecursionContext(localctx, _startState, PainlessParserRULE_noncondexpression)
				p.SetState(299)

				if !(p.Precpred(p.GetParserRuleContext(), 1)) {
					p.SetError(antlr.NewFailedPredicateException(p, "p.Precpred(p.GetParserRuleContext(), 1)", ""))
					goto errorExit
				}
				{
					p.SetState(300)
					p.Match(PainlessParserELVIS)
					if p.HasError() {
						// Recognition error - abort rule
						goto errorExit
					}
				}
				{
					p.SetState(301)
					p.noncondexpression(1)
				}

			case 13:
				localctx = NewInstanceofContext(p, NewNoncondexpressionContext(p, _parentctx, _parentState))
				p.PushNewRecursionContext(localctx, _startState, PainlessParserRULE_noncondexpression)
				p.SetState(302)

				if !(p.Precpred(p.GetParserRuleContext(), 8)) {
					p.SetError(antlr.NewFailedPredicateException(p, "p.Precpred(p.GetParserRuleContext(), 8)", ""))
					goto errorExit
				}
				{
					p.SetState(303)
					p.Match(PainlessParserINSTANCEOF)
					if p.HasError() {
						// Recognition error - abort rule
						goto errorExit
					}
				}
				{
					p.SetState(304)
					p.Decltype()
				}

			case antlr.ATNInvalidAltNumber:
				goto errorExit
			}

		}
		p.SetState(309)
		p.GetErrorHandler().Sync(p)
		if p.HasError() {
			goto errorExit
		}
		_alt = p.GetInterpreter().AdaptivePredict(p.BaseParser, p.GetTokenStream(), 24, p.GetParserRuleContext())
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
	return localctx
	goto errorExit // Trick to prevent compiler error if the label is not used
}

// IExpressionContext is an interface to support dynamic dispatch.
type IExpressionContext interface {
	antlr.ParserRuleContext

	// GetParser returns the parser.
	GetParser() antlr.Parser
	// IsExpressionContext differentiates from other interfaces.
	IsExpressionContext()
}

type ExpressionContext struct {
	antlr.BaseParserRuleContext
	parser antlr.Parser
}

func NewEmptyExpressionContext() *ExpressionContext {
	var p = new(ExpressionContext)
	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, nil, -1)
	p.RuleIndex = PainlessParserRULE_expression
	return p
}

func InitEmptyExpressionContext(p *ExpressionContext) {
	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, nil, -1)
	p.RuleIndex = PainlessParserRULE_expression
}

func (*ExpressionContext) IsExpressionContext() {}

func NewExpressionContext(parser antlr.Parser, parent antlr.ParserRuleContext, invokingState int) *ExpressionContext {
	var p = new(ExpressionContext)

	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, parent, invokingState)

	p.parser = parser
	p.RuleIndex = PainlessParserRULE_expression

	return p
}

func (s *ExpressionContext) GetParser() antlr.Parser { return s.parser }

func (s *ExpressionContext) CopyAll(ctx *ExpressionContext) {
	s.CopyFrom(&ctx.BaseParserRuleContext)
}

func (s *ExpressionContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *ExpressionContext) ToStringTree(ruleNames []string, recog antlr.Recognizer) string {
	return antlr.TreesStringTree(s, ruleNames, recog)
}

type ConditionalContext struct {
	ExpressionContext
}

func NewConditionalContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *ConditionalContext {
	var p = new(ConditionalContext)

	InitEmptyExpressionContext(&p.ExpressionContext)
	p.parser = parser
	p.CopyAll(ctx.(*ExpressionContext))

	return p
}

func (s *ConditionalContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *ConditionalContext) Noncondexpression() INoncondexpressionContext {
	var t antlr.RuleContext
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(INoncondexpressionContext); ok {
			t = ctx.(antlr.RuleContext)
			break
		}
	}

	if t == nil {
		return nil
	}

	return t.(INoncondexpressionContext)
}

func (s *ConditionalContext) COND() antlr.TerminalNode {
	return s.GetToken(PainlessParserCOND, 0)
}

func (s *ConditionalContext) AllExpression() []IExpressionContext {
	children := s.GetChildren()
	len := 0
	for _, ctx := range children {
		if _, ok := ctx.(IExpressionContext); ok {
			len++
		}
	}

	tst := make([]IExpressionContext, len)
	i := 0
	for _, ctx := range children {
		if t, ok := ctx.(IExpressionContext); ok {
			tst[i] = t.(IExpressionContext)
			i++
		}
	}

	return tst
}

func (s *ConditionalContext) Expression(i int) IExpressionContext {
	var t antlr.RuleContext
	j := 0
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IExpressionContext); ok {
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

	return t.(IExpressionContext)
}

func (s *ConditionalContext) COLON() antlr.TerminalNode {
	return s.GetToken(PainlessParserCOLON, 0)
}

func (s *ConditionalContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PainlessParserListener); ok {
		listenerT.EnterConditional(s)
	}
}

func (s *ConditionalContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PainlessParserListener); ok {
		listenerT.ExitConditional(s)
	}
}

func (s *ConditionalContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PainlessParserVisitor:
		return t.VisitConditional(s)

	default:
		return t.VisitChildren(s)
	}
}

type AssignmentContext struct {
	ExpressionContext
}

func NewAssignmentContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *AssignmentContext {
	var p = new(AssignmentContext)

	InitEmptyExpressionContext(&p.ExpressionContext)
	p.parser = parser
	p.CopyAll(ctx.(*ExpressionContext))

	return p
}

func (s *AssignmentContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *AssignmentContext) Noncondexpression() INoncondexpressionContext {
	var t antlr.RuleContext
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(INoncondexpressionContext); ok {
			t = ctx.(antlr.RuleContext)
			break
		}
	}

	if t == nil {
		return nil
	}

	return t.(INoncondexpressionContext)
}

func (s *AssignmentContext) Expression() IExpressionContext {
	var t antlr.RuleContext
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IExpressionContext); ok {
			t = ctx.(antlr.RuleContext)
			break
		}
	}

	if t == nil {
		return nil
	}

	return t.(IExpressionContext)
}

func (s *AssignmentContext) ASSIGN() antlr.TerminalNode {
	return s.GetToken(PainlessParserASSIGN, 0)
}

func (s *AssignmentContext) AADD() antlr.TerminalNode {
	return s.GetToken(PainlessParserAADD, 0)
}

func (s *AssignmentContext) ASUB() antlr.TerminalNode {
	return s.GetToken(PainlessParserASUB, 0)
}

func (s *AssignmentContext) AMUL() antlr.TerminalNode {
	return s.GetToken(PainlessParserAMUL, 0)
}

func (s *AssignmentContext) ADIV() antlr.TerminalNode {
	return s.GetToken(PainlessParserADIV, 0)
}

func (s *AssignmentContext) AREM() antlr.TerminalNode {
	return s.GetToken(PainlessParserAREM, 0)
}

func (s *AssignmentContext) AAND() antlr.TerminalNode {
	return s.GetToken(PainlessParserAAND, 0)
}

func (s *AssignmentContext) AXOR() antlr.TerminalNode {
	return s.GetToken(PainlessParserAXOR, 0)
}

func (s *AssignmentContext) AOR() antlr.TerminalNode {
	return s.GetToken(PainlessParserAOR, 0)
}

func (s *AssignmentContext) ALSH() antlr.TerminalNode {
	return s.GetToken(PainlessParserALSH, 0)
}

func (s *AssignmentContext) ARSH() antlr.TerminalNode {
	return s.GetToken(PainlessParserARSH, 0)
}

func (s *AssignmentContext) AUSH() antlr.TerminalNode {
	return s.GetToken(PainlessParserAUSH, 0)
}

func (s *AssignmentContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PainlessParserListener); ok {
		listenerT.EnterAssignment(s)
	}
}

func (s *AssignmentContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PainlessParserListener); ok {
		listenerT.ExitAssignment(s)
	}
}

func (s *AssignmentContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PainlessParserVisitor:
		return t.VisitAssignment(s)

	default:
		return t.VisitChildren(s)
	}
}

type NonconditionalContext struct {
	ExpressionContext
}

func NewNonconditionalContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *NonconditionalContext {
	var p = new(NonconditionalContext)

	InitEmptyExpressionContext(&p.ExpressionContext)
	p.parser = parser
	p.CopyAll(ctx.(*ExpressionContext))

	return p
}

func (s *NonconditionalContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *NonconditionalContext) Noncondexpression() INoncondexpressionContext {
	var t antlr.RuleContext
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(INoncondexpressionContext); ok {
			t = ctx.(antlr.RuleContext)
			break
		}
	}

	if t == nil {
		return nil
	}

	return t.(INoncondexpressionContext)
}

func (s *NonconditionalContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PainlessParserListener); ok {
		listenerT.EnterNonconditional(s)
	}
}

func (s *NonconditionalContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PainlessParserListener); ok {
		listenerT.ExitNonconditional(s)
	}
}

func (s *NonconditionalContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PainlessParserVisitor:
		return t.VisitNonconditional(s)

	default:
		return t.VisitChildren(s)
	}
}

func (p *PainlessParser) Expression() (localctx IExpressionContext) {
	localctx = NewExpressionContext(p, p.GetParserRuleContext(), p.GetState())
	p.EnterRule(localctx, 34, PainlessParserRULE_expression)
	var _la int

	p.SetState(321)
	p.GetErrorHandler().Sync(p)
	if p.HasError() {
		goto errorExit
	}

	switch p.GetInterpreter().AdaptivePredict(p.BaseParser, p.GetTokenStream(), 25, p.GetParserRuleContext()) {
	case 1:
		localctx = NewNonconditionalContext(p, localctx)
		p.EnterOuterAlt(localctx, 1)
		{
			p.SetState(310)
			p.noncondexpression(0)
		}

	case 2:
		localctx = NewConditionalContext(p, localctx)
		p.EnterOuterAlt(localctx, 2)
		{
			p.SetState(311)
			p.noncondexpression(0)
		}
		{
			p.SetState(312)
			p.Match(PainlessParserCOND)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(313)
			p.Expression()
		}
		{
			p.SetState(314)
			p.Match(PainlessParserCOLON)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(315)
			p.Expression()
		}

	case 3:
		localctx = NewAssignmentContext(p, localctx)
		p.EnterOuterAlt(localctx, 3)
		{
			p.SetState(317)
			p.noncondexpression(0)
		}
		{
			p.SetState(318)
			_la = p.GetTokenStream().LA(1)

			if !((int64((_la-60)) & ^0x3f) == 0 && ((int64(1)<<(_la-60))&4095) != 0) {
				p.GetErrorHandler().RecoverInline(p)
			} else {
				p.GetErrorHandler().ReportMatch(p)
				p.Consume()
			}
		}
		{
			p.SetState(319)
			p.Expression()
		}

	case antlr.ATNInvalidAltNumber:
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
	return localctx
	goto errorExit // Trick to prevent compiler error if the label is not used
}

// IUnaryContext is an interface to support dynamic dispatch.
type IUnaryContext interface {
	antlr.ParserRuleContext

	// GetParser returns the parser.
	GetParser() antlr.Parser
	// IsUnaryContext differentiates from other interfaces.
	IsUnaryContext()
}

type UnaryContext struct {
	antlr.BaseParserRuleContext
	parser antlr.Parser
}

func NewEmptyUnaryContext() *UnaryContext {
	var p = new(UnaryContext)
	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, nil, -1)
	p.RuleIndex = PainlessParserRULE_unary
	return p
}

func InitEmptyUnaryContext(p *UnaryContext) {
	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, nil, -1)
	p.RuleIndex = PainlessParserRULE_unary
}

func (*UnaryContext) IsUnaryContext() {}

func NewUnaryContext(parser antlr.Parser, parent antlr.ParserRuleContext, invokingState int) *UnaryContext {
	var p = new(UnaryContext)

	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, parent, invokingState)

	p.parser = parser
	p.RuleIndex = PainlessParserRULE_unary

	return p
}

func (s *UnaryContext) GetParser() antlr.Parser { return s.parser }

func (s *UnaryContext) CopyAll(ctx *UnaryContext) {
	s.CopyFrom(&ctx.BaseParserRuleContext)
}

func (s *UnaryContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *UnaryContext) ToStringTree(ruleNames []string, recog antlr.Recognizer) string {
	return antlr.TreesStringTree(s, ruleNames, recog)
}

type NotaddsubContext struct {
	UnaryContext
}

func NewNotaddsubContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *NotaddsubContext {
	var p = new(NotaddsubContext)

	InitEmptyUnaryContext(&p.UnaryContext)
	p.parser = parser
	p.CopyAll(ctx.(*UnaryContext))

	return p
}

func (s *NotaddsubContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *NotaddsubContext) Unarynotaddsub() IUnarynotaddsubContext {
	var t antlr.RuleContext
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IUnarynotaddsubContext); ok {
			t = ctx.(antlr.RuleContext)
			break
		}
	}

	if t == nil {
		return nil
	}

	return t.(IUnarynotaddsubContext)
}

func (s *NotaddsubContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PainlessParserListener); ok {
		listenerT.EnterNotaddsub(s)
	}
}

func (s *NotaddsubContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PainlessParserListener); ok {
		listenerT.ExitNotaddsub(s)
	}
}

func (s *NotaddsubContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PainlessParserVisitor:
		return t.VisitNotaddsub(s)

	default:
		return t.VisitChildren(s)
	}
}

type PreContext struct {
	UnaryContext
}

func NewPreContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *PreContext {
	var p = new(PreContext)

	InitEmptyUnaryContext(&p.UnaryContext)
	p.parser = parser
	p.CopyAll(ctx.(*UnaryContext))

	return p
}

func (s *PreContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *PreContext) Chain() IChainContext {
	var t antlr.RuleContext
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IChainContext); ok {
			t = ctx.(antlr.RuleContext)
			break
		}
	}

	if t == nil {
		return nil
	}

	return t.(IChainContext)
}

func (s *PreContext) INCR() antlr.TerminalNode {
	return s.GetToken(PainlessParserINCR, 0)
}

func (s *PreContext) DECR() antlr.TerminalNode {
	return s.GetToken(PainlessParserDECR, 0)
}

func (s *PreContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PainlessParserListener); ok {
		listenerT.EnterPre(s)
	}
}

func (s *PreContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PainlessParserListener); ok {
		listenerT.ExitPre(s)
	}
}

func (s *PreContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PainlessParserVisitor:
		return t.VisitPre(s)

	default:
		return t.VisitChildren(s)
	}
}

type AddsubContext struct {
	UnaryContext
}

func NewAddsubContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *AddsubContext {
	var p = new(AddsubContext)

	InitEmptyUnaryContext(&p.UnaryContext)
	p.parser = parser
	p.CopyAll(ctx.(*UnaryContext))

	return p
}

func (s *AddsubContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *AddsubContext) Unary() IUnaryContext {
	var t antlr.RuleContext
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IUnaryContext); ok {
			t = ctx.(antlr.RuleContext)
			break
		}
	}

	if t == nil {
		return nil
	}

	return t.(IUnaryContext)
}

func (s *AddsubContext) ADD() antlr.TerminalNode {
	return s.GetToken(PainlessParserADD, 0)
}

func (s *AddsubContext) SUB() antlr.TerminalNode {
	return s.GetToken(PainlessParserSUB, 0)
}

func (s *AddsubContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PainlessParserListener); ok {
		listenerT.EnterAddsub(s)
	}
}

func (s *AddsubContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PainlessParserListener); ok {
		listenerT.ExitAddsub(s)
	}
}

func (s *AddsubContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PainlessParserVisitor:
		return t.VisitAddsub(s)

	default:
		return t.VisitChildren(s)
	}
}

func (p *PainlessParser) Unary() (localctx IUnaryContext) {
	localctx = NewUnaryContext(p, p.GetParserRuleContext(), p.GetState())
	p.EnterRule(localctx, 36, PainlessParserRULE_unary)
	var _la int

	p.SetState(328)
	p.GetErrorHandler().Sync(p)
	if p.HasError() {
		goto errorExit
	}

	switch p.GetTokenStream().LA(1) {
	case PainlessParserINCR, PainlessParserDECR:
		localctx = NewPreContext(p, localctx)
		p.EnterOuterAlt(localctx, 1)
		{
			p.SetState(323)
			_la = p.GetTokenStream().LA(1)

			if !(_la == PainlessParserINCR || _la == PainlessParserDECR) {
				p.GetErrorHandler().RecoverInline(p)
			} else {
				p.GetErrorHandler().ReportMatch(p)
				p.Consume()
			}
		}
		{
			p.SetState(324)
			p.Chain()
		}

	case PainlessParserADD, PainlessParserSUB:
		localctx = NewAddsubContext(p, localctx)
		p.EnterOuterAlt(localctx, 2)
		{
			p.SetState(325)
			_la = p.GetTokenStream().LA(1)

			if !(_la == PainlessParserADD || _la == PainlessParserSUB) {
				p.GetErrorHandler().RecoverInline(p)
			} else {
				p.GetErrorHandler().ReportMatch(p)
				p.Consume()
			}
		}
		{
			p.SetState(326)
			p.Unary()
		}

	case PainlessParserLBRACE, PainlessParserLP, PainlessParserNEW, PainlessParserBOOLNOT, PainlessParserBWNOT, PainlessParserOCTAL, PainlessParserHEX, PainlessParserINTEGER, PainlessParserDECIMAL, PainlessParserSTRING, PainlessParserREGEX, PainlessParserTRUE, PainlessParserFALSE, PainlessParserNULL, PainlessParserID:
		localctx = NewNotaddsubContext(p, localctx)
		p.EnterOuterAlt(localctx, 3)
		{
			p.SetState(327)
			p.Unarynotaddsub()
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
	return localctx
	goto errorExit // Trick to prevent compiler error if the label is not used
}

// IUnarynotaddsubContext is an interface to support dynamic dispatch.
type IUnarynotaddsubContext interface {
	antlr.ParserRuleContext

	// GetParser returns the parser.
	GetParser() antlr.Parser
	// IsUnarynotaddsubContext differentiates from other interfaces.
	IsUnarynotaddsubContext()
}

type UnarynotaddsubContext struct {
	antlr.BaseParserRuleContext
	parser antlr.Parser
}

func NewEmptyUnarynotaddsubContext() *UnarynotaddsubContext {
	var p = new(UnarynotaddsubContext)
	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, nil, -1)
	p.RuleIndex = PainlessParserRULE_unarynotaddsub
	return p
}

func InitEmptyUnarynotaddsubContext(p *UnarynotaddsubContext) {
	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, nil, -1)
	p.RuleIndex = PainlessParserRULE_unarynotaddsub
}

func (*UnarynotaddsubContext) IsUnarynotaddsubContext() {}

func NewUnarynotaddsubContext(parser antlr.Parser, parent antlr.ParserRuleContext, invokingState int) *UnarynotaddsubContext {
	var p = new(UnarynotaddsubContext)

	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, parent, invokingState)

	p.parser = parser
	p.RuleIndex = PainlessParserRULE_unarynotaddsub

	return p
}

func (s *UnarynotaddsubContext) GetParser() antlr.Parser { return s.parser }

func (s *UnarynotaddsubContext) CopyAll(ctx *UnarynotaddsubContext) {
	s.CopyFrom(&ctx.BaseParserRuleContext)
}

func (s *UnarynotaddsubContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *UnarynotaddsubContext) ToStringTree(ruleNames []string, recog antlr.Recognizer) string {
	return antlr.TreesStringTree(s, ruleNames, recog)
}

type CastContext struct {
	UnarynotaddsubContext
}

func NewCastContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *CastContext {
	var p = new(CastContext)

	InitEmptyUnarynotaddsubContext(&p.UnarynotaddsubContext)
	p.parser = parser
	p.CopyAll(ctx.(*UnarynotaddsubContext))

	return p
}

func (s *CastContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *CastContext) Castexpression() ICastexpressionContext {
	var t antlr.RuleContext
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(ICastexpressionContext); ok {
			t = ctx.(antlr.RuleContext)
			break
		}
	}

	if t == nil {
		return nil
	}

	return t.(ICastexpressionContext)
}

func (s *CastContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PainlessParserListener); ok {
		listenerT.EnterCast(s)
	}
}

func (s *CastContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PainlessParserListener); ok {
		listenerT.ExitCast(s)
	}
}

func (s *CastContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PainlessParserVisitor:
		return t.VisitCast(s)

	default:
		return t.VisitChildren(s)
	}
}

type NotContext struct {
	UnarynotaddsubContext
}

func NewNotContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *NotContext {
	var p = new(NotContext)

	InitEmptyUnarynotaddsubContext(&p.UnarynotaddsubContext)
	p.parser = parser
	p.CopyAll(ctx.(*UnarynotaddsubContext))

	return p
}

func (s *NotContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *NotContext) Unary() IUnaryContext {
	var t antlr.RuleContext
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IUnaryContext); ok {
			t = ctx.(antlr.RuleContext)
			break
		}
	}

	if t == nil {
		return nil
	}

	return t.(IUnaryContext)
}

func (s *NotContext) BOOLNOT() antlr.TerminalNode {
	return s.GetToken(PainlessParserBOOLNOT, 0)
}

func (s *NotContext) BWNOT() antlr.TerminalNode {
	return s.GetToken(PainlessParserBWNOT, 0)
}

func (s *NotContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PainlessParserListener); ok {
		listenerT.EnterNot(s)
	}
}

func (s *NotContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PainlessParserListener); ok {
		listenerT.ExitNot(s)
	}
}

func (s *NotContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PainlessParserVisitor:
		return t.VisitNot(s)

	default:
		return t.VisitChildren(s)
	}
}

type ReadContext struct {
	UnarynotaddsubContext
}

func NewReadContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *ReadContext {
	var p = new(ReadContext)

	InitEmptyUnarynotaddsubContext(&p.UnarynotaddsubContext)
	p.parser = parser
	p.CopyAll(ctx.(*UnarynotaddsubContext))

	return p
}

func (s *ReadContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *ReadContext) Chain() IChainContext {
	var t antlr.RuleContext
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IChainContext); ok {
			t = ctx.(antlr.RuleContext)
			break
		}
	}

	if t == nil {
		return nil
	}

	return t.(IChainContext)
}

func (s *ReadContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PainlessParserListener); ok {
		listenerT.EnterRead(s)
	}
}

func (s *ReadContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PainlessParserListener); ok {
		listenerT.ExitRead(s)
	}
}

func (s *ReadContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PainlessParserVisitor:
		return t.VisitRead(s)

	default:
		return t.VisitChildren(s)
	}
}

type PostContext struct {
	UnarynotaddsubContext
}

func NewPostContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *PostContext {
	var p = new(PostContext)

	InitEmptyUnarynotaddsubContext(&p.UnarynotaddsubContext)
	p.parser = parser
	p.CopyAll(ctx.(*UnarynotaddsubContext))

	return p
}

func (s *PostContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *PostContext) Chain() IChainContext {
	var t antlr.RuleContext
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IChainContext); ok {
			t = ctx.(antlr.RuleContext)
			break
		}
	}

	if t == nil {
		return nil
	}

	return t.(IChainContext)
}

func (s *PostContext) INCR() antlr.TerminalNode {
	return s.GetToken(PainlessParserINCR, 0)
}

func (s *PostContext) DECR() antlr.TerminalNode {
	return s.GetToken(PainlessParserDECR, 0)
}

func (s *PostContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PainlessParserListener); ok {
		listenerT.EnterPost(s)
	}
}

func (s *PostContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PainlessParserListener); ok {
		listenerT.ExitPost(s)
	}
}

func (s *PostContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PainlessParserVisitor:
		return t.VisitPost(s)

	default:
		return t.VisitChildren(s)
	}
}

func (p *PainlessParser) Unarynotaddsub() (localctx IUnarynotaddsubContext) {
	localctx = NewUnarynotaddsubContext(p, p.GetParserRuleContext(), p.GetState())
	p.EnterRule(localctx, 38, PainlessParserRULE_unarynotaddsub)
	var _la int

	p.SetState(337)
	p.GetErrorHandler().Sync(p)
	if p.HasError() {
		goto errorExit
	}

	switch p.GetInterpreter().AdaptivePredict(p.BaseParser, p.GetTokenStream(), 27, p.GetParserRuleContext()) {
	case 1:
		localctx = NewReadContext(p, localctx)
		p.EnterOuterAlt(localctx, 1)
		{
			p.SetState(330)
			p.Chain()
		}

	case 2:
		localctx = NewPostContext(p, localctx)
		p.EnterOuterAlt(localctx, 2)
		{
			p.SetState(331)
			p.Chain()
		}
		{
			p.SetState(332)
			_la = p.GetTokenStream().LA(1)

			if !(_la == PainlessParserINCR || _la == PainlessParserDECR) {
				p.GetErrorHandler().RecoverInline(p)
			} else {
				p.GetErrorHandler().ReportMatch(p)
				p.Consume()
			}
		}

	case 3:
		localctx = NewNotContext(p, localctx)
		p.EnterOuterAlt(localctx, 3)
		{
			p.SetState(334)
			_la = p.GetTokenStream().LA(1)

			if !(_la == PainlessParserBOOLNOT || _la == PainlessParserBWNOT) {
				p.GetErrorHandler().RecoverInline(p)
			} else {
				p.GetErrorHandler().ReportMatch(p)
				p.Consume()
			}
		}
		{
			p.SetState(335)
			p.Unary()
		}

	case 4:
		localctx = NewCastContext(p, localctx)
		p.EnterOuterAlt(localctx, 4)
		{
			p.SetState(336)
			p.Castexpression()
		}

	case antlr.ATNInvalidAltNumber:
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
	return localctx
	goto errorExit // Trick to prevent compiler error if the label is not used
}

// ICastexpressionContext is an interface to support dynamic dispatch.
type ICastexpressionContext interface {
	antlr.ParserRuleContext

	// GetParser returns the parser.
	GetParser() antlr.Parser
	// IsCastexpressionContext differentiates from other interfaces.
	IsCastexpressionContext()
}

type CastexpressionContext struct {
	antlr.BaseParserRuleContext
	parser antlr.Parser
}

func NewEmptyCastexpressionContext() *CastexpressionContext {
	var p = new(CastexpressionContext)
	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, nil, -1)
	p.RuleIndex = PainlessParserRULE_castexpression
	return p
}

func InitEmptyCastexpressionContext(p *CastexpressionContext) {
	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, nil, -1)
	p.RuleIndex = PainlessParserRULE_castexpression
}

func (*CastexpressionContext) IsCastexpressionContext() {}

func NewCastexpressionContext(parser antlr.Parser, parent antlr.ParserRuleContext, invokingState int) *CastexpressionContext {
	var p = new(CastexpressionContext)

	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, parent, invokingState)

	p.parser = parser
	p.RuleIndex = PainlessParserRULE_castexpression

	return p
}

func (s *CastexpressionContext) GetParser() antlr.Parser { return s.parser }

func (s *CastexpressionContext) CopyAll(ctx *CastexpressionContext) {
	s.CopyFrom(&ctx.BaseParserRuleContext)
}

func (s *CastexpressionContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *CastexpressionContext) ToStringTree(ruleNames []string, recog antlr.Recognizer) string {
	return antlr.TreesStringTree(s, ruleNames, recog)
}

type RefcastContext struct {
	CastexpressionContext
}

func NewRefcastContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *RefcastContext {
	var p = new(RefcastContext)

	InitEmptyCastexpressionContext(&p.CastexpressionContext)
	p.parser = parser
	p.CopyAll(ctx.(*CastexpressionContext))

	return p
}

func (s *RefcastContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *RefcastContext) LP() antlr.TerminalNode {
	return s.GetToken(PainlessParserLP, 0)
}

func (s *RefcastContext) Refcasttype() IRefcasttypeContext {
	var t antlr.RuleContext
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IRefcasttypeContext); ok {
			t = ctx.(antlr.RuleContext)
			break
		}
	}

	if t == nil {
		return nil
	}

	return t.(IRefcasttypeContext)
}

func (s *RefcastContext) RP() antlr.TerminalNode {
	return s.GetToken(PainlessParserRP, 0)
}

func (s *RefcastContext) Unarynotaddsub() IUnarynotaddsubContext {
	var t antlr.RuleContext
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IUnarynotaddsubContext); ok {
			t = ctx.(antlr.RuleContext)
			break
		}
	}

	if t == nil {
		return nil
	}

	return t.(IUnarynotaddsubContext)
}

func (s *RefcastContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PainlessParserListener); ok {
		listenerT.EnterRefcast(s)
	}
}

func (s *RefcastContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PainlessParserListener); ok {
		listenerT.ExitRefcast(s)
	}
}

func (s *RefcastContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PainlessParserVisitor:
		return t.VisitRefcast(s)

	default:
		return t.VisitChildren(s)
	}
}

type PrimordefcastContext struct {
	CastexpressionContext
}

func NewPrimordefcastContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *PrimordefcastContext {
	var p = new(PrimordefcastContext)

	InitEmptyCastexpressionContext(&p.CastexpressionContext)
	p.parser = parser
	p.CopyAll(ctx.(*CastexpressionContext))

	return p
}

func (s *PrimordefcastContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *PrimordefcastContext) LP() antlr.TerminalNode {
	return s.GetToken(PainlessParserLP, 0)
}

func (s *PrimordefcastContext) Primordefcasttype() IPrimordefcasttypeContext {
	var t antlr.RuleContext
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IPrimordefcasttypeContext); ok {
			t = ctx.(antlr.RuleContext)
			break
		}
	}

	if t == nil {
		return nil
	}

	return t.(IPrimordefcasttypeContext)
}

func (s *PrimordefcastContext) RP() antlr.TerminalNode {
	return s.GetToken(PainlessParserRP, 0)
}

func (s *PrimordefcastContext) Unary() IUnaryContext {
	var t antlr.RuleContext
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IUnaryContext); ok {
			t = ctx.(antlr.RuleContext)
			break
		}
	}

	if t == nil {
		return nil
	}

	return t.(IUnaryContext)
}

func (s *PrimordefcastContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PainlessParserListener); ok {
		listenerT.EnterPrimordefcast(s)
	}
}

func (s *PrimordefcastContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PainlessParserListener); ok {
		listenerT.ExitPrimordefcast(s)
	}
}

func (s *PrimordefcastContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PainlessParserVisitor:
		return t.VisitPrimordefcast(s)

	default:
		return t.VisitChildren(s)
	}
}

func (p *PainlessParser) Castexpression() (localctx ICastexpressionContext) {
	localctx = NewCastexpressionContext(p, p.GetParserRuleContext(), p.GetState())
	p.EnterRule(localctx, 40, PainlessParserRULE_castexpression)
	p.SetState(349)
	p.GetErrorHandler().Sync(p)
	if p.HasError() {
		goto errorExit
	}

	switch p.GetInterpreter().AdaptivePredict(p.BaseParser, p.GetTokenStream(), 28, p.GetParserRuleContext()) {
	case 1:
		localctx = NewPrimordefcastContext(p, localctx)
		p.EnterOuterAlt(localctx, 1)
		{
			p.SetState(339)
			p.Match(PainlessParserLP)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(340)
			p.Primordefcasttype()
		}
		{
			p.SetState(341)
			p.Match(PainlessParserRP)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(342)
			p.Unary()
		}

	case 2:
		localctx = NewRefcastContext(p, localctx)
		p.EnterOuterAlt(localctx, 2)
		{
			p.SetState(344)
			p.Match(PainlessParserLP)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(345)
			p.Refcasttype()
		}
		{
			p.SetState(346)
			p.Match(PainlessParserRP)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(347)
			p.Unarynotaddsub()
		}

	case antlr.ATNInvalidAltNumber:
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
	return localctx
	goto errorExit // Trick to prevent compiler error if the label is not used
}

// IPrimordefcasttypeContext is an interface to support dynamic dispatch.
type IPrimordefcasttypeContext interface {
	antlr.ParserRuleContext

	// GetParser returns the parser.
	GetParser() antlr.Parser

	// Getter signatures
	DEF() antlr.TerminalNode
	PRIMITIVE() antlr.TerminalNode

	// IsPrimordefcasttypeContext differentiates from other interfaces.
	IsPrimordefcasttypeContext()
}

type PrimordefcasttypeContext struct {
	antlr.BaseParserRuleContext
	parser antlr.Parser
}

func NewEmptyPrimordefcasttypeContext() *PrimordefcasttypeContext {
	var p = new(PrimordefcasttypeContext)
	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, nil, -1)
	p.RuleIndex = PainlessParserRULE_primordefcasttype
	return p
}

func InitEmptyPrimordefcasttypeContext(p *PrimordefcasttypeContext) {
	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, nil, -1)
	p.RuleIndex = PainlessParserRULE_primordefcasttype
}

func (*PrimordefcasttypeContext) IsPrimordefcasttypeContext() {}

func NewPrimordefcasttypeContext(parser antlr.Parser, parent antlr.ParserRuleContext, invokingState int) *PrimordefcasttypeContext {
	var p = new(PrimordefcasttypeContext)

	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, parent, invokingState)

	p.parser = parser
	p.RuleIndex = PainlessParserRULE_primordefcasttype

	return p
}

func (s *PrimordefcasttypeContext) GetParser() antlr.Parser { return s.parser }

func (s *PrimordefcasttypeContext) DEF() antlr.TerminalNode {
	return s.GetToken(PainlessParserDEF, 0)
}

func (s *PrimordefcasttypeContext) PRIMITIVE() antlr.TerminalNode {
	return s.GetToken(PainlessParserPRIMITIVE, 0)
}

func (s *PrimordefcasttypeContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *PrimordefcasttypeContext) ToStringTree(ruleNames []string, recog antlr.Recognizer) string {
	return antlr.TreesStringTree(s, ruleNames, recog)
}

func (s *PrimordefcasttypeContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PainlessParserListener); ok {
		listenerT.EnterPrimordefcasttype(s)
	}
}

func (s *PrimordefcasttypeContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PainlessParserListener); ok {
		listenerT.ExitPrimordefcasttype(s)
	}
}

func (s *PrimordefcasttypeContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PainlessParserVisitor:
		return t.VisitPrimordefcasttype(s)

	default:
		return t.VisitChildren(s)
	}
}

func (p *PainlessParser) Primordefcasttype() (localctx IPrimordefcasttypeContext) {
	localctx = NewPrimordefcasttypeContext(p, p.GetParserRuleContext(), p.GetState())
	p.EnterRule(localctx, 42, PainlessParserRULE_primordefcasttype)
	var _la int

	p.EnterOuterAlt(localctx, 1)
	{
		p.SetState(351)
		_la = p.GetTokenStream().LA(1)

		if !(_la == PainlessParserPRIMITIVE || _la == PainlessParserDEF) {
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
	return localctx
	goto errorExit // Trick to prevent compiler error if the label is not used
}

// IRefcasttypeContext is an interface to support dynamic dispatch.
type IRefcasttypeContext interface {
	antlr.ParserRuleContext

	// GetParser returns the parser.
	GetParser() antlr.Parser

	// Getter signatures
	DEF() antlr.TerminalNode
	AllLBRACE() []antlr.TerminalNode
	LBRACE(i int) antlr.TerminalNode
	AllRBRACE() []antlr.TerminalNode
	RBRACE(i int) antlr.TerminalNode
	PRIMITIVE() antlr.TerminalNode
	ID() antlr.TerminalNode
	AllDOT() []antlr.TerminalNode
	DOT(i int) antlr.TerminalNode
	AllDOTID() []antlr.TerminalNode
	DOTID(i int) antlr.TerminalNode

	// IsRefcasttypeContext differentiates from other interfaces.
	IsRefcasttypeContext()
}

type RefcasttypeContext struct {
	antlr.BaseParserRuleContext
	parser antlr.Parser
}

func NewEmptyRefcasttypeContext() *RefcasttypeContext {
	var p = new(RefcasttypeContext)
	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, nil, -1)
	p.RuleIndex = PainlessParserRULE_refcasttype
	return p
}

func InitEmptyRefcasttypeContext(p *RefcasttypeContext) {
	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, nil, -1)
	p.RuleIndex = PainlessParserRULE_refcasttype
}

func (*RefcasttypeContext) IsRefcasttypeContext() {}

func NewRefcasttypeContext(parser antlr.Parser, parent antlr.ParserRuleContext, invokingState int) *RefcasttypeContext {
	var p = new(RefcasttypeContext)

	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, parent, invokingState)

	p.parser = parser
	p.RuleIndex = PainlessParserRULE_refcasttype

	return p
}

func (s *RefcasttypeContext) GetParser() antlr.Parser { return s.parser }

func (s *RefcasttypeContext) DEF() antlr.TerminalNode {
	return s.GetToken(PainlessParserDEF, 0)
}

func (s *RefcasttypeContext) AllLBRACE() []antlr.TerminalNode {
	return s.GetTokens(PainlessParserLBRACE)
}

func (s *RefcasttypeContext) LBRACE(i int) antlr.TerminalNode {
	return s.GetToken(PainlessParserLBRACE, i)
}

func (s *RefcasttypeContext) AllRBRACE() []antlr.TerminalNode {
	return s.GetTokens(PainlessParserRBRACE)
}

func (s *RefcasttypeContext) RBRACE(i int) antlr.TerminalNode {
	return s.GetToken(PainlessParserRBRACE, i)
}

func (s *RefcasttypeContext) PRIMITIVE() antlr.TerminalNode {
	return s.GetToken(PainlessParserPRIMITIVE, 0)
}

func (s *RefcasttypeContext) ID() antlr.TerminalNode {
	return s.GetToken(PainlessParserID, 0)
}

func (s *RefcasttypeContext) AllDOT() []antlr.TerminalNode {
	return s.GetTokens(PainlessParserDOT)
}

func (s *RefcasttypeContext) DOT(i int) antlr.TerminalNode {
	return s.GetToken(PainlessParserDOT, i)
}

func (s *RefcasttypeContext) AllDOTID() []antlr.TerminalNode {
	return s.GetTokens(PainlessParserDOTID)
}

func (s *RefcasttypeContext) DOTID(i int) antlr.TerminalNode {
	return s.GetToken(PainlessParserDOTID, i)
}

func (s *RefcasttypeContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *RefcasttypeContext) ToStringTree(ruleNames []string, recog antlr.Recognizer) string {
	return antlr.TreesStringTree(s, ruleNames, recog)
}

func (s *RefcasttypeContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PainlessParserListener); ok {
		listenerT.EnterRefcasttype(s)
	}
}

func (s *RefcasttypeContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PainlessParserListener); ok {
		listenerT.ExitRefcasttype(s)
	}
}

func (s *RefcasttypeContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PainlessParserVisitor:
		return t.VisitRefcasttype(s)

	default:
		return t.VisitChildren(s)
	}
}

func (p *PainlessParser) Refcasttype() (localctx IRefcasttypeContext) {
	localctx = NewRefcasttypeContext(p, p.GetParserRuleContext(), p.GetState())
	p.EnterRule(localctx, 44, PainlessParserRULE_refcasttype)
	var _la int

	p.SetState(382)
	p.GetErrorHandler().Sync(p)
	if p.HasError() {
		goto errorExit
	}

	switch p.GetTokenStream().LA(1) {
	case PainlessParserDEF:
		p.EnterOuterAlt(localctx, 1)
		{
			p.SetState(353)
			p.Match(PainlessParserDEF)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		p.SetState(356)
		p.GetErrorHandler().Sync(p)
		if p.HasError() {
			goto errorExit
		}
		_la = p.GetTokenStream().LA(1)

		for ok := true; ok; ok = _la == PainlessParserLBRACE {
			{
				p.SetState(354)
				p.Match(PainlessParserLBRACE)
				if p.HasError() {
					// Recognition error - abort rule
					goto errorExit
				}
			}
			{
				p.SetState(355)
				p.Match(PainlessParserRBRACE)
				if p.HasError() {
					// Recognition error - abort rule
					goto errorExit
				}
			}

			p.SetState(358)
			p.GetErrorHandler().Sync(p)
			if p.HasError() {
				goto errorExit
			}
			_la = p.GetTokenStream().LA(1)
		}

	case PainlessParserPRIMITIVE:
		p.EnterOuterAlt(localctx, 2)
		{
			p.SetState(360)
			p.Match(PainlessParserPRIMITIVE)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		p.SetState(363)
		p.GetErrorHandler().Sync(p)
		if p.HasError() {
			goto errorExit
		}
		_la = p.GetTokenStream().LA(1)

		for ok := true; ok; ok = _la == PainlessParserLBRACE {
			{
				p.SetState(361)
				p.Match(PainlessParserLBRACE)
				if p.HasError() {
					// Recognition error - abort rule
					goto errorExit
				}
			}
			{
				p.SetState(362)
				p.Match(PainlessParserRBRACE)
				if p.HasError() {
					// Recognition error - abort rule
					goto errorExit
				}
			}

			p.SetState(365)
			p.GetErrorHandler().Sync(p)
			if p.HasError() {
				goto errorExit
			}
			_la = p.GetTokenStream().LA(1)
		}

	case PainlessParserID:
		p.EnterOuterAlt(localctx, 3)
		{
			p.SetState(367)
			p.Match(PainlessParserID)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		p.SetState(372)
		p.GetErrorHandler().Sync(p)
		if p.HasError() {
			goto errorExit
		}
		_la = p.GetTokenStream().LA(1)

		for _la == PainlessParserDOT {
			{
				p.SetState(368)
				p.Match(PainlessParserDOT)
				if p.HasError() {
					// Recognition error - abort rule
					goto errorExit
				}
			}
			{
				p.SetState(369)
				p.Match(PainlessParserDOTID)
				if p.HasError() {
					// Recognition error - abort rule
					goto errorExit
				}
			}

			p.SetState(374)
			p.GetErrorHandler().Sync(p)
			if p.HasError() {
				goto errorExit
			}
			_la = p.GetTokenStream().LA(1)
		}
		p.SetState(379)
		p.GetErrorHandler().Sync(p)
		if p.HasError() {
			goto errorExit
		}
		_la = p.GetTokenStream().LA(1)

		for _la == PainlessParserLBRACE {
			{
				p.SetState(375)
				p.Match(PainlessParserLBRACE)
				if p.HasError() {
					// Recognition error - abort rule
					goto errorExit
				}
			}
			{
				p.SetState(376)
				p.Match(PainlessParserRBRACE)
				if p.HasError() {
					// Recognition error - abort rule
					goto errorExit
				}
			}

			p.SetState(381)
			p.GetErrorHandler().Sync(p)
			if p.HasError() {
				goto errorExit
			}
			_la = p.GetTokenStream().LA(1)
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
	return localctx
	goto errorExit // Trick to prevent compiler error if the label is not used
}

// IChainContext is an interface to support dynamic dispatch.
type IChainContext interface {
	antlr.ParserRuleContext

	// GetParser returns the parser.
	GetParser() antlr.Parser
	// IsChainContext differentiates from other interfaces.
	IsChainContext()
}

type ChainContext struct {
	antlr.BaseParserRuleContext
	parser antlr.Parser
}

func NewEmptyChainContext() *ChainContext {
	var p = new(ChainContext)
	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, nil, -1)
	p.RuleIndex = PainlessParserRULE_chain
	return p
}

func InitEmptyChainContext(p *ChainContext) {
	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, nil, -1)
	p.RuleIndex = PainlessParserRULE_chain
}

func (*ChainContext) IsChainContext() {}

func NewChainContext(parser antlr.Parser, parent antlr.ParserRuleContext, invokingState int) *ChainContext {
	var p = new(ChainContext)

	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, parent, invokingState)

	p.parser = parser
	p.RuleIndex = PainlessParserRULE_chain

	return p
}

func (s *ChainContext) GetParser() antlr.Parser { return s.parser }

func (s *ChainContext) CopyAll(ctx *ChainContext) {
	s.CopyFrom(&ctx.BaseParserRuleContext)
}

func (s *ChainContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *ChainContext) ToStringTree(ruleNames []string, recog antlr.Recognizer) string {
	return antlr.TreesStringTree(s, ruleNames, recog)
}

type DynamicContext struct {
	ChainContext
}

func NewDynamicContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *DynamicContext {
	var p = new(DynamicContext)

	InitEmptyChainContext(&p.ChainContext)
	p.parser = parser
	p.CopyAll(ctx.(*ChainContext))

	return p
}

func (s *DynamicContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *DynamicContext) Primary() IPrimaryContext {
	var t antlr.RuleContext
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IPrimaryContext); ok {
			t = ctx.(antlr.RuleContext)
			break
		}
	}

	if t == nil {
		return nil
	}

	return t.(IPrimaryContext)
}

func (s *DynamicContext) AllPostfix() []IPostfixContext {
	children := s.GetChildren()
	len := 0
	for _, ctx := range children {
		if _, ok := ctx.(IPostfixContext); ok {
			len++
		}
	}

	tst := make([]IPostfixContext, len)
	i := 0
	for _, ctx := range children {
		if t, ok := ctx.(IPostfixContext); ok {
			tst[i] = t.(IPostfixContext)
			i++
		}
	}

	return tst
}

func (s *DynamicContext) Postfix(i int) IPostfixContext {
	var t antlr.RuleContext
	j := 0
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IPostfixContext); ok {
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

	return t.(IPostfixContext)
}

func (s *DynamicContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PainlessParserListener); ok {
		listenerT.EnterDynamic(s)
	}
}

func (s *DynamicContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PainlessParserListener); ok {
		listenerT.ExitDynamic(s)
	}
}

func (s *DynamicContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PainlessParserVisitor:
		return t.VisitDynamic(s)

	default:
		return t.VisitChildren(s)
	}
}

type NewarrayContext struct {
	ChainContext
}

func NewNewarrayContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *NewarrayContext {
	var p = new(NewarrayContext)

	InitEmptyChainContext(&p.ChainContext)
	p.parser = parser
	p.CopyAll(ctx.(*ChainContext))

	return p
}

func (s *NewarrayContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *NewarrayContext) Arrayinitializer() IArrayinitializerContext {
	var t antlr.RuleContext
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IArrayinitializerContext); ok {
			t = ctx.(antlr.RuleContext)
			break
		}
	}

	if t == nil {
		return nil
	}

	return t.(IArrayinitializerContext)
}

func (s *NewarrayContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PainlessParserListener); ok {
		listenerT.EnterNewarray(s)
	}
}

func (s *NewarrayContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PainlessParserListener); ok {
		listenerT.ExitNewarray(s)
	}
}

func (s *NewarrayContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PainlessParserVisitor:
		return t.VisitNewarray(s)

	default:
		return t.VisitChildren(s)
	}
}

func (p *PainlessParser) Chain() (localctx IChainContext) {
	localctx = NewChainContext(p, p.GetParserRuleContext(), p.GetState())
	p.EnterRule(localctx, 46, PainlessParserRULE_chain)
	var _alt int

	p.SetState(392)
	p.GetErrorHandler().Sync(p)
	if p.HasError() {
		goto errorExit
	}

	switch p.GetInterpreter().AdaptivePredict(p.BaseParser, p.GetTokenStream(), 35, p.GetParserRuleContext()) {
	case 1:
		localctx = NewDynamicContext(p, localctx)
		p.EnterOuterAlt(localctx, 1)
		{
			p.SetState(384)
			p.Primary()
		}
		p.SetState(388)
		p.GetErrorHandler().Sync(p)
		if p.HasError() {
			goto errorExit
		}
		_alt = p.GetInterpreter().AdaptivePredict(p.BaseParser, p.GetTokenStream(), 34, p.GetParserRuleContext())
		if p.HasError() {
			goto errorExit
		}
		for _alt != 2 && _alt != antlr.ATNInvalidAltNumber {
			if _alt == 1 {
				{
					p.SetState(385)
					p.Postfix()
				}

			}
			p.SetState(390)
			p.GetErrorHandler().Sync(p)
			if p.HasError() {
				goto errorExit
			}
			_alt = p.GetInterpreter().AdaptivePredict(p.BaseParser, p.GetTokenStream(), 34, p.GetParserRuleContext())
			if p.HasError() {
				goto errorExit
			}
		}

	case 2:
		localctx = NewNewarrayContext(p, localctx)
		p.EnterOuterAlt(localctx, 2)
		{
			p.SetState(391)
			p.Arrayinitializer()
		}

	case antlr.ATNInvalidAltNumber:
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
	return localctx
	goto errorExit // Trick to prevent compiler error if the label is not used
}

// IPrimaryContext is an interface to support dynamic dispatch.
type IPrimaryContext interface {
	antlr.ParserRuleContext

	// GetParser returns the parser.
	GetParser() antlr.Parser
	// IsPrimaryContext differentiates from other interfaces.
	IsPrimaryContext()
}

type PrimaryContext struct {
	antlr.BaseParserRuleContext
	parser antlr.Parser
}

func NewEmptyPrimaryContext() *PrimaryContext {
	var p = new(PrimaryContext)
	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, nil, -1)
	p.RuleIndex = PainlessParserRULE_primary
	return p
}

func InitEmptyPrimaryContext(p *PrimaryContext) {
	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, nil, -1)
	p.RuleIndex = PainlessParserRULE_primary
}

func (*PrimaryContext) IsPrimaryContext() {}

func NewPrimaryContext(parser antlr.Parser, parent antlr.ParserRuleContext, invokingState int) *PrimaryContext {
	var p = new(PrimaryContext)

	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, parent, invokingState)

	p.parser = parser
	p.RuleIndex = PainlessParserRULE_primary

	return p
}

func (s *PrimaryContext) GetParser() antlr.Parser { return s.parser }

func (s *PrimaryContext) CopyAll(ctx *PrimaryContext) {
	s.CopyFrom(&ctx.BaseParserRuleContext)
}

func (s *PrimaryContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *PrimaryContext) ToStringTree(ruleNames []string, recog antlr.Recognizer) string {
	return antlr.TreesStringTree(s, ruleNames, recog)
}

type ListinitContext struct {
	PrimaryContext
}

func NewListinitContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *ListinitContext {
	var p = new(ListinitContext)

	InitEmptyPrimaryContext(&p.PrimaryContext)
	p.parser = parser
	p.CopyAll(ctx.(*PrimaryContext))

	return p
}

func (s *ListinitContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *ListinitContext) Listinitializer() IListinitializerContext {
	var t antlr.RuleContext
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IListinitializerContext); ok {
			t = ctx.(antlr.RuleContext)
			break
		}
	}

	if t == nil {
		return nil
	}

	return t.(IListinitializerContext)
}

func (s *ListinitContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PainlessParserListener); ok {
		listenerT.EnterListinit(s)
	}
}

func (s *ListinitContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PainlessParserListener); ok {
		listenerT.ExitListinit(s)
	}
}

func (s *ListinitContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PainlessParserVisitor:
		return t.VisitListinit(s)

	default:
		return t.VisitChildren(s)
	}
}

type RegexContext struct {
	PrimaryContext
}

func NewRegexContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *RegexContext {
	var p = new(RegexContext)

	InitEmptyPrimaryContext(&p.PrimaryContext)
	p.parser = parser
	p.CopyAll(ctx.(*PrimaryContext))

	return p
}

func (s *RegexContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *RegexContext) REGEX() antlr.TerminalNode {
	return s.GetToken(PainlessParserREGEX, 0)
}

func (s *RegexContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PainlessParserListener); ok {
		listenerT.EnterRegex(s)
	}
}

func (s *RegexContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PainlessParserListener); ok {
		listenerT.ExitRegex(s)
	}
}

func (s *RegexContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PainlessParserVisitor:
		return t.VisitRegex(s)

	default:
		return t.VisitChildren(s)
	}
}

type NullContext struct {
	PrimaryContext
}

func NewNullContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *NullContext {
	var p = new(NullContext)

	InitEmptyPrimaryContext(&p.PrimaryContext)
	p.parser = parser
	p.CopyAll(ctx.(*PrimaryContext))

	return p
}

func (s *NullContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *NullContext) NULL() antlr.TerminalNode {
	return s.GetToken(PainlessParserNULL, 0)
}

func (s *NullContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PainlessParserListener); ok {
		listenerT.EnterNull(s)
	}
}

func (s *NullContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PainlessParserListener); ok {
		listenerT.ExitNull(s)
	}
}

func (s *NullContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PainlessParserVisitor:
		return t.VisitNull(s)

	default:
		return t.VisitChildren(s)
	}
}

type StringContext struct {
	PrimaryContext
}

func NewStringContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *StringContext {
	var p = new(StringContext)

	InitEmptyPrimaryContext(&p.PrimaryContext)
	p.parser = parser
	p.CopyAll(ctx.(*PrimaryContext))

	return p
}

func (s *StringContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *StringContext) STRING() antlr.TerminalNode {
	return s.GetToken(PainlessParserSTRING, 0)
}

func (s *StringContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PainlessParserListener); ok {
		listenerT.EnterString(s)
	}
}

func (s *StringContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PainlessParserListener); ok {
		listenerT.ExitString(s)
	}
}

func (s *StringContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PainlessParserVisitor:
		return t.VisitString(s)

	default:
		return t.VisitChildren(s)
	}
}

type MapinitContext struct {
	PrimaryContext
}

func NewMapinitContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *MapinitContext {
	var p = new(MapinitContext)

	InitEmptyPrimaryContext(&p.PrimaryContext)
	p.parser = parser
	p.CopyAll(ctx.(*PrimaryContext))

	return p
}

func (s *MapinitContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *MapinitContext) Mapinitializer() IMapinitializerContext {
	var t antlr.RuleContext
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IMapinitializerContext); ok {
			t = ctx.(antlr.RuleContext)
			break
		}
	}

	if t == nil {
		return nil
	}

	return t.(IMapinitializerContext)
}

func (s *MapinitContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PainlessParserListener); ok {
		listenerT.EnterMapinit(s)
	}
}

func (s *MapinitContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PainlessParserListener); ok {
		listenerT.ExitMapinit(s)
	}
}

func (s *MapinitContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PainlessParserVisitor:
		return t.VisitMapinit(s)

	default:
		return t.VisitChildren(s)
	}
}

type CalllocalContext struct {
	PrimaryContext
}

func NewCalllocalContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *CalllocalContext {
	var p = new(CalllocalContext)

	InitEmptyPrimaryContext(&p.PrimaryContext)
	p.parser = parser
	p.CopyAll(ctx.(*PrimaryContext))

	return p
}

func (s *CalllocalContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *CalllocalContext) ID() antlr.TerminalNode {
	return s.GetToken(PainlessParserID, 0)
}

func (s *CalllocalContext) Arguments() IArgumentsContext {
	var t antlr.RuleContext
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IArgumentsContext); ok {
			t = ctx.(antlr.RuleContext)
			break
		}
	}

	if t == nil {
		return nil
	}

	return t.(IArgumentsContext)
}

func (s *CalllocalContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PainlessParserListener); ok {
		listenerT.EnterCalllocal(s)
	}
}

func (s *CalllocalContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PainlessParserListener); ok {
		listenerT.ExitCalllocal(s)
	}
}

func (s *CalllocalContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PainlessParserVisitor:
		return t.VisitCalllocal(s)

	default:
		return t.VisitChildren(s)
	}
}

type TrueContext struct {
	PrimaryContext
}

func NewTrueContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *TrueContext {
	var p = new(TrueContext)

	InitEmptyPrimaryContext(&p.PrimaryContext)
	p.parser = parser
	p.CopyAll(ctx.(*PrimaryContext))

	return p
}

func (s *TrueContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *TrueContext) TRUE() antlr.TerminalNode {
	return s.GetToken(PainlessParserTRUE, 0)
}

func (s *TrueContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PainlessParserListener); ok {
		listenerT.EnterTrue(s)
	}
}

func (s *TrueContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PainlessParserListener); ok {
		listenerT.ExitTrue(s)
	}
}

func (s *TrueContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PainlessParserVisitor:
		return t.VisitTrue(s)

	default:
		return t.VisitChildren(s)
	}
}

type FalseContext struct {
	PrimaryContext
}

func NewFalseContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *FalseContext {
	var p = new(FalseContext)

	InitEmptyPrimaryContext(&p.PrimaryContext)
	p.parser = parser
	p.CopyAll(ctx.(*PrimaryContext))

	return p
}

func (s *FalseContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *FalseContext) FALSE() antlr.TerminalNode {
	return s.GetToken(PainlessParserFALSE, 0)
}

func (s *FalseContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PainlessParserListener); ok {
		listenerT.EnterFalse(s)
	}
}

func (s *FalseContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PainlessParserListener); ok {
		listenerT.ExitFalse(s)
	}
}

func (s *FalseContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PainlessParserVisitor:
		return t.VisitFalse(s)

	default:
		return t.VisitChildren(s)
	}
}

type VariableContext struct {
	PrimaryContext
}

func NewVariableContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *VariableContext {
	var p = new(VariableContext)

	InitEmptyPrimaryContext(&p.PrimaryContext)
	p.parser = parser
	p.CopyAll(ctx.(*PrimaryContext))

	return p
}

func (s *VariableContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *VariableContext) ID() antlr.TerminalNode {
	return s.GetToken(PainlessParserID, 0)
}

func (s *VariableContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PainlessParserListener); ok {
		listenerT.EnterVariable(s)
	}
}

func (s *VariableContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PainlessParserListener); ok {
		listenerT.ExitVariable(s)
	}
}

func (s *VariableContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PainlessParserVisitor:
		return t.VisitVariable(s)

	default:
		return t.VisitChildren(s)
	}
}

type NumericContext struct {
	PrimaryContext
}

func NewNumericContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *NumericContext {
	var p = new(NumericContext)

	InitEmptyPrimaryContext(&p.PrimaryContext)
	p.parser = parser
	p.CopyAll(ctx.(*PrimaryContext))

	return p
}

func (s *NumericContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *NumericContext) OCTAL() antlr.TerminalNode {
	return s.GetToken(PainlessParserOCTAL, 0)
}

func (s *NumericContext) HEX() antlr.TerminalNode {
	return s.GetToken(PainlessParserHEX, 0)
}

func (s *NumericContext) INTEGER() antlr.TerminalNode {
	return s.GetToken(PainlessParserINTEGER, 0)
}

func (s *NumericContext) DECIMAL() antlr.TerminalNode {
	return s.GetToken(PainlessParserDECIMAL, 0)
}

func (s *NumericContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PainlessParserListener); ok {
		listenerT.EnterNumeric(s)
	}
}

func (s *NumericContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PainlessParserListener); ok {
		listenerT.ExitNumeric(s)
	}
}

func (s *NumericContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PainlessParserVisitor:
		return t.VisitNumeric(s)

	default:
		return t.VisitChildren(s)
	}
}

type NewobjectContext struct {
	PrimaryContext
}

func NewNewobjectContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *NewobjectContext {
	var p = new(NewobjectContext)

	InitEmptyPrimaryContext(&p.PrimaryContext)
	p.parser = parser
	p.CopyAll(ctx.(*PrimaryContext))

	return p
}

func (s *NewobjectContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *NewobjectContext) NEW() antlr.TerminalNode {
	return s.GetToken(PainlessParserNEW, 0)
}

func (s *NewobjectContext) Type_() ITypeContext {
	var t antlr.RuleContext
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(ITypeContext); ok {
			t = ctx.(antlr.RuleContext)
			break
		}
	}

	if t == nil {
		return nil
	}

	return t.(ITypeContext)
}

func (s *NewobjectContext) Arguments() IArgumentsContext {
	var t antlr.RuleContext
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IArgumentsContext); ok {
			t = ctx.(antlr.RuleContext)
			break
		}
	}

	if t == nil {
		return nil
	}

	return t.(IArgumentsContext)
}

func (s *NewobjectContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PainlessParserListener); ok {
		listenerT.EnterNewobject(s)
	}
}

func (s *NewobjectContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PainlessParserListener); ok {
		listenerT.ExitNewobject(s)
	}
}

func (s *NewobjectContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PainlessParserVisitor:
		return t.VisitNewobject(s)

	default:
		return t.VisitChildren(s)
	}
}

type PrecedenceContext struct {
	PrimaryContext
}

func NewPrecedenceContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *PrecedenceContext {
	var p = new(PrecedenceContext)

	InitEmptyPrimaryContext(&p.PrimaryContext)
	p.parser = parser
	p.CopyAll(ctx.(*PrimaryContext))

	return p
}

func (s *PrecedenceContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *PrecedenceContext) LP() antlr.TerminalNode {
	return s.GetToken(PainlessParserLP, 0)
}

func (s *PrecedenceContext) Expression() IExpressionContext {
	var t antlr.RuleContext
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IExpressionContext); ok {
			t = ctx.(antlr.RuleContext)
			break
		}
	}

	if t == nil {
		return nil
	}

	return t.(IExpressionContext)
}

func (s *PrecedenceContext) RP() antlr.TerminalNode {
	return s.GetToken(PainlessParserRP, 0)
}

func (s *PrecedenceContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PainlessParserListener); ok {
		listenerT.EnterPrecedence(s)
	}
}

func (s *PrecedenceContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PainlessParserListener); ok {
		listenerT.ExitPrecedence(s)
	}
}

func (s *PrecedenceContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PainlessParserVisitor:
		return t.VisitPrecedence(s)

	default:
		return t.VisitChildren(s)
	}
}

func (p *PainlessParser) Primary() (localctx IPrimaryContext) {
	localctx = NewPrimaryContext(p, p.GetParserRuleContext(), p.GetState())
	p.EnterRule(localctx, 48, PainlessParserRULE_primary)
	var _la int

	p.SetState(413)
	p.GetErrorHandler().Sync(p)
	if p.HasError() {
		goto errorExit
	}

	switch p.GetInterpreter().AdaptivePredict(p.BaseParser, p.GetTokenStream(), 36, p.GetParserRuleContext()) {
	case 1:
		localctx = NewPrecedenceContext(p, localctx)
		p.EnterOuterAlt(localctx, 1)
		{
			p.SetState(394)
			p.Match(PainlessParserLP)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(395)
			p.Expression()
		}
		{
			p.SetState(396)
			p.Match(PainlessParserRP)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}

	case 2:
		localctx = NewNumericContext(p, localctx)
		p.EnterOuterAlt(localctx, 2)
		{
			p.SetState(398)
			_la = p.GetTokenStream().LA(1)

			if !((int64((_la-72)) & ^0x3f) == 0 && ((int64(1)<<(_la-72))&15) != 0) {
				p.GetErrorHandler().RecoverInline(p)
			} else {
				p.GetErrorHandler().ReportMatch(p)
				p.Consume()
			}
		}

	case 3:
		localctx = NewTrueContext(p, localctx)
		p.EnterOuterAlt(localctx, 3)
		{
			p.SetState(399)
			p.Match(PainlessParserTRUE)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}

	case 4:
		localctx = NewFalseContext(p, localctx)
		p.EnterOuterAlt(localctx, 4)
		{
			p.SetState(400)
			p.Match(PainlessParserFALSE)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}

	case 5:
		localctx = NewNullContext(p, localctx)
		p.EnterOuterAlt(localctx, 5)
		{
			p.SetState(401)
			p.Match(PainlessParserNULL)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}

	case 6:
		localctx = NewStringContext(p, localctx)
		p.EnterOuterAlt(localctx, 6)
		{
			p.SetState(402)
			p.Match(PainlessParserSTRING)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}

	case 7:
		localctx = NewRegexContext(p, localctx)
		p.EnterOuterAlt(localctx, 7)
		{
			p.SetState(403)
			p.Match(PainlessParserREGEX)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}

	case 8:
		localctx = NewListinitContext(p, localctx)
		p.EnterOuterAlt(localctx, 8)
		{
			p.SetState(404)
			p.Listinitializer()
		}

	case 9:
		localctx = NewMapinitContext(p, localctx)
		p.EnterOuterAlt(localctx, 9)
		{
			p.SetState(405)
			p.Mapinitializer()
		}

	case 10:
		localctx = NewVariableContext(p, localctx)
		p.EnterOuterAlt(localctx, 10)
		{
			p.SetState(406)
			p.Match(PainlessParserID)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}

	case 11:
		localctx = NewCalllocalContext(p, localctx)
		p.EnterOuterAlt(localctx, 11)
		{
			p.SetState(407)
			p.Match(PainlessParserID)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(408)
			p.Arguments()
		}

	case 12:
		localctx = NewNewobjectContext(p, localctx)
		p.EnterOuterAlt(localctx, 12)
		{
			p.SetState(409)
			p.Match(PainlessParserNEW)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(410)
			p.Type_()
		}
		{
			p.SetState(411)
			p.Arguments()
		}

	case antlr.ATNInvalidAltNumber:
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
	return localctx
	goto errorExit // Trick to prevent compiler error if the label is not used
}

// IPostfixContext is an interface to support dynamic dispatch.
type IPostfixContext interface {
	antlr.ParserRuleContext

	// GetParser returns the parser.
	GetParser() antlr.Parser

	// Getter signatures
	Callinvoke() ICallinvokeContext
	Fieldaccess() IFieldaccessContext
	Braceaccess() IBraceaccessContext

	// IsPostfixContext differentiates from other interfaces.
	IsPostfixContext()
}

type PostfixContext struct {
	antlr.BaseParserRuleContext
	parser antlr.Parser
}

func NewEmptyPostfixContext() *PostfixContext {
	var p = new(PostfixContext)
	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, nil, -1)
	p.RuleIndex = PainlessParserRULE_postfix
	return p
}

func InitEmptyPostfixContext(p *PostfixContext) {
	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, nil, -1)
	p.RuleIndex = PainlessParserRULE_postfix
}

func (*PostfixContext) IsPostfixContext() {}

func NewPostfixContext(parser antlr.Parser, parent antlr.ParserRuleContext, invokingState int) *PostfixContext {
	var p = new(PostfixContext)

	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, parent, invokingState)

	p.parser = parser
	p.RuleIndex = PainlessParserRULE_postfix

	return p
}

func (s *PostfixContext) GetParser() antlr.Parser { return s.parser }

func (s *PostfixContext) Callinvoke() ICallinvokeContext {
	var t antlr.RuleContext
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(ICallinvokeContext); ok {
			t = ctx.(antlr.RuleContext)
			break
		}
	}

	if t == nil {
		return nil
	}

	return t.(ICallinvokeContext)
}

func (s *PostfixContext) Fieldaccess() IFieldaccessContext {
	var t antlr.RuleContext
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IFieldaccessContext); ok {
			t = ctx.(antlr.RuleContext)
			break
		}
	}

	if t == nil {
		return nil
	}

	return t.(IFieldaccessContext)
}

func (s *PostfixContext) Braceaccess() IBraceaccessContext {
	var t antlr.RuleContext
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IBraceaccessContext); ok {
			t = ctx.(antlr.RuleContext)
			break
		}
	}

	if t == nil {
		return nil
	}

	return t.(IBraceaccessContext)
}

func (s *PostfixContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *PostfixContext) ToStringTree(ruleNames []string, recog antlr.Recognizer) string {
	return antlr.TreesStringTree(s, ruleNames, recog)
}

func (s *PostfixContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PainlessParserListener); ok {
		listenerT.EnterPostfix(s)
	}
}

func (s *PostfixContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PainlessParserListener); ok {
		listenerT.ExitPostfix(s)
	}
}

func (s *PostfixContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PainlessParserVisitor:
		return t.VisitPostfix(s)

	default:
		return t.VisitChildren(s)
	}
}

func (p *PainlessParser) Postfix() (localctx IPostfixContext) {
	localctx = NewPostfixContext(p, p.GetParserRuleContext(), p.GetState())
	p.EnterRule(localctx, 50, PainlessParserRULE_postfix)
	p.SetState(418)
	p.GetErrorHandler().Sync(p)
	if p.HasError() {
		goto errorExit
	}

	switch p.GetInterpreter().AdaptivePredict(p.BaseParser, p.GetTokenStream(), 37, p.GetParserRuleContext()) {
	case 1:
		p.EnterOuterAlt(localctx, 1)
		{
			p.SetState(415)
			p.Callinvoke()
		}

	case 2:
		p.EnterOuterAlt(localctx, 2)
		{
			p.SetState(416)
			p.Fieldaccess()
		}

	case 3:
		p.EnterOuterAlt(localctx, 3)
		{
			p.SetState(417)
			p.Braceaccess()
		}

	case antlr.ATNInvalidAltNumber:
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
	return localctx
	goto errorExit // Trick to prevent compiler error if the label is not used
}

// IPostdotContext is an interface to support dynamic dispatch.
type IPostdotContext interface {
	antlr.ParserRuleContext

	// GetParser returns the parser.
	GetParser() antlr.Parser

	// Getter signatures
	Callinvoke() ICallinvokeContext
	Fieldaccess() IFieldaccessContext

	// IsPostdotContext differentiates from other interfaces.
	IsPostdotContext()
}

type PostdotContext struct {
	antlr.BaseParserRuleContext
	parser antlr.Parser
}

func NewEmptyPostdotContext() *PostdotContext {
	var p = new(PostdotContext)
	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, nil, -1)
	p.RuleIndex = PainlessParserRULE_postdot
	return p
}

func InitEmptyPostdotContext(p *PostdotContext) {
	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, nil, -1)
	p.RuleIndex = PainlessParserRULE_postdot
}

func (*PostdotContext) IsPostdotContext() {}

func NewPostdotContext(parser antlr.Parser, parent antlr.ParserRuleContext, invokingState int) *PostdotContext {
	var p = new(PostdotContext)

	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, parent, invokingState)

	p.parser = parser
	p.RuleIndex = PainlessParserRULE_postdot

	return p
}

func (s *PostdotContext) GetParser() antlr.Parser { return s.parser }

func (s *PostdotContext) Callinvoke() ICallinvokeContext {
	var t antlr.RuleContext
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(ICallinvokeContext); ok {
			t = ctx.(antlr.RuleContext)
			break
		}
	}

	if t == nil {
		return nil
	}

	return t.(ICallinvokeContext)
}

func (s *PostdotContext) Fieldaccess() IFieldaccessContext {
	var t antlr.RuleContext
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IFieldaccessContext); ok {
			t = ctx.(antlr.RuleContext)
			break
		}
	}

	if t == nil {
		return nil
	}

	return t.(IFieldaccessContext)
}

func (s *PostdotContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *PostdotContext) ToStringTree(ruleNames []string, recog antlr.Recognizer) string {
	return antlr.TreesStringTree(s, ruleNames, recog)
}

func (s *PostdotContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PainlessParserListener); ok {
		listenerT.EnterPostdot(s)
	}
}

func (s *PostdotContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PainlessParserListener); ok {
		listenerT.ExitPostdot(s)
	}
}

func (s *PostdotContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PainlessParserVisitor:
		return t.VisitPostdot(s)

	default:
		return t.VisitChildren(s)
	}
}

func (p *PainlessParser) Postdot() (localctx IPostdotContext) {
	localctx = NewPostdotContext(p, p.GetParserRuleContext(), p.GetState())
	p.EnterRule(localctx, 52, PainlessParserRULE_postdot)
	p.SetState(422)
	p.GetErrorHandler().Sync(p)
	if p.HasError() {
		goto errorExit
	}

	switch p.GetInterpreter().AdaptivePredict(p.BaseParser, p.GetTokenStream(), 38, p.GetParserRuleContext()) {
	case 1:
		p.EnterOuterAlt(localctx, 1)
		{
			p.SetState(420)
			p.Callinvoke()
		}

	case 2:
		p.EnterOuterAlt(localctx, 2)
		{
			p.SetState(421)
			p.Fieldaccess()
		}

	case antlr.ATNInvalidAltNumber:
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
	return localctx
	goto errorExit // Trick to prevent compiler error if the label is not used
}

// ICallinvokeContext is an interface to support dynamic dispatch.
type ICallinvokeContext interface {
	antlr.ParserRuleContext

	// GetParser returns the parser.
	GetParser() antlr.Parser

	// Getter signatures
	DOTID() antlr.TerminalNode
	Arguments() IArgumentsContext
	DOT() antlr.TerminalNode
	NSDOT() antlr.TerminalNode

	// IsCallinvokeContext differentiates from other interfaces.
	IsCallinvokeContext()
}

type CallinvokeContext struct {
	antlr.BaseParserRuleContext
	parser antlr.Parser
}

func NewEmptyCallinvokeContext() *CallinvokeContext {
	var p = new(CallinvokeContext)
	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, nil, -1)
	p.RuleIndex = PainlessParserRULE_callinvoke
	return p
}

func InitEmptyCallinvokeContext(p *CallinvokeContext) {
	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, nil, -1)
	p.RuleIndex = PainlessParserRULE_callinvoke
}

func (*CallinvokeContext) IsCallinvokeContext() {}

func NewCallinvokeContext(parser antlr.Parser, parent antlr.ParserRuleContext, invokingState int) *CallinvokeContext {
	var p = new(CallinvokeContext)

	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, parent, invokingState)

	p.parser = parser
	p.RuleIndex = PainlessParserRULE_callinvoke

	return p
}

func (s *CallinvokeContext) GetParser() antlr.Parser { return s.parser }

func (s *CallinvokeContext) DOTID() antlr.TerminalNode {
	return s.GetToken(PainlessParserDOTID, 0)
}

func (s *CallinvokeContext) Arguments() IArgumentsContext {
	var t antlr.RuleContext
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IArgumentsContext); ok {
			t = ctx.(antlr.RuleContext)
			break
		}
	}

	if t == nil {
		return nil
	}

	return t.(IArgumentsContext)
}

func (s *CallinvokeContext) DOT() antlr.TerminalNode {
	return s.GetToken(PainlessParserDOT, 0)
}

func (s *CallinvokeContext) NSDOT() antlr.TerminalNode {
	return s.GetToken(PainlessParserNSDOT, 0)
}

func (s *CallinvokeContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *CallinvokeContext) ToStringTree(ruleNames []string, recog antlr.Recognizer) string {
	return antlr.TreesStringTree(s, ruleNames, recog)
}

func (s *CallinvokeContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PainlessParserListener); ok {
		listenerT.EnterCallinvoke(s)
	}
}

func (s *CallinvokeContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PainlessParserListener); ok {
		listenerT.ExitCallinvoke(s)
	}
}

func (s *CallinvokeContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PainlessParserVisitor:
		return t.VisitCallinvoke(s)

	default:
		return t.VisitChildren(s)
	}
}

func (p *PainlessParser) Callinvoke() (localctx ICallinvokeContext) {
	localctx = NewCallinvokeContext(p, p.GetParserRuleContext(), p.GetState())
	p.EnterRule(localctx, 54, PainlessParserRULE_callinvoke)
	var _la int

	p.EnterOuterAlt(localctx, 1)
	{
		p.SetState(424)
		_la = p.GetTokenStream().LA(1)

		if !(_la == PainlessParserDOT || _la == PainlessParserNSDOT) {
			p.GetErrorHandler().RecoverInline(p)
		} else {
			p.GetErrorHandler().ReportMatch(p)
			p.Consume()
		}
	}
	{
		p.SetState(425)
		p.Match(PainlessParserDOTID)
		if p.HasError() {
			// Recognition error - abort rule
			goto errorExit
		}
	}
	{
		p.SetState(426)
		p.Arguments()
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
	return localctx
	goto errorExit // Trick to prevent compiler error if the label is not used
}

// IFieldaccessContext is an interface to support dynamic dispatch.
type IFieldaccessContext interface {
	antlr.ParserRuleContext

	// GetParser returns the parser.
	GetParser() antlr.Parser

	// Getter signatures
	DOT() antlr.TerminalNode
	NSDOT() antlr.TerminalNode
	DOTID() antlr.TerminalNode
	DOTINTEGER() antlr.TerminalNode

	// IsFieldaccessContext differentiates from other interfaces.
	IsFieldaccessContext()
}

type FieldaccessContext struct {
	antlr.BaseParserRuleContext
	parser antlr.Parser
}

func NewEmptyFieldaccessContext() *FieldaccessContext {
	var p = new(FieldaccessContext)
	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, nil, -1)
	p.RuleIndex = PainlessParserRULE_fieldaccess
	return p
}

func InitEmptyFieldaccessContext(p *FieldaccessContext) {
	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, nil, -1)
	p.RuleIndex = PainlessParserRULE_fieldaccess
}

func (*FieldaccessContext) IsFieldaccessContext() {}

func NewFieldaccessContext(parser antlr.Parser, parent antlr.ParserRuleContext, invokingState int) *FieldaccessContext {
	var p = new(FieldaccessContext)

	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, parent, invokingState)

	p.parser = parser
	p.RuleIndex = PainlessParserRULE_fieldaccess

	return p
}

func (s *FieldaccessContext) GetParser() antlr.Parser { return s.parser }

func (s *FieldaccessContext) DOT() antlr.TerminalNode {
	return s.GetToken(PainlessParserDOT, 0)
}

func (s *FieldaccessContext) NSDOT() antlr.TerminalNode {
	return s.GetToken(PainlessParserNSDOT, 0)
}

func (s *FieldaccessContext) DOTID() antlr.TerminalNode {
	return s.GetToken(PainlessParserDOTID, 0)
}

func (s *FieldaccessContext) DOTINTEGER() antlr.TerminalNode {
	return s.GetToken(PainlessParserDOTINTEGER, 0)
}

func (s *FieldaccessContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *FieldaccessContext) ToStringTree(ruleNames []string, recog antlr.Recognizer) string {
	return antlr.TreesStringTree(s, ruleNames, recog)
}

func (s *FieldaccessContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PainlessParserListener); ok {
		listenerT.EnterFieldaccess(s)
	}
}

func (s *FieldaccessContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PainlessParserListener); ok {
		listenerT.ExitFieldaccess(s)
	}
}

func (s *FieldaccessContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PainlessParserVisitor:
		return t.VisitFieldaccess(s)

	default:
		return t.VisitChildren(s)
	}
}

func (p *PainlessParser) Fieldaccess() (localctx IFieldaccessContext) {
	localctx = NewFieldaccessContext(p, p.GetParserRuleContext(), p.GetState())
	p.EnterRule(localctx, 56, PainlessParserRULE_fieldaccess)
	var _la int

	p.EnterOuterAlt(localctx, 1)
	{
		p.SetState(428)
		_la = p.GetTokenStream().LA(1)

		if !(_la == PainlessParserDOT || _la == PainlessParserNSDOT) {
			p.GetErrorHandler().RecoverInline(p)
		} else {
			p.GetErrorHandler().ReportMatch(p)
			p.Consume()
		}
	}
	{
		p.SetState(429)
		_la = p.GetTokenStream().LA(1)

		if !(_la == PainlessParserDOTINTEGER || _la == PainlessParserDOTID) {
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
	return localctx
	goto errorExit // Trick to prevent compiler error if the label is not used
}

// IBraceaccessContext is an interface to support dynamic dispatch.
type IBraceaccessContext interface {
	antlr.ParserRuleContext

	// GetParser returns the parser.
	GetParser() antlr.Parser

	// Getter signatures
	LBRACE() antlr.TerminalNode
	Expression() IExpressionContext
	RBRACE() antlr.TerminalNode

	// IsBraceaccessContext differentiates from other interfaces.
	IsBraceaccessContext()
}

type BraceaccessContext struct {
	antlr.BaseParserRuleContext
	parser antlr.Parser
}

func NewEmptyBraceaccessContext() *BraceaccessContext {
	var p = new(BraceaccessContext)
	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, nil, -1)
	p.RuleIndex = PainlessParserRULE_braceaccess
	return p
}

func InitEmptyBraceaccessContext(p *BraceaccessContext) {
	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, nil, -1)
	p.RuleIndex = PainlessParserRULE_braceaccess
}

func (*BraceaccessContext) IsBraceaccessContext() {}

func NewBraceaccessContext(parser antlr.Parser, parent antlr.ParserRuleContext, invokingState int) *BraceaccessContext {
	var p = new(BraceaccessContext)

	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, parent, invokingState)

	p.parser = parser
	p.RuleIndex = PainlessParserRULE_braceaccess

	return p
}

func (s *BraceaccessContext) GetParser() antlr.Parser { return s.parser }

func (s *BraceaccessContext) LBRACE() antlr.TerminalNode {
	return s.GetToken(PainlessParserLBRACE, 0)
}

func (s *BraceaccessContext) Expression() IExpressionContext {
	var t antlr.RuleContext
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IExpressionContext); ok {
			t = ctx.(antlr.RuleContext)
			break
		}
	}

	if t == nil {
		return nil
	}

	return t.(IExpressionContext)
}

func (s *BraceaccessContext) RBRACE() antlr.TerminalNode {
	return s.GetToken(PainlessParserRBRACE, 0)
}

func (s *BraceaccessContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *BraceaccessContext) ToStringTree(ruleNames []string, recog antlr.Recognizer) string {
	return antlr.TreesStringTree(s, ruleNames, recog)
}

func (s *BraceaccessContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PainlessParserListener); ok {
		listenerT.EnterBraceaccess(s)
	}
}

func (s *BraceaccessContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PainlessParserListener); ok {
		listenerT.ExitBraceaccess(s)
	}
}

func (s *BraceaccessContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PainlessParserVisitor:
		return t.VisitBraceaccess(s)

	default:
		return t.VisitChildren(s)
	}
}

func (p *PainlessParser) Braceaccess() (localctx IBraceaccessContext) {
	localctx = NewBraceaccessContext(p, p.GetParserRuleContext(), p.GetState())
	p.EnterRule(localctx, 58, PainlessParserRULE_braceaccess)
	p.EnterOuterAlt(localctx, 1)
	{
		p.SetState(431)
		p.Match(PainlessParserLBRACE)
		if p.HasError() {
			// Recognition error - abort rule
			goto errorExit
		}
	}
	{
		p.SetState(432)
		p.Expression()
	}
	{
		p.SetState(433)
		p.Match(PainlessParserRBRACE)
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
	return localctx
	goto errorExit // Trick to prevent compiler error if the label is not used
}

// IArrayinitializerContext is an interface to support dynamic dispatch.
type IArrayinitializerContext interface {
	antlr.ParserRuleContext

	// GetParser returns the parser.
	GetParser() antlr.Parser
	// IsArrayinitializerContext differentiates from other interfaces.
	IsArrayinitializerContext()
}

type ArrayinitializerContext struct {
	antlr.BaseParserRuleContext
	parser antlr.Parser
}

func NewEmptyArrayinitializerContext() *ArrayinitializerContext {
	var p = new(ArrayinitializerContext)
	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, nil, -1)
	p.RuleIndex = PainlessParserRULE_arrayinitializer
	return p
}

func InitEmptyArrayinitializerContext(p *ArrayinitializerContext) {
	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, nil, -1)
	p.RuleIndex = PainlessParserRULE_arrayinitializer
}

func (*ArrayinitializerContext) IsArrayinitializerContext() {}

func NewArrayinitializerContext(parser antlr.Parser, parent antlr.ParserRuleContext, invokingState int) *ArrayinitializerContext {
	var p = new(ArrayinitializerContext)

	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, parent, invokingState)

	p.parser = parser
	p.RuleIndex = PainlessParserRULE_arrayinitializer

	return p
}

func (s *ArrayinitializerContext) GetParser() antlr.Parser { return s.parser }

func (s *ArrayinitializerContext) CopyAll(ctx *ArrayinitializerContext) {
	s.CopyFrom(&ctx.BaseParserRuleContext)
}

func (s *ArrayinitializerContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *ArrayinitializerContext) ToStringTree(ruleNames []string, recog antlr.Recognizer) string {
	return antlr.TreesStringTree(s, ruleNames, recog)
}

type NewstandardarrayContext struct {
	ArrayinitializerContext
}

func NewNewstandardarrayContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *NewstandardarrayContext {
	var p = new(NewstandardarrayContext)

	InitEmptyArrayinitializerContext(&p.ArrayinitializerContext)
	p.parser = parser
	p.CopyAll(ctx.(*ArrayinitializerContext))

	return p
}

func (s *NewstandardarrayContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *NewstandardarrayContext) NEW() antlr.TerminalNode {
	return s.GetToken(PainlessParserNEW, 0)
}

func (s *NewstandardarrayContext) Type_() ITypeContext {
	var t antlr.RuleContext
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(ITypeContext); ok {
			t = ctx.(antlr.RuleContext)
			break
		}
	}

	if t == nil {
		return nil
	}

	return t.(ITypeContext)
}

func (s *NewstandardarrayContext) AllLBRACE() []antlr.TerminalNode {
	return s.GetTokens(PainlessParserLBRACE)
}

func (s *NewstandardarrayContext) LBRACE(i int) antlr.TerminalNode {
	return s.GetToken(PainlessParserLBRACE, i)
}

func (s *NewstandardarrayContext) AllExpression() []IExpressionContext {
	children := s.GetChildren()
	len := 0
	for _, ctx := range children {
		if _, ok := ctx.(IExpressionContext); ok {
			len++
		}
	}

	tst := make([]IExpressionContext, len)
	i := 0
	for _, ctx := range children {
		if t, ok := ctx.(IExpressionContext); ok {
			tst[i] = t.(IExpressionContext)
			i++
		}
	}

	return tst
}

func (s *NewstandardarrayContext) Expression(i int) IExpressionContext {
	var t antlr.RuleContext
	j := 0
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IExpressionContext); ok {
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

	return t.(IExpressionContext)
}

func (s *NewstandardarrayContext) AllRBRACE() []antlr.TerminalNode {
	return s.GetTokens(PainlessParserRBRACE)
}

func (s *NewstandardarrayContext) RBRACE(i int) antlr.TerminalNode {
	return s.GetToken(PainlessParserRBRACE, i)
}

func (s *NewstandardarrayContext) Postdot() IPostdotContext {
	var t antlr.RuleContext
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IPostdotContext); ok {
			t = ctx.(antlr.RuleContext)
			break
		}
	}

	if t == nil {
		return nil
	}

	return t.(IPostdotContext)
}

func (s *NewstandardarrayContext) AllPostfix() []IPostfixContext {
	children := s.GetChildren()
	len := 0
	for _, ctx := range children {
		if _, ok := ctx.(IPostfixContext); ok {
			len++
		}
	}

	tst := make([]IPostfixContext, len)
	i := 0
	for _, ctx := range children {
		if t, ok := ctx.(IPostfixContext); ok {
			tst[i] = t.(IPostfixContext)
			i++
		}
	}

	return tst
}

func (s *NewstandardarrayContext) Postfix(i int) IPostfixContext {
	var t antlr.RuleContext
	j := 0
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IPostfixContext); ok {
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

	return t.(IPostfixContext)
}

func (s *NewstandardarrayContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PainlessParserListener); ok {
		listenerT.EnterNewstandardarray(s)
	}
}

func (s *NewstandardarrayContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PainlessParserListener); ok {
		listenerT.ExitNewstandardarray(s)
	}
}

func (s *NewstandardarrayContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PainlessParserVisitor:
		return t.VisitNewstandardarray(s)

	default:
		return t.VisitChildren(s)
	}
}

type NewinitializedarrayContext struct {
	ArrayinitializerContext
}

func NewNewinitializedarrayContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *NewinitializedarrayContext {
	var p = new(NewinitializedarrayContext)

	InitEmptyArrayinitializerContext(&p.ArrayinitializerContext)
	p.parser = parser
	p.CopyAll(ctx.(*ArrayinitializerContext))

	return p
}

func (s *NewinitializedarrayContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *NewinitializedarrayContext) NEW() antlr.TerminalNode {
	return s.GetToken(PainlessParserNEW, 0)
}

func (s *NewinitializedarrayContext) Type_() ITypeContext {
	var t antlr.RuleContext
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(ITypeContext); ok {
			t = ctx.(antlr.RuleContext)
			break
		}
	}

	if t == nil {
		return nil
	}

	return t.(ITypeContext)
}

func (s *NewinitializedarrayContext) LBRACE() antlr.TerminalNode {
	return s.GetToken(PainlessParserLBRACE, 0)
}

func (s *NewinitializedarrayContext) RBRACE() antlr.TerminalNode {
	return s.GetToken(PainlessParserRBRACE, 0)
}

func (s *NewinitializedarrayContext) LBRACK() antlr.TerminalNode {
	return s.GetToken(PainlessParserLBRACK, 0)
}

func (s *NewinitializedarrayContext) RBRACK() antlr.TerminalNode {
	return s.GetToken(PainlessParserRBRACK, 0)
}

func (s *NewinitializedarrayContext) AllExpression() []IExpressionContext {
	children := s.GetChildren()
	len := 0
	for _, ctx := range children {
		if _, ok := ctx.(IExpressionContext); ok {
			len++
		}
	}

	tst := make([]IExpressionContext, len)
	i := 0
	for _, ctx := range children {
		if t, ok := ctx.(IExpressionContext); ok {
			tst[i] = t.(IExpressionContext)
			i++
		}
	}

	return tst
}

func (s *NewinitializedarrayContext) Expression(i int) IExpressionContext {
	var t antlr.RuleContext
	j := 0
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IExpressionContext); ok {
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

	return t.(IExpressionContext)
}

func (s *NewinitializedarrayContext) AllPostfix() []IPostfixContext {
	children := s.GetChildren()
	len := 0
	for _, ctx := range children {
		if _, ok := ctx.(IPostfixContext); ok {
			len++
		}
	}

	tst := make([]IPostfixContext, len)
	i := 0
	for _, ctx := range children {
		if t, ok := ctx.(IPostfixContext); ok {
			tst[i] = t.(IPostfixContext)
			i++
		}
	}

	return tst
}

func (s *NewinitializedarrayContext) Postfix(i int) IPostfixContext {
	var t antlr.RuleContext
	j := 0
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IPostfixContext); ok {
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

	return t.(IPostfixContext)
}

func (s *NewinitializedarrayContext) AllCOMMA() []antlr.TerminalNode {
	return s.GetTokens(PainlessParserCOMMA)
}

func (s *NewinitializedarrayContext) COMMA(i int) antlr.TerminalNode {
	return s.GetToken(PainlessParserCOMMA, i)
}

func (s *NewinitializedarrayContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PainlessParserListener); ok {
		listenerT.EnterNewinitializedarray(s)
	}
}

func (s *NewinitializedarrayContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PainlessParserListener); ok {
		listenerT.ExitNewinitializedarray(s)
	}
}

func (s *NewinitializedarrayContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PainlessParserVisitor:
		return t.VisitNewinitializedarray(s)

	default:
		return t.VisitChildren(s)
	}
}

func (p *PainlessParser) Arrayinitializer() (localctx IArrayinitializerContext) {
	localctx = NewArrayinitializerContext(p, p.GetParserRuleContext(), p.GetState())
	p.EnterRule(localctx, 60, PainlessParserRULE_arrayinitializer)
	var _la int

	var _alt int

	p.SetState(476)
	p.GetErrorHandler().Sync(p)
	if p.HasError() {
		goto errorExit
	}

	switch p.GetInterpreter().AdaptivePredict(p.BaseParser, p.GetTokenStream(), 45, p.GetParserRuleContext()) {
	case 1:
		localctx = NewNewstandardarrayContext(p, localctx)
		p.EnterOuterAlt(localctx, 1)
		{
			p.SetState(435)
			p.Match(PainlessParserNEW)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(436)
			p.Type_()
		}
		p.SetState(441)
		p.GetErrorHandler().Sync(p)
		if p.HasError() {
			goto errorExit
		}
		_alt = 1
		for ok := true; ok; ok = _alt != 2 && _alt != antlr.ATNInvalidAltNumber {
			switch _alt {
			case 1:
				{
					p.SetState(437)
					p.Match(PainlessParserLBRACE)
					if p.HasError() {
						// Recognition error - abort rule
						goto errorExit
					}
				}
				{
					p.SetState(438)
					p.Expression()
				}
				{
					p.SetState(439)
					p.Match(PainlessParserRBRACE)
					if p.HasError() {
						// Recognition error - abort rule
						goto errorExit
					}
				}

			default:
				p.SetError(antlr.NewNoViableAltException(p, nil, nil, nil, nil, nil))
				goto errorExit
			}

			p.SetState(443)
			p.GetErrorHandler().Sync(p)
			_alt = p.GetInterpreter().AdaptivePredict(p.BaseParser, p.GetTokenStream(), 39, p.GetParserRuleContext())
			if p.HasError() {
				goto errorExit
			}
		}
		p.SetState(452)
		p.GetErrorHandler().Sync(p)

		if p.GetInterpreter().AdaptivePredict(p.BaseParser, p.GetTokenStream(), 41, p.GetParserRuleContext()) == 1 {
			{
				p.SetState(445)
				p.Postdot()
			}
			p.SetState(449)
			p.GetErrorHandler().Sync(p)
			if p.HasError() {
				goto errorExit
			}
			_alt = p.GetInterpreter().AdaptivePredict(p.BaseParser, p.GetTokenStream(), 40, p.GetParserRuleContext())
			if p.HasError() {
				goto errorExit
			}
			for _alt != 2 && _alt != antlr.ATNInvalidAltNumber {
				if _alt == 1 {
					{
						p.SetState(446)
						p.Postfix()
					}

				}
				p.SetState(451)
				p.GetErrorHandler().Sync(p)
				if p.HasError() {
					goto errorExit
				}
				_alt = p.GetInterpreter().AdaptivePredict(p.BaseParser, p.GetTokenStream(), 40, p.GetParserRuleContext())
				if p.HasError() {
					goto errorExit
				}
			}

		} else if p.HasError() { // JIM
			goto errorExit
		}

	case 2:
		localctx = NewNewinitializedarrayContext(p, localctx)
		p.EnterOuterAlt(localctx, 2)
		{
			p.SetState(454)
			p.Match(PainlessParserNEW)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(455)
			p.Type_()
		}
		{
			p.SetState(456)
			p.Match(PainlessParserLBRACE)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(457)
			p.Match(PainlessParserRBRACE)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(458)
			p.Match(PainlessParserLBRACK)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		p.SetState(467)
		p.GetErrorHandler().Sync(p)
		if p.HasError() {
			goto errorExit
		}
		_la = p.GetTokenStream().LA(1)

		if ((int64(_la) & ^0x3f) == 0 && ((int64(1)<<_la)&864691155034439840) != 0) || ((int64((_la-72)) & ^0x3f) == 0 && ((int64(1)<<(_la-72))&2559) != 0) {
			{
				p.SetState(459)
				p.Expression()
			}
			p.SetState(464)
			p.GetErrorHandler().Sync(p)
			if p.HasError() {
				goto errorExit
			}
			_la = p.GetTokenStream().LA(1)

			for _la == PainlessParserCOMMA {
				{
					p.SetState(460)
					p.Match(PainlessParserCOMMA)
					if p.HasError() {
						// Recognition error - abort rule
						goto errorExit
					}
				}
				{
					p.SetState(461)
					p.Expression()
				}

				p.SetState(466)
				p.GetErrorHandler().Sync(p)
				if p.HasError() {
					goto errorExit
				}
				_la = p.GetTokenStream().LA(1)
			}

		}
		{
			p.SetState(469)
			p.Match(PainlessParserRBRACK)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		p.SetState(473)
		p.GetErrorHandler().Sync(p)
		if p.HasError() {
			goto errorExit
		}
		_alt = p.GetInterpreter().AdaptivePredict(p.BaseParser, p.GetTokenStream(), 44, p.GetParserRuleContext())
		if p.HasError() {
			goto errorExit
		}
		for _alt != 2 && _alt != antlr.ATNInvalidAltNumber {
			if _alt == 1 {
				{
					p.SetState(470)
					p.Postfix()
				}

			}
			p.SetState(475)
			p.GetErrorHandler().Sync(p)
			if p.HasError() {
				goto errorExit
			}
			_alt = p.GetInterpreter().AdaptivePredict(p.BaseParser, p.GetTokenStream(), 44, p.GetParserRuleContext())
			if p.HasError() {
				goto errorExit
			}
		}

	case antlr.ATNInvalidAltNumber:
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
	return localctx
	goto errorExit // Trick to prevent compiler error if the label is not used
}

// IListinitializerContext is an interface to support dynamic dispatch.
type IListinitializerContext interface {
	antlr.ParserRuleContext

	// GetParser returns the parser.
	GetParser() antlr.Parser

	// Getter signatures
	LBRACE() antlr.TerminalNode
	AllExpression() []IExpressionContext
	Expression(i int) IExpressionContext
	RBRACE() antlr.TerminalNode
	AllCOMMA() []antlr.TerminalNode
	COMMA(i int) antlr.TerminalNode

	// IsListinitializerContext differentiates from other interfaces.
	IsListinitializerContext()
}

type ListinitializerContext struct {
	antlr.BaseParserRuleContext
	parser antlr.Parser
}

func NewEmptyListinitializerContext() *ListinitializerContext {
	var p = new(ListinitializerContext)
	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, nil, -1)
	p.RuleIndex = PainlessParserRULE_listinitializer
	return p
}

func InitEmptyListinitializerContext(p *ListinitializerContext) {
	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, nil, -1)
	p.RuleIndex = PainlessParserRULE_listinitializer
}

func (*ListinitializerContext) IsListinitializerContext() {}

func NewListinitializerContext(parser antlr.Parser, parent antlr.ParserRuleContext, invokingState int) *ListinitializerContext {
	var p = new(ListinitializerContext)

	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, parent, invokingState)

	p.parser = parser
	p.RuleIndex = PainlessParserRULE_listinitializer

	return p
}

func (s *ListinitializerContext) GetParser() antlr.Parser { return s.parser }

func (s *ListinitializerContext) LBRACE() antlr.TerminalNode {
	return s.GetToken(PainlessParserLBRACE, 0)
}

func (s *ListinitializerContext) AllExpression() []IExpressionContext {
	children := s.GetChildren()
	len := 0
	for _, ctx := range children {
		if _, ok := ctx.(IExpressionContext); ok {
			len++
		}
	}

	tst := make([]IExpressionContext, len)
	i := 0
	for _, ctx := range children {
		if t, ok := ctx.(IExpressionContext); ok {
			tst[i] = t.(IExpressionContext)
			i++
		}
	}

	return tst
}

func (s *ListinitializerContext) Expression(i int) IExpressionContext {
	var t antlr.RuleContext
	j := 0
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IExpressionContext); ok {
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

	return t.(IExpressionContext)
}

func (s *ListinitializerContext) RBRACE() antlr.TerminalNode {
	return s.GetToken(PainlessParserRBRACE, 0)
}

func (s *ListinitializerContext) AllCOMMA() []antlr.TerminalNode {
	return s.GetTokens(PainlessParserCOMMA)
}

func (s *ListinitializerContext) COMMA(i int) antlr.TerminalNode {
	return s.GetToken(PainlessParserCOMMA, i)
}

func (s *ListinitializerContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *ListinitializerContext) ToStringTree(ruleNames []string, recog antlr.Recognizer) string {
	return antlr.TreesStringTree(s, ruleNames, recog)
}

func (s *ListinitializerContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PainlessParserListener); ok {
		listenerT.EnterListinitializer(s)
	}
}

func (s *ListinitializerContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PainlessParserListener); ok {
		listenerT.ExitListinitializer(s)
	}
}

func (s *ListinitializerContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PainlessParserVisitor:
		return t.VisitListinitializer(s)

	default:
		return t.VisitChildren(s)
	}
}

func (p *PainlessParser) Listinitializer() (localctx IListinitializerContext) {
	localctx = NewListinitializerContext(p, p.GetParserRuleContext(), p.GetState())
	p.EnterRule(localctx, 62, PainlessParserRULE_listinitializer)
	var _la int

	p.SetState(491)
	p.GetErrorHandler().Sync(p)
	if p.HasError() {
		goto errorExit
	}

	switch p.GetInterpreter().AdaptivePredict(p.BaseParser, p.GetTokenStream(), 47, p.GetParserRuleContext()) {
	case 1:
		p.EnterOuterAlt(localctx, 1)
		{
			p.SetState(478)
			p.Match(PainlessParserLBRACE)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(479)
			p.Expression()
		}
		p.SetState(484)
		p.GetErrorHandler().Sync(p)
		if p.HasError() {
			goto errorExit
		}
		_la = p.GetTokenStream().LA(1)

		for _la == PainlessParserCOMMA {
			{
				p.SetState(480)
				p.Match(PainlessParserCOMMA)
				if p.HasError() {
					// Recognition error - abort rule
					goto errorExit
				}
			}
			{
				p.SetState(481)
				p.Expression()
			}

			p.SetState(486)
			p.GetErrorHandler().Sync(p)
			if p.HasError() {
				goto errorExit
			}
			_la = p.GetTokenStream().LA(1)
		}
		{
			p.SetState(487)
			p.Match(PainlessParserRBRACE)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}

	case 2:
		p.EnterOuterAlt(localctx, 2)
		{
			p.SetState(489)
			p.Match(PainlessParserLBRACE)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(490)
			p.Match(PainlessParserRBRACE)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}

	case antlr.ATNInvalidAltNumber:
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
	return localctx
	goto errorExit // Trick to prevent compiler error if the label is not used
}

// IMapinitializerContext is an interface to support dynamic dispatch.
type IMapinitializerContext interface {
	antlr.ParserRuleContext

	// GetParser returns the parser.
	GetParser() antlr.Parser

	// Getter signatures
	LBRACE() antlr.TerminalNode
	AllMaptoken() []IMaptokenContext
	Maptoken(i int) IMaptokenContext
	RBRACE() antlr.TerminalNode
	AllCOMMA() []antlr.TerminalNode
	COMMA(i int) antlr.TerminalNode
	COLON() antlr.TerminalNode

	// IsMapinitializerContext differentiates from other interfaces.
	IsMapinitializerContext()
}

type MapinitializerContext struct {
	antlr.BaseParserRuleContext
	parser antlr.Parser
}

func NewEmptyMapinitializerContext() *MapinitializerContext {
	var p = new(MapinitializerContext)
	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, nil, -1)
	p.RuleIndex = PainlessParserRULE_mapinitializer
	return p
}

func InitEmptyMapinitializerContext(p *MapinitializerContext) {
	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, nil, -1)
	p.RuleIndex = PainlessParserRULE_mapinitializer
}

func (*MapinitializerContext) IsMapinitializerContext() {}

func NewMapinitializerContext(parser antlr.Parser, parent antlr.ParserRuleContext, invokingState int) *MapinitializerContext {
	var p = new(MapinitializerContext)

	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, parent, invokingState)

	p.parser = parser
	p.RuleIndex = PainlessParserRULE_mapinitializer

	return p
}

func (s *MapinitializerContext) GetParser() antlr.Parser { return s.parser }

func (s *MapinitializerContext) LBRACE() antlr.TerminalNode {
	return s.GetToken(PainlessParserLBRACE, 0)
}

func (s *MapinitializerContext) AllMaptoken() []IMaptokenContext {
	children := s.GetChildren()
	len := 0
	for _, ctx := range children {
		if _, ok := ctx.(IMaptokenContext); ok {
			len++
		}
	}

	tst := make([]IMaptokenContext, len)
	i := 0
	for _, ctx := range children {
		if t, ok := ctx.(IMaptokenContext); ok {
			tst[i] = t.(IMaptokenContext)
			i++
		}
	}

	return tst
}

func (s *MapinitializerContext) Maptoken(i int) IMaptokenContext {
	var t antlr.RuleContext
	j := 0
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IMaptokenContext); ok {
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

	return t.(IMaptokenContext)
}

func (s *MapinitializerContext) RBRACE() antlr.TerminalNode {
	return s.GetToken(PainlessParserRBRACE, 0)
}

func (s *MapinitializerContext) AllCOMMA() []antlr.TerminalNode {
	return s.GetTokens(PainlessParserCOMMA)
}

func (s *MapinitializerContext) COMMA(i int) antlr.TerminalNode {
	return s.GetToken(PainlessParserCOMMA, i)
}

func (s *MapinitializerContext) COLON() antlr.TerminalNode {
	return s.GetToken(PainlessParserCOLON, 0)
}

func (s *MapinitializerContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *MapinitializerContext) ToStringTree(ruleNames []string, recog antlr.Recognizer) string {
	return antlr.TreesStringTree(s, ruleNames, recog)
}

func (s *MapinitializerContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PainlessParserListener); ok {
		listenerT.EnterMapinitializer(s)
	}
}

func (s *MapinitializerContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PainlessParserListener); ok {
		listenerT.ExitMapinitializer(s)
	}
}

func (s *MapinitializerContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PainlessParserVisitor:
		return t.VisitMapinitializer(s)

	default:
		return t.VisitChildren(s)
	}
}

func (p *PainlessParser) Mapinitializer() (localctx IMapinitializerContext) {
	localctx = NewMapinitializerContext(p, p.GetParserRuleContext(), p.GetState())
	p.EnterRule(localctx, 64, PainlessParserRULE_mapinitializer)
	var _la int

	p.SetState(507)
	p.GetErrorHandler().Sync(p)
	if p.HasError() {
		goto errorExit
	}

	switch p.GetInterpreter().AdaptivePredict(p.BaseParser, p.GetTokenStream(), 49, p.GetParserRuleContext()) {
	case 1:
		p.EnterOuterAlt(localctx, 1)
		{
			p.SetState(493)
			p.Match(PainlessParserLBRACE)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(494)
			p.Maptoken()
		}
		p.SetState(499)
		p.GetErrorHandler().Sync(p)
		if p.HasError() {
			goto errorExit
		}
		_la = p.GetTokenStream().LA(1)

		for _la == PainlessParserCOMMA {
			{
				p.SetState(495)
				p.Match(PainlessParserCOMMA)
				if p.HasError() {
					// Recognition error - abort rule
					goto errorExit
				}
			}
			{
				p.SetState(496)
				p.Maptoken()
			}

			p.SetState(501)
			p.GetErrorHandler().Sync(p)
			if p.HasError() {
				goto errorExit
			}
			_la = p.GetTokenStream().LA(1)
		}
		{
			p.SetState(502)
			p.Match(PainlessParserRBRACE)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}

	case 2:
		p.EnterOuterAlt(localctx, 2)
		{
			p.SetState(504)
			p.Match(PainlessParserLBRACE)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(505)
			p.Match(PainlessParserCOLON)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(506)
			p.Match(PainlessParserRBRACE)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}

	case antlr.ATNInvalidAltNumber:
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
	return localctx
	goto errorExit // Trick to prevent compiler error if the label is not used
}

// IMaptokenContext is an interface to support dynamic dispatch.
type IMaptokenContext interface {
	antlr.ParserRuleContext

	// GetParser returns the parser.
	GetParser() antlr.Parser

	// Getter signatures
	AllExpression() []IExpressionContext
	Expression(i int) IExpressionContext
	COLON() antlr.TerminalNode

	// IsMaptokenContext differentiates from other interfaces.
	IsMaptokenContext()
}

type MaptokenContext struct {
	antlr.BaseParserRuleContext
	parser antlr.Parser
}

func NewEmptyMaptokenContext() *MaptokenContext {
	var p = new(MaptokenContext)
	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, nil, -1)
	p.RuleIndex = PainlessParserRULE_maptoken
	return p
}

func InitEmptyMaptokenContext(p *MaptokenContext) {
	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, nil, -1)
	p.RuleIndex = PainlessParserRULE_maptoken
}

func (*MaptokenContext) IsMaptokenContext() {}

func NewMaptokenContext(parser antlr.Parser, parent antlr.ParserRuleContext, invokingState int) *MaptokenContext {
	var p = new(MaptokenContext)

	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, parent, invokingState)

	p.parser = parser
	p.RuleIndex = PainlessParserRULE_maptoken

	return p
}

func (s *MaptokenContext) GetParser() antlr.Parser { return s.parser }

func (s *MaptokenContext) AllExpression() []IExpressionContext {
	children := s.GetChildren()
	len := 0
	for _, ctx := range children {
		if _, ok := ctx.(IExpressionContext); ok {
			len++
		}
	}

	tst := make([]IExpressionContext, len)
	i := 0
	for _, ctx := range children {
		if t, ok := ctx.(IExpressionContext); ok {
			tst[i] = t.(IExpressionContext)
			i++
		}
	}

	return tst
}

func (s *MaptokenContext) Expression(i int) IExpressionContext {
	var t antlr.RuleContext
	j := 0
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IExpressionContext); ok {
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

	return t.(IExpressionContext)
}

func (s *MaptokenContext) COLON() antlr.TerminalNode {
	return s.GetToken(PainlessParserCOLON, 0)
}

func (s *MaptokenContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *MaptokenContext) ToStringTree(ruleNames []string, recog antlr.Recognizer) string {
	return antlr.TreesStringTree(s, ruleNames, recog)
}

func (s *MaptokenContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PainlessParserListener); ok {
		listenerT.EnterMaptoken(s)
	}
}

func (s *MaptokenContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PainlessParserListener); ok {
		listenerT.ExitMaptoken(s)
	}
}

func (s *MaptokenContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PainlessParserVisitor:
		return t.VisitMaptoken(s)

	default:
		return t.VisitChildren(s)
	}
}

func (p *PainlessParser) Maptoken() (localctx IMaptokenContext) {
	localctx = NewMaptokenContext(p, p.GetParserRuleContext(), p.GetState())
	p.EnterRule(localctx, 66, PainlessParserRULE_maptoken)
	p.EnterOuterAlt(localctx, 1)
	{
		p.SetState(509)
		p.Expression()
	}
	{
		p.SetState(510)
		p.Match(PainlessParserCOLON)
		if p.HasError() {
			// Recognition error - abort rule
			goto errorExit
		}
	}
	{
		p.SetState(511)
		p.Expression()
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
	return localctx
	goto errorExit // Trick to prevent compiler error if the label is not used
}

// IArgumentsContext is an interface to support dynamic dispatch.
type IArgumentsContext interface {
	antlr.ParserRuleContext

	// GetParser returns the parser.
	GetParser() antlr.Parser

	// Getter signatures
	LP() antlr.TerminalNode
	RP() antlr.TerminalNode
	AllArgument() []IArgumentContext
	Argument(i int) IArgumentContext
	AllCOMMA() []antlr.TerminalNode
	COMMA(i int) antlr.TerminalNode

	// IsArgumentsContext differentiates from other interfaces.
	IsArgumentsContext()
}

type ArgumentsContext struct {
	antlr.BaseParserRuleContext
	parser antlr.Parser
}

func NewEmptyArgumentsContext() *ArgumentsContext {
	var p = new(ArgumentsContext)
	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, nil, -1)
	p.RuleIndex = PainlessParserRULE_arguments
	return p
}

func InitEmptyArgumentsContext(p *ArgumentsContext) {
	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, nil, -1)
	p.RuleIndex = PainlessParserRULE_arguments
}

func (*ArgumentsContext) IsArgumentsContext() {}

func NewArgumentsContext(parser antlr.Parser, parent antlr.ParserRuleContext, invokingState int) *ArgumentsContext {
	var p = new(ArgumentsContext)

	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, parent, invokingState)

	p.parser = parser
	p.RuleIndex = PainlessParserRULE_arguments

	return p
}

func (s *ArgumentsContext) GetParser() antlr.Parser { return s.parser }

func (s *ArgumentsContext) LP() antlr.TerminalNode {
	return s.GetToken(PainlessParserLP, 0)
}

func (s *ArgumentsContext) RP() antlr.TerminalNode {
	return s.GetToken(PainlessParserRP, 0)
}

func (s *ArgumentsContext) AllArgument() []IArgumentContext {
	children := s.GetChildren()
	len := 0
	for _, ctx := range children {
		if _, ok := ctx.(IArgumentContext); ok {
			len++
		}
	}

	tst := make([]IArgumentContext, len)
	i := 0
	for _, ctx := range children {
		if t, ok := ctx.(IArgumentContext); ok {
			tst[i] = t.(IArgumentContext)
			i++
		}
	}

	return tst
}

func (s *ArgumentsContext) Argument(i int) IArgumentContext {
	var t antlr.RuleContext
	j := 0
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IArgumentContext); ok {
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

	return t.(IArgumentContext)
}

func (s *ArgumentsContext) AllCOMMA() []antlr.TerminalNode {
	return s.GetTokens(PainlessParserCOMMA)
}

func (s *ArgumentsContext) COMMA(i int) antlr.TerminalNode {
	return s.GetToken(PainlessParserCOMMA, i)
}

func (s *ArgumentsContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *ArgumentsContext) ToStringTree(ruleNames []string, recog antlr.Recognizer) string {
	return antlr.TreesStringTree(s, ruleNames, recog)
}

func (s *ArgumentsContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PainlessParserListener); ok {
		listenerT.EnterArguments(s)
	}
}

func (s *ArgumentsContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PainlessParserListener); ok {
		listenerT.ExitArguments(s)
	}
}

func (s *ArgumentsContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PainlessParserVisitor:
		return t.VisitArguments(s)

	default:
		return t.VisitChildren(s)
	}
}

func (p *PainlessParser) Arguments() (localctx IArgumentsContext) {
	localctx = NewArgumentsContext(p, p.GetParserRuleContext(), p.GetState())
	p.EnterRule(localctx, 68, PainlessParserRULE_arguments)
	var _la int

	p.EnterOuterAlt(localctx, 1)
	{
		p.SetState(513)
		p.Match(PainlessParserLP)
		if p.HasError() {
			// Recognition error - abort rule
			goto errorExit
		}
	}
	p.SetState(522)
	p.GetErrorHandler().Sync(p)
	if p.HasError() {
		goto errorExit
	}
	_la = p.GetTokenStream().LA(1)

	if ((int64(_la) & ^0x3f) == 0 && ((int64(1)<<_la)&864691155101548704) != 0) || ((int64((_la-72)) & ^0x3f) == 0 && ((int64(1)<<(_la-72))&4095) != 0) {
		{
			p.SetState(514)
			p.Argument()
		}
		p.SetState(519)
		p.GetErrorHandler().Sync(p)
		if p.HasError() {
			goto errorExit
		}
		_la = p.GetTokenStream().LA(1)

		for _la == PainlessParserCOMMA {
			{
				p.SetState(515)
				p.Match(PainlessParserCOMMA)
				if p.HasError() {
					// Recognition error - abort rule
					goto errorExit
				}
			}
			{
				p.SetState(516)
				p.Argument()
			}

			p.SetState(521)
			p.GetErrorHandler().Sync(p)
			if p.HasError() {
				goto errorExit
			}
			_la = p.GetTokenStream().LA(1)
		}

	}
	{
		p.SetState(524)
		p.Match(PainlessParserRP)
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
	return localctx
	goto errorExit // Trick to prevent compiler error if the label is not used
}

// IArgumentContext is an interface to support dynamic dispatch.
type IArgumentContext interface {
	antlr.ParserRuleContext

	// GetParser returns the parser.
	GetParser() antlr.Parser

	// Getter signatures
	Expression() IExpressionContext
	Lambda() ILambdaContext
	Funcref() IFuncrefContext

	// IsArgumentContext differentiates from other interfaces.
	IsArgumentContext()
}

type ArgumentContext struct {
	antlr.BaseParserRuleContext
	parser antlr.Parser
}

func NewEmptyArgumentContext() *ArgumentContext {
	var p = new(ArgumentContext)
	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, nil, -1)
	p.RuleIndex = PainlessParserRULE_argument
	return p
}

func InitEmptyArgumentContext(p *ArgumentContext) {
	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, nil, -1)
	p.RuleIndex = PainlessParserRULE_argument
}

func (*ArgumentContext) IsArgumentContext() {}

func NewArgumentContext(parser antlr.Parser, parent antlr.ParserRuleContext, invokingState int) *ArgumentContext {
	var p = new(ArgumentContext)

	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, parent, invokingState)

	p.parser = parser
	p.RuleIndex = PainlessParserRULE_argument

	return p
}

func (s *ArgumentContext) GetParser() antlr.Parser { return s.parser }

func (s *ArgumentContext) Expression() IExpressionContext {
	var t antlr.RuleContext
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IExpressionContext); ok {
			t = ctx.(antlr.RuleContext)
			break
		}
	}

	if t == nil {
		return nil
	}

	return t.(IExpressionContext)
}

func (s *ArgumentContext) Lambda() ILambdaContext {
	var t antlr.RuleContext
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(ILambdaContext); ok {
			t = ctx.(antlr.RuleContext)
			break
		}
	}

	if t == nil {
		return nil
	}

	return t.(ILambdaContext)
}

func (s *ArgumentContext) Funcref() IFuncrefContext {
	var t antlr.RuleContext
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IFuncrefContext); ok {
			t = ctx.(antlr.RuleContext)
			break
		}
	}

	if t == nil {
		return nil
	}

	return t.(IFuncrefContext)
}

func (s *ArgumentContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *ArgumentContext) ToStringTree(ruleNames []string, recog antlr.Recognizer) string {
	return antlr.TreesStringTree(s, ruleNames, recog)
}

func (s *ArgumentContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PainlessParserListener); ok {
		listenerT.EnterArgument(s)
	}
}

func (s *ArgumentContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PainlessParserListener); ok {
		listenerT.ExitArgument(s)
	}
}

func (s *ArgumentContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PainlessParserVisitor:
		return t.VisitArgument(s)

	default:
		return t.VisitChildren(s)
	}
}

func (p *PainlessParser) Argument() (localctx IArgumentContext) {
	localctx = NewArgumentContext(p, p.GetParserRuleContext(), p.GetState())
	p.EnterRule(localctx, 70, PainlessParserRULE_argument)
	p.SetState(529)
	p.GetErrorHandler().Sync(p)
	if p.HasError() {
		goto errorExit
	}

	switch p.GetInterpreter().AdaptivePredict(p.BaseParser, p.GetTokenStream(), 52, p.GetParserRuleContext()) {
	case 1:
		p.EnterOuterAlt(localctx, 1)
		{
			p.SetState(526)
			p.Expression()
		}

	case 2:
		p.EnterOuterAlt(localctx, 2)
		{
			p.SetState(527)
			p.Lambda()
		}

	case 3:
		p.EnterOuterAlt(localctx, 3)
		{
			p.SetState(528)
			p.Funcref()
		}

	case antlr.ATNInvalidAltNumber:
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
	return localctx
	goto errorExit // Trick to prevent compiler error if the label is not used
}

// ILambdaContext is an interface to support dynamic dispatch.
type ILambdaContext interface {
	antlr.ParserRuleContext

	// GetParser returns the parser.
	GetParser() antlr.Parser

	// Getter signatures
	ARROW() antlr.TerminalNode
	AllLamtype() []ILamtypeContext
	Lamtype(i int) ILamtypeContext
	LP() antlr.TerminalNode
	RP() antlr.TerminalNode
	Block() IBlockContext
	Expression() IExpressionContext
	AllCOMMA() []antlr.TerminalNode
	COMMA(i int) antlr.TerminalNode

	// IsLambdaContext differentiates from other interfaces.
	IsLambdaContext()
}

type LambdaContext struct {
	antlr.BaseParserRuleContext
	parser antlr.Parser
}

func NewEmptyLambdaContext() *LambdaContext {
	var p = new(LambdaContext)
	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, nil, -1)
	p.RuleIndex = PainlessParserRULE_lambda
	return p
}

func InitEmptyLambdaContext(p *LambdaContext) {
	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, nil, -1)
	p.RuleIndex = PainlessParserRULE_lambda
}

func (*LambdaContext) IsLambdaContext() {}

func NewLambdaContext(parser antlr.Parser, parent antlr.ParserRuleContext, invokingState int) *LambdaContext {
	var p = new(LambdaContext)

	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, parent, invokingState)

	p.parser = parser
	p.RuleIndex = PainlessParserRULE_lambda

	return p
}

func (s *LambdaContext) GetParser() antlr.Parser { return s.parser }

func (s *LambdaContext) ARROW() antlr.TerminalNode {
	return s.GetToken(PainlessParserARROW, 0)
}

func (s *LambdaContext) AllLamtype() []ILamtypeContext {
	children := s.GetChildren()
	len := 0
	for _, ctx := range children {
		if _, ok := ctx.(ILamtypeContext); ok {
			len++
		}
	}

	tst := make([]ILamtypeContext, len)
	i := 0
	for _, ctx := range children {
		if t, ok := ctx.(ILamtypeContext); ok {
			tst[i] = t.(ILamtypeContext)
			i++
		}
	}

	return tst
}

func (s *LambdaContext) Lamtype(i int) ILamtypeContext {
	var t antlr.RuleContext
	j := 0
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(ILamtypeContext); ok {
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

	return t.(ILamtypeContext)
}

func (s *LambdaContext) LP() antlr.TerminalNode {
	return s.GetToken(PainlessParserLP, 0)
}

func (s *LambdaContext) RP() antlr.TerminalNode {
	return s.GetToken(PainlessParserRP, 0)
}

func (s *LambdaContext) Block() IBlockContext {
	var t antlr.RuleContext
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IBlockContext); ok {
			t = ctx.(antlr.RuleContext)
			break
		}
	}

	if t == nil {
		return nil
	}

	return t.(IBlockContext)
}

func (s *LambdaContext) Expression() IExpressionContext {
	var t antlr.RuleContext
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IExpressionContext); ok {
			t = ctx.(antlr.RuleContext)
			break
		}
	}

	if t == nil {
		return nil
	}

	return t.(IExpressionContext)
}

func (s *LambdaContext) AllCOMMA() []antlr.TerminalNode {
	return s.GetTokens(PainlessParserCOMMA)
}

func (s *LambdaContext) COMMA(i int) antlr.TerminalNode {
	return s.GetToken(PainlessParserCOMMA, i)
}

func (s *LambdaContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *LambdaContext) ToStringTree(ruleNames []string, recog antlr.Recognizer) string {
	return antlr.TreesStringTree(s, ruleNames, recog)
}

func (s *LambdaContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PainlessParserListener); ok {
		listenerT.EnterLambda(s)
	}
}

func (s *LambdaContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PainlessParserListener); ok {
		listenerT.ExitLambda(s)
	}
}

func (s *LambdaContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PainlessParserVisitor:
		return t.VisitLambda(s)

	default:
		return t.VisitChildren(s)
	}
}

func (p *PainlessParser) Lambda() (localctx ILambdaContext) {
	localctx = NewLambdaContext(p, p.GetParserRuleContext(), p.GetState())
	p.EnterRule(localctx, 72, PainlessParserRULE_lambda)
	var _la int

	p.EnterOuterAlt(localctx, 1)
	p.SetState(544)
	p.GetErrorHandler().Sync(p)
	if p.HasError() {
		goto errorExit
	}

	switch p.GetTokenStream().LA(1) {
	case PainlessParserPRIMITIVE, PainlessParserDEF, PainlessParserID:
		{
			p.SetState(531)
			p.Lamtype()
		}

	case PainlessParserLP:
		{
			p.SetState(532)
			p.Match(PainlessParserLP)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		p.SetState(541)
		p.GetErrorHandler().Sync(p)
		if p.HasError() {
			goto errorExit
		}
		_la = p.GetTokenStream().LA(1)

		if (int64((_la-81)) & ^0x3f) == 0 && ((int64(1)<<(_la-81))&7) != 0 {
			{
				p.SetState(533)
				p.Lamtype()
			}
			p.SetState(538)
			p.GetErrorHandler().Sync(p)
			if p.HasError() {
				goto errorExit
			}
			_la = p.GetTokenStream().LA(1)

			for _la == PainlessParserCOMMA {
				{
					p.SetState(534)
					p.Match(PainlessParserCOMMA)
					if p.HasError() {
						// Recognition error - abort rule
						goto errorExit
					}
				}
				{
					p.SetState(535)
					p.Lamtype()
				}

				p.SetState(540)
				p.GetErrorHandler().Sync(p)
				if p.HasError() {
					goto errorExit
				}
				_la = p.GetTokenStream().LA(1)
			}

		}
		{
			p.SetState(543)
			p.Match(PainlessParserRP)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}

	default:
		p.SetError(antlr.NewNoViableAltException(p, nil, nil, nil, nil, nil))
		goto errorExit
	}
	{
		p.SetState(546)
		p.Match(PainlessParserARROW)
		if p.HasError() {
			// Recognition error - abort rule
			goto errorExit
		}
	}
	p.SetState(549)
	p.GetErrorHandler().Sync(p)
	if p.HasError() {
		goto errorExit
	}

	switch p.GetTokenStream().LA(1) {
	case PainlessParserLBRACK:
		{
			p.SetState(547)
			p.Block()
		}

	case PainlessParserLBRACE, PainlessParserLP, PainlessParserNEW, PainlessParserBOOLNOT, PainlessParserBWNOT, PainlessParserADD, PainlessParserSUB, PainlessParserINCR, PainlessParserDECR, PainlessParserOCTAL, PainlessParserHEX, PainlessParserINTEGER, PainlessParserDECIMAL, PainlessParserSTRING, PainlessParserREGEX, PainlessParserTRUE, PainlessParserFALSE, PainlessParserNULL, PainlessParserID:
		{
			p.SetState(548)
			p.Expression()
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
	return localctx
	goto errorExit // Trick to prevent compiler error if the label is not used
}

// ILamtypeContext is an interface to support dynamic dispatch.
type ILamtypeContext interface {
	antlr.ParserRuleContext

	// GetParser returns the parser.
	GetParser() antlr.Parser

	// Getter signatures
	ID() antlr.TerminalNode
	Decltype() IDecltypeContext

	// IsLamtypeContext differentiates from other interfaces.
	IsLamtypeContext()
}

type LamtypeContext struct {
	antlr.BaseParserRuleContext
	parser antlr.Parser
}

func NewEmptyLamtypeContext() *LamtypeContext {
	var p = new(LamtypeContext)
	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, nil, -1)
	p.RuleIndex = PainlessParserRULE_lamtype
	return p
}

func InitEmptyLamtypeContext(p *LamtypeContext) {
	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, nil, -1)
	p.RuleIndex = PainlessParserRULE_lamtype
}

func (*LamtypeContext) IsLamtypeContext() {}

func NewLamtypeContext(parser antlr.Parser, parent antlr.ParserRuleContext, invokingState int) *LamtypeContext {
	var p = new(LamtypeContext)

	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, parent, invokingState)

	p.parser = parser
	p.RuleIndex = PainlessParserRULE_lamtype

	return p
}

func (s *LamtypeContext) GetParser() antlr.Parser { return s.parser }

func (s *LamtypeContext) ID() antlr.TerminalNode {
	return s.GetToken(PainlessParserID, 0)
}

func (s *LamtypeContext) Decltype() IDecltypeContext {
	var t antlr.RuleContext
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IDecltypeContext); ok {
			t = ctx.(antlr.RuleContext)
			break
		}
	}

	if t == nil {
		return nil
	}

	return t.(IDecltypeContext)
}

func (s *LamtypeContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *LamtypeContext) ToStringTree(ruleNames []string, recog antlr.Recognizer) string {
	return antlr.TreesStringTree(s, ruleNames, recog)
}

func (s *LamtypeContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PainlessParserListener); ok {
		listenerT.EnterLamtype(s)
	}
}

func (s *LamtypeContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PainlessParserListener); ok {
		listenerT.ExitLamtype(s)
	}
}

func (s *LamtypeContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PainlessParserVisitor:
		return t.VisitLamtype(s)

	default:
		return t.VisitChildren(s)
	}
}

func (p *PainlessParser) Lamtype() (localctx ILamtypeContext) {
	localctx = NewLamtypeContext(p, p.GetParserRuleContext(), p.GetState())
	p.EnterRule(localctx, 74, PainlessParserRULE_lamtype)
	p.EnterOuterAlt(localctx, 1)
	p.SetState(552)
	p.GetErrorHandler().Sync(p)

	if p.GetInterpreter().AdaptivePredict(p.BaseParser, p.GetTokenStream(), 57, p.GetParserRuleContext()) == 1 {
		{
			p.SetState(551)
			p.Decltype()
		}

	} else if p.HasError() { // JIM
		goto errorExit
	}
	{
		p.SetState(554)
		p.Match(PainlessParserID)
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
	return localctx
	goto errorExit // Trick to prevent compiler error if the label is not used
}

// IFuncrefContext is an interface to support dynamic dispatch.
type IFuncrefContext interface {
	antlr.ParserRuleContext

	// GetParser returns the parser.
	GetParser() antlr.Parser
	// IsFuncrefContext differentiates from other interfaces.
	IsFuncrefContext()
}

type FuncrefContext struct {
	antlr.BaseParserRuleContext
	parser antlr.Parser
}

func NewEmptyFuncrefContext() *FuncrefContext {
	var p = new(FuncrefContext)
	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, nil, -1)
	p.RuleIndex = PainlessParserRULE_funcref
	return p
}

func InitEmptyFuncrefContext(p *FuncrefContext) {
	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, nil, -1)
	p.RuleIndex = PainlessParserRULE_funcref
}

func (*FuncrefContext) IsFuncrefContext() {}

func NewFuncrefContext(parser antlr.Parser, parent antlr.ParserRuleContext, invokingState int) *FuncrefContext {
	var p = new(FuncrefContext)

	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, parent, invokingState)

	p.parser = parser
	p.RuleIndex = PainlessParserRULE_funcref

	return p
}

func (s *FuncrefContext) GetParser() antlr.Parser { return s.parser }

func (s *FuncrefContext) CopyAll(ctx *FuncrefContext) {
	s.CopyFrom(&ctx.BaseParserRuleContext)
}

func (s *FuncrefContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *FuncrefContext) ToStringTree(ruleNames []string, recog antlr.Recognizer) string {
	return antlr.TreesStringTree(s, ruleNames, recog)
}

type ClassfuncrefContext struct {
	FuncrefContext
}

func NewClassfuncrefContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *ClassfuncrefContext {
	var p = new(ClassfuncrefContext)

	InitEmptyFuncrefContext(&p.FuncrefContext)
	p.parser = parser
	p.CopyAll(ctx.(*FuncrefContext))

	return p
}

func (s *ClassfuncrefContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *ClassfuncrefContext) Decltype() IDecltypeContext {
	var t antlr.RuleContext
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IDecltypeContext); ok {
			t = ctx.(antlr.RuleContext)
			break
		}
	}

	if t == nil {
		return nil
	}

	return t.(IDecltypeContext)
}

func (s *ClassfuncrefContext) REF() antlr.TerminalNode {
	return s.GetToken(PainlessParserREF, 0)
}

func (s *ClassfuncrefContext) ID() antlr.TerminalNode {
	return s.GetToken(PainlessParserID, 0)
}

func (s *ClassfuncrefContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PainlessParserListener); ok {
		listenerT.EnterClassfuncref(s)
	}
}

func (s *ClassfuncrefContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PainlessParserListener); ok {
		listenerT.ExitClassfuncref(s)
	}
}

func (s *ClassfuncrefContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PainlessParserVisitor:
		return t.VisitClassfuncref(s)

	default:
		return t.VisitChildren(s)
	}
}

type ConstructorfuncrefContext struct {
	FuncrefContext
}

func NewConstructorfuncrefContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *ConstructorfuncrefContext {
	var p = new(ConstructorfuncrefContext)

	InitEmptyFuncrefContext(&p.FuncrefContext)
	p.parser = parser
	p.CopyAll(ctx.(*FuncrefContext))

	return p
}

func (s *ConstructorfuncrefContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *ConstructorfuncrefContext) Decltype() IDecltypeContext {
	var t antlr.RuleContext
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IDecltypeContext); ok {
			t = ctx.(antlr.RuleContext)
			break
		}
	}

	if t == nil {
		return nil
	}

	return t.(IDecltypeContext)
}

func (s *ConstructorfuncrefContext) REF() antlr.TerminalNode {
	return s.GetToken(PainlessParserREF, 0)
}

func (s *ConstructorfuncrefContext) NEW() antlr.TerminalNode {
	return s.GetToken(PainlessParserNEW, 0)
}

func (s *ConstructorfuncrefContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PainlessParserListener); ok {
		listenerT.EnterConstructorfuncref(s)
	}
}

func (s *ConstructorfuncrefContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PainlessParserListener); ok {
		listenerT.ExitConstructorfuncref(s)
	}
}

func (s *ConstructorfuncrefContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PainlessParserVisitor:
		return t.VisitConstructorfuncref(s)

	default:
		return t.VisitChildren(s)
	}
}

type LocalfuncrefContext struct {
	FuncrefContext
}

func NewLocalfuncrefContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *LocalfuncrefContext {
	var p = new(LocalfuncrefContext)

	InitEmptyFuncrefContext(&p.FuncrefContext)
	p.parser = parser
	p.CopyAll(ctx.(*FuncrefContext))

	return p
}

func (s *LocalfuncrefContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *LocalfuncrefContext) THIS() antlr.TerminalNode {
	return s.GetToken(PainlessParserTHIS, 0)
}

func (s *LocalfuncrefContext) REF() antlr.TerminalNode {
	return s.GetToken(PainlessParserREF, 0)
}

func (s *LocalfuncrefContext) ID() antlr.TerminalNode {
	return s.GetToken(PainlessParserID, 0)
}

func (s *LocalfuncrefContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PainlessParserListener); ok {
		listenerT.EnterLocalfuncref(s)
	}
}

func (s *LocalfuncrefContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PainlessParserListener); ok {
		listenerT.ExitLocalfuncref(s)
	}
}

func (s *LocalfuncrefContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PainlessParserVisitor:
		return t.VisitLocalfuncref(s)

	default:
		return t.VisitChildren(s)
	}
}

func (p *PainlessParser) Funcref() (localctx IFuncrefContext) {
	localctx = NewFuncrefContext(p, p.GetParserRuleContext(), p.GetState())
	p.EnterRule(localctx, 76, PainlessParserRULE_funcref)
	p.SetState(567)
	p.GetErrorHandler().Sync(p)
	if p.HasError() {
		goto errorExit
	}

	switch p.GetInterpreter().AdaptivePredict(p.BaseParser, p.GetTokenStream(), 58, p.GetParserRuleContext()) {
	case 1:
		localctx = NewClassfuncrefContext(p, localctx)
		p.EnterOuterAlt(localctx, 1)
		{
			p.SetState(556)
			p.Decltype()
		}
		{
			p.SetState(557)
			p.Match(PainlessParserREF)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(558)
			p.Match(PainlessParserID)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}

	case 2:
		localctx = NewConstructorfuncrefContext(p, localctx)
		p.EnterOuterAlt(localctx, 2)
		{
			p.SetState(560)
			p.Decltype()
		}
		{
			p.SetState(561)
			p.Match(PainlessParserREF)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(562)
			p.Match(PainlessParserNEW)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}

	case 3:
		localctx = NewLocalfuncrefContext(p, localctx)
		p.EnterOuterAlt(localctx, 3)
		{
			p.SetState(564)
			p.Match(PainlessParserTHIS)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(565)
			p.Match(PainlessParserREF)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(566)
			p.Match(PainlessParserID)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}

	case antlr.ATNInvalidAltNumber:
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
	return localctx
	goto errorExit // Trick to prevent compiler error if the label is not used
}

func (p *PainlessParser) Sempred(localctx antlr.RuleContext, ruleIndex, predIndex int) bool {
	switch ruleIndex {
	case 16:
		var t *NoncondexpressionContext = nil
		if localctx != nil {
			t = localctx.(*NoncondexpressionContext)
		}
		return p.Noncondexpression_Sempred(t, predIndex)

	default:
		panic("No predicate with index: " + fmt.Sprint(ruleIndex))
	}
}

func (p *PainlessParser) Noncondexpression_Sempred(localctx antlr.RuleContext, predIndex int) bool {
	switch predIndex {
	case 0:
		return p.Precpred(p.GetParserRuleContext(), 13)

	case 1:
		return p.Precpred(p.GetParserRuleContext(), 12)

	case 2:
		return p.Precpred(p.GetParserRuleContext(), 11)

	case 3:
		return p.Precpred(p.GetParserRuleContext(), 10)

	case 4:
		return p.Precpred(p.GetParserRuleContext(), 9)

	case 5:
		return p.Precpred(p.GetParserRuleContext(), 7)

	case 6:
		return p.Precpred(p.GetParserRuleContext(), 6)

	case 7:
		return p.Precpred(p.GetParserRuleContext(), 5)

	case 8:
		return p.Precpred(p.GetParserRuleContext(), 4)

	case 9:
		return p.Precpred(p.GetParserRuleContext(), 3)

	case 10:
		return p.Precpred(p.GetParserRuleContext(), 2)

	case 11:
		return p.Precpred(p.GetParserRuleContext(), 1)

	case 12:
		return p.Precpred(p.GetParserRuleContext(), 8)

	default:
		panic("No predicate with index: " + fmt.Sprint(predIndex))
	}
}
