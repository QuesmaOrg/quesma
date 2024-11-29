// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
// Code generated from quesma/queryparser/painless/antlr/PainlessLexer.g4 by ANTLR 4.13.2. DO NOT EDIT.

package parser

import (
	"fmt"
	"github.com/antlr4-go/antlr/v4"
	"sync"
	"unicode"
)

// Suppress unused import error
var _ = fmt.Printf
var _ = sync.Once{}
var _ = unicode.IsLetter

type PainlessLexer struct {
	*antlr.BaseLexer
	channelNames []string
	modeNames    []string
	// TODO: EOF string
}

var PainlessLexerLexerStaticData struct {
	once                   sync.Once
	serializedATN          []int32
	ChannelNames           []string
	ModeNames              []string
	LiteralNames           []string
	SymbolicNames          []string
	RuleNames              []string
	PredictionContextCache *antlr.PredictionContextCache
	atn                    *antlr.ATN
	decisionToDFA          []*antlr.DFA
}

func painlesslexerLexerInit() {
	staticData := &PainlessLexerLexerStaticData
	staticData.ChannelNames = []string{
		"DEFAULT_TOKEN_CHANNEL", "HIDDEN",
	}
	staticData.ModeNames = []string{
		"DEFAULT_MODE", "AFTER_DOT",
	}
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
		"WS", "COMMENT", "LBRACK", "RBRACK", "LBRACE", "RBRACE", "LP", "RP",
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
	staticData.PredictionContextCache = antlr.NewPredictionContextCache()
	staticData.serializedATN = []int32{
		4, 0, 85, 631, 6, -1, 6, -1, 2, 0, 7, 0, 2, 1, 7, 1, 2, 2, 7, 2, 2, 3,
		7, 3, 2, 4, 7, 4, 2, 5, 7, 5, 2, 6, 7, 6, 2, 7, 7, 7, 2, 8, 7, 8, 2, 9,
		7, 9, 2, 10, 7, 10, 2, 11, 7, 11, 2, 12, 7, 12, 2, 13, 7, 13, 2, 14, 7,
		14, 2, 15, 7, 15, 2, 16, 7, 16, 2, 17, 7, 17, 2, 18, 7, 18, 2, 19, 7, 19,
		2, 20, 7, 20, 2, 21, 7, 21, 2, 22, 7, 22, 2, 23, 7, 23, 2, 24, 7, 24, 2,
		25, 7, 25, 2, 26, 7, 26, 2, 27, 7, 27, 2, 28, 7, 28, 2, 29, 7, 29, 2, 30,
		7, 30, 2, 31, 7, 31, 2, 32, 7, 32, 2, 33, 7, 33, 2, 34, 7, 34, 2, 35, 7,
		35, 2, 36, 7, 36, 2, 37, 7, 37, 2, 38, 7, 38, 2, 39, 7, 39, 2, 40, 7, 40,
		2, 41, 7, 41, 2, 42, 7, 42, 2, 43, 7, 43, 2, 44, 7, 44, 2, 45, 7, 45, 2,
		46, 7, 46, 2, 47, 7, 47, 2, 48, 7, 48, 2, 49, 7, 49, 2, 50, 7, 50, 2, 51,
		7, 51, 2, 52, 7, 52, 2, 53, 7, 53, 2, 54, 7, 54, 2, 55, 7, 55, 2, 56, 7,
		56, 2, 57, 7, 57, 2, 58, 7, 58, 2, 59, 7, 59, 2, 60, 7, 60, 2, 61, 7, 61,
		2, 62, 7, 62, 2, 63, 7, 63, 2, 64, 7, 64, 2, 65, 7, 65, 2, 66, 7, 66, 2,
		67, 7, 67, 2, 68, 7, 68, 2, 69, 7, 69, 2, 70, 7, 70, 2, 71, 7, 71, 2, 72,
		7, 72, 2, 73, 7, 73, 2, 74, 7, 74, 2, 75, 7, 75, 2, 76, 7, 76, 2, 77, 7,
		77, 2, 78, 7, 78, 2, 79, 7, 79, 2, 80, 7, 80, 2, 81, 7, 81, 2, 82, 7, 82,
		2, 83, 7, 83, 2, 84, 7, 84, 1, 0, 4, 0, 174, 8, 0, 11, 0, 12, 0, 175, 1,
		0, 1, 0, 1, 1, 1, 1, 1, 1, 1, 1, 5, 1, 184, 8, 1, 10, 1, 12, 1, 187, 9,
		1, 1, 1, 1, 1, 1, 1, 1, 1, 5, 1, 193, 8, 1, 10, 1, 12, 1, 196, 9, 1, 1,
		1, 1, 1, 3, 1, 200, 8, 1, 1, 1, 1, 1, 1, 2, 1, 2, 1, 3, 1, 3, 1, 4, 1,
		4, 1, 5, 1, 5, 1, 6, 1, 6, 1, 7, 1, 7, 1, 8, 1, 8, 1, 8, 1, 8, 1, 9, 1,
		9, 1, 9, 1, 9, 1, 9, 1, 10, 1, 10, 1, 11, 1, 11, 1, 12, 1, 12, 1, 12, 1,
		13, 1, 13, 1, 13, 1, 14, 1, 14, 1, 14, 1, 14, 1, 14, 1, 15, 1, 15, 1, 15,
		1, 15, 1, 15, 1, 15, 1, 16, 1, 16, 1, 16, 1, 17, 1, 17, 1, 17, 1, 17, 1,
		18, 1, 18, 1, 18, 1, 18, 1, 18, 1, 18, 1, 18, 1, 18, 1, 18, 1, 19, 1, 19,
		1, 19, 1, 19, 1, 19, 1, 19, 1, 20, 1, 20, 1, 20, 1, 20, 1, 20, 1, 20, 1,
		20, 1, 21, 1, 21, 1, 21, 1, 21, 1, 22, 1, 22, 1, 22, 1, 22, 1, 23, 1, 23,
		1, 23, 1, 23, 1, 23, 1, 23, 1, 24, 1, 24, 1, 24, 1, 24, 1, 24, 1, 24, 1,
		25, 1, 25, 1, 25, 1, 25, 1, 25, 1, 26, 1, 26, 1, 26, 1, 26, 1, 26, 1, 26,
		1, 26, 1, 26, 1, 26, 1, 26, 1, 26, 1, 27, 1, 27, 1, 28, 1, 28, 1, 29, 1,
		29, 1, 30, 1, 30, 1, 30, 1, 31, 1, 31, 1, 32, 1, 32, 1, 33, 1, 33, 1, 34,
		1, 34, 1, 34, 1, 35, 1, 35, 1, 35, 1, 36, 1, 36, 1, 36, 1, 36, 1, 37, 1,
		37, 1, 38, 1, 38, 1, 38, 1, 39, 1, 39, 1, 40, 1, 40, 1, 40, 1, 41, 1, 41,
		1, 41, 1, 42, 1, 42, 1, 42, 1, 42, 1, 43, 1, 43, 1, 43, 1, 44, 1, 44, 1,
		44, 1, 44, 1, 45, 1, 45, 1, 46, 1, 46, 1, 47, 1, 47, 1, 48, 1, 48, 1, 48,
		1, 49, 1, 49, 1, 49, 1, 50, 1, 50, 1, 51, 1, 51, 1, 52, 1, 52, 1, 52, 1,
		53, 1, 53, 1, 53, 1, 54, 1, 54, 1, 54, 1, 55, 1, 55, 1, 55, 1, 56, 1, 56,
		1, 56, 1, 56, 1, 57, 1, 57, 1, 57, 1, 58, 1, 58, 1, 58, 1, 59, 1, 59, 1,
		60, 1, 60, 1, 60, 1, 61, 1, 61, 1, 61, 1, 62, 1, 62, 1, 62, 1, 63, 1, 63,
		1, 63, 1, 64, 1, 64, 1, 64, 1, 65, 1, 65, 1, 65, 1, 66, 1, 66, 1, 66, 1,
		67, 1, 67, 1, 67, 1, 68, 1, 68, 1, 68, 1, 68, 1, 69, 1, 69, 1, 69, 1, 69,
		1, 70, 1, 70, 1, 70, 1, 70, 1, 70, 1, 71, 1, 71, 4, 71, 439, 8, 71, 11,
		71, 12, 71, 440, 1, 71, 3, 71, 444, 8, 71, 1, 72, 1, 72, 1, 72, 4, 72,
		449, 8, 72, 11, 72, 12, 72, 450, 1, 72, 3, 72, 454, 8, 72, 1, 73, 1, 73,
		1, 73, 5, 73, 459, 8, 73, 10, 73, 12, 73, 462, 9, 73, 3, 73, 464, 8, 73,
		1, 73, 3, 73, 467, 8, 73, 1, 74, 1, 74, 1, 74, 5, 74, 472, 8, 74, 10, 74,
		12, 74, 475, 9, 74, 3, 74, 477, 8, 74, 1, 74, 1, 74, 4, 74, 481, 8, 74,
		11, 74, 12, 74, 482, 3, 74, 485, 8, 74, 1, 74, 1, 74, 3, 74, 489, 8, 74,
		1, 74, 4, 74, 492, 8, 74, 11, 74, 12, 74, 493, 3, 74, 496, 8, 74, 1, 74,
		3, 74, 499, 8, 74, 1, 75, 1, 75, 1, 75, 1, 75, 1, 75, 1, 75, 5, 75, 507,
		8, 75, 10, 75, 12, 75, 510, 9, 75, 1, 75, 1, 75, 1, 75, 1, 75, 1, 75, 1,
		75, 1, 75, 5, 75, 519, 8, 75, 10, 75, 12, 75, 522, 9, 75, 1, 75, 3, 75,
		525, 8, 75, 1, 76, 1, 76, 1, 76, 1, 76, 4, 76, 531, 8, 76, 11, 76, 12,
		76, 532, 1, 76, 1, 76, 5, 76, 537, 8, 76, 10, 76, 12, 76, 540, 9, 76, 1,
		76, 1, 76, 1, 77, 1, 77, 1, 77, 1, 77, 1, 77, 1, 78, 1, 78, 1, 78, 1, 78,
		1, 78, 1, 78, 1, 79, 1, 79, 1, 79, 1, 79, 1, 79, 1, 80, 1, 80, 1, 80, 1,
		80, 1, 80, 1, 80, 1, 80, 1, 80, 1, 80, 1, 80, 1, 80, 1, 80, 1, 80, 1, 80,
		1, 80, 1, 80, 1, 80, 1, 80, 1, 80, 1, 80, 1, 80, 1, 80, 1, 80, 1, 80, 1,
		80, 1, 80, 1, 80, 1, 80, 1, 80, 1, 80, 1, 80, 1, 80, 1, 80, 1, 80, 1, 80,
		1, 80, 1, 80, 1, 80, 3, 80, 598, 8, 80, 1, 81, 1, 81, 1, 81, 1, 81, 1,
		82, 1, 82, 5, 82, 606, 8, 82, 10, 82, 12, 82, 609, 9, 82, 1, 83, 1, 83,
		1, 83, 5, 83, 614, 8, 83, 10, 83, 12, 83, 617, 9, 83, 3, 83, 619, 8, 83,
		1, 83, 1, 83, 1, 84, 1, 84, 5, 84, 625, 8, 84, 10, 84, 12, 84, 628, 9,
		84, 1, 84, 1, 84, 4, 194, 508, 520, 532, 0, 85, 2, 1, 4, 2, 6, 3, 8, 4,
		10, 5, 12, 6, 14, 7, 16, 8, 18, 9, 20, 10, 22, 11, 24, 12, 26, 13, 28,
		14, 30, 15, 32, 16, 34, 17, 36, 18, 38, 19, 40, 20, 42, 21, 44, 22, 46,
		23, 48, 24, 50, 25, 52, 26, 54, 27, 56, 28, 58, 29, 60, 30, 62, 31, 64,
		32, 66, 33, 68, 34, 70, 35, 72, 36, 74, 37, 76, 38, 78, 39, 80, 40, 82,
		41, 84, 42, 86, 43, 88, 44, 90, 45, 92, 46, 94, 47, 96, 48, 98, 49, 100,
		50, 102, 51, 104, 52, 106, 53, 108, 54, 110, 55, 112, 56, 114, 57, 116,
		58, 118, 59, 120, 60, 122, 61, 124, 62, 126, 63, 128, 64, 130, 65, 132,
		66, 134, 67, 136, 68, 138, 69, 140, 70, 142, 71, 144, 72, 146, 73, 148,
		74, 150, 75, 152, 76, 154, 77, 156, 78, 158, 79, 160, 80, 162, 81, 164,
		82, 166, 83, 168, 84, 170, 85, 2, 0, 1, 19, 3, 0, 9, 10, 13, 13, 32, 32,
		2, 0, 10, 10, 13, 13, 1, 0, 48, 55, 2, 0, 76, 76, 108, 108, 2, 0, 88, 88,
		120, 120, 3, 0, 48, 57, 65, 70, 97, 102, 1, 0, 49, 57, 1, 0, 48, 57, 6,
		0, 68, 68, 70, 70, 76, 76, 100, 100, 102, 102, 108, 108, 2, 0, 69, 69,
		101, 101, 2, 0, 43, 43, 45, 45, 4, 0, 68, 68, 70, 70, 100, 100, 102, 102,
		2, 0, 34, 34, 92, 92, 2, 0, 39, 39, 92, 92, 1, 0, 10, 10, 2, 0, 10, 10,
		47, 47, 7, 0, 85, 85, 99, 99, 105, 105, 108, 109, 115, 115, 117, 117, 120,
		120, 3, 0, 65, 90, 95, 95, 97, 122, 4, 0, 48, 57, 65, 90, 95, 95, 97, 122,
		669, 0, 2, 1, 0, 0, 0, 0, 4, 1, 0, 0, 0, 0, 6, 1, 0, 0, 0, 0, 8, 1, 0,
		0, 0, 0, 10, 1, 0, 0, 0, 0, 12, 1, 0, 0, 0, 0, 14, 1, 0, 0, 0, 0, 16, 1,
		0, 0, 0, 0, 18, 1, 0, 0, 0, 0, 20, 1, 0, 0, 0, 0, 22, 1, 0, 0, 0, 0, 24,
		1, 0, 0, 0, 0, 26, 1, 0, 0, 0, 0, 28, 1, 0, 0, 0, 0, 30, 1, 0, 0, 0, 0,
		32, 1, 0, 0, 0, 0, 34, 1, 0, 0, 0, 0, 36, 1, 0, 0, 0, 0, 38, 1, 0, 0, 0,
		0, 40, 1, 0, 0, 0, 0, 42, 1, 0, 0, 0, 0, 44, 1, 0, 0, 0, 0, 46, 1, 0, 0,
		0, 0, 48, 1, 0, 0, 0, 0, 50, 1, 0, 0, 0, 0, 52, 1, 0, 0, 0, 0, 54, 1, 0,
		0, 0, 0, 56, 1, 0, 0, 0, 0, 58, 1, 0, 0, 0, 0, 60, 1, 0, 0, 0, 0, 62, 1,
		0, 0, 0, 0, 64, 1, 0, 0, 0, 0, 66, 1, 0, 0, 0, 0, 68, 1, 0, 0, 0, 0, 70,
		1, 0, 0, 0, 0, 72, 1, 0, 0, 0, 0, 74, 1, 0, 0, 0, 0, 76, 1, 0, 0, 0, 0,
		78, 1, 0, 0, 0, 0, 80, 1, 0, 0, 0, 0, 82, 1, 0, 0, 0, 0, 84, 1, 0, 0, 0,
		0, 86, 1, 0, 0, 0, 0, 88, 1, 0, 0, 0, 0, 90, 1, 0, 0, 0, 0, 92, 1, 0, 0,
		0, 0, 94, 1, 0, 0, 0, 0, 96, 1, 0, 0, 0, 0, 98, 1, 0, 0, 0, 0, 100, 1,
		0, 0, 0, 0, 102, 1, 0, 0, 0, 0, 104, 1, 0, 0, 0, 0, 106, 1, 0, 0, 0, 0,
		108, 1, 0, 0, 0, 0, 110, 1, 0, 0, 0, 0, 112, 1, 0, 0, 0, 0, 114, 1, 0,
		0, 0, 0, 116, 1, 0, 0, 0, 0, 118, 1, 0, 0, 0, 0, 120, 1, 0, 0, 0, 0, 122,
		1, 0, 0, 0, 0, 124, 1, 0, 0, 0, 0, 126, 1, 0, 0, 0, 0, 128, 1, 0, 0, 0,
		0, 130, 1, 0, 0, 0, 0, 132, 1, 0, 0, 0, 0, 134, 1, 0, 0, 0, 0, 136, 1,
		0, 0, 0, 0, 138, 1, 0, 0, 0, 0, 140, 1, 0, 0, 0, 0, 142, 1, 0, 0, 0, 0,
		144, 1, 0, 0, 0, 0, 146, 1, 0, 0, 0, 0, 148, 1, 0, 0, 0, 0, 150, 1, 0,
		0, 0, 0, 152, 1, 0, 0, 0, 0, 154, 1, 0, 0, 0, 0, 156, 1, 0, 0, 0, 0, 158,
		1, 0, 0, 0, 0, 160, 1, 0, 0, 0, 0, 162, 1, 0, 0, 0, 0, 164, 1, 0, 0, 0,
		0, 166, 1, 0, 0, 0, 1, 168, 1, 0, 0, 0, 1, 170, 1, 0, 0, 0, 2, 173, 1,
		0, 0, 0, 4, 199, 1, 0, 0, 0, 6, 203, 1, 0, 0, 0, 8, 205, 1, 0, 0, 0, 10,
		207, 1, 0, 0, 0, 12, 209, 1, 0, 0, 0, 14, 211, 1, 0, 0, 0, 16, 213, 1,
		0, 0, 0, 18, 215, 1, 0, 0, 0, 20, 219, 1, 0, 0, 0, 22, 224, 1, 0, 0, 0,
		24, 226, 1, 0, 0, 0, 26, 228, 1, 0, 0, 0, 28, 231, 1, 0, 0, 0, 30, 234,
		1, 0, 0, 0, 32, 239, 1, 0, 0, 0, 34, 245, 1, 0, 0, 0, 36, 248, 1, 0, 0,
		0, 38, 252, 1, 0, 0, 0, 40, 261, 1, 0, 0, 0, 42, 267, 1, 0, 0, 0, 44, 274,
		1, 0, 0, 0, 46, 278, 1, 0, 0, 0, 48, 282, 1, 0, 0, 0, 50, 288, 1, 0, 0,
		0, 52, 294, 1, 0, 0, 0, 54, 299, 1, 0, 0, 0, 56, 310, 1, 0, 0, 0, 58, 312,
		1, 0, 0, 0, 60, 314, 1, 0, 0, 0, 62, 316, 1, 0, 0, 0, 64, 319, 1, 0, 0,
		0, 66, 321, 1, 0, 0, 0, 68, 323, 1, 0, 0, 0, 70, 325, 1, 0, 0, 0, 72, 328,
		1, 0, 0, 0, 74, 331, 1, 0, 0, 0, 76, 335, 1, 0, 0, 0, 78, 337, 1, 0, 0,
		0, 80, 340, 1, 0, 0, 0, 82, 342, 1, 0, 0, 0, 84, 345, 1, 0, 0, 0, 86, 348,
		1, 0, 0, 0, 88, 352, 1, 0, 0, 0, 90, 355, 1, 0, 0, 0, 92, 359, 1, 0, 0,
		0, 94, 361, 1, 0, 0, 0, 96, 363, 1, 0, 0, 0, 98, 365, 1, 0, 0, 0, 100,
		368, 1, 0, 0, 0, 102, 371, 1, 0, 0, 0, 104, 373, 1, 0, 0, 0, 106, 375,
		1, 0, 0, 0, 108, 378, 1, 0, 0, 0, 110, 381, 1, 0, 0, 0, 112, 384, 1, 0,
		0, 0, 114, 387, 1, 0, 0, 0, 116, 391, 1, 0, 0, 0, 118, 394, 1, 0, 0, 0,
		120, 397, 1, 0, 0, 0, 122, 399, 1, 0, 0, 0, 124, 402, 1, 0, 0, 0, 126,
		405, 1, 0, 0, 0, 128, 408, 1, 0, 0, 0, 130, 411, 1, 0, 0, 0, 132, 414,
		1, 0, 0, 0, 134, 417, 1, 0, 0, 0, 136, 420, 1, 0, 0, 0, 138, 423, 1, 0,
		0, 0, 140, 427, 1, 0, 0, 0, 142, 431, 1, 0, 0, 0, 144, 436, 1, 0, 0, 0,
		146, 445, 1, 0, 0, 0, 148, 463, 1, 0, 0, 0, 150, 476, 1, 0, 0, 0, 152,
		524, 1, 0, 0, 0, 154, 526, 1, 0, 0, 0, 156, 543, 1, 0, 0, 0, 158, 548,
		1, 0, 0, 0, 160, 554, 1, 0, 0, 0, 162, 597, 1, 0, 0, 0, 164, 599, 1, 0,
		0, 0, 166, 603, 1, 0, 0, 0, 168, 618, 1, 0, 0, 0, 170, 622, 1, 0, 0, 0,
		172, 174, 7, 0, 0, 0, 173, 172, 1, 0, 0, 0, 174, 175, 1, 0, 0, 0, 175,
		173, 1, 0, 0, 0, 175, 176, 1, 0, 0, 0, 176, 177, 1, 0, 0, 0, 177, 178,
		6, 0, 0, 0, 178, 3, 1, 0, 0, 0, 179, 180, 5, 47, 0, 0, 180, 181, 5, 47,
		0, 0, 181, 185, 1, 0, 0, 0, 182, 184, 8, 1, 0, 0, 183, 182, 1, 0, 0, 0,
		184, 187, 1, 0, 0, 0, 185, 183, 1, 0, 0, 0, 185, 186, 1, 0, 0, 0, 186,
		200, 1, 0, 0, 0, 187, 185, 1, 0, 0, 0, 188, 189, 5, 47, 0, 0, 189, 190,
		5, 42, 0, 0, 190, 194, 1, 0, 0, 0, 191, 193, 9, 0, 0, 0, 192, 191, 1, 0,
		0, 0, 193, 196, 1, 0, 0, 0, 194, 195, 1, 0, 0, 0, 194, 192, 1, 0, 0, 0,
		195, 197, 1, 0, 0, 0, 196, 194, 1, 0, 0, 0, 197, 198, 5, 42, 0, 0, 198,
		200, 5, 47, 0, 0, 199, 179, 1, 0, 0, 0, 199, 188, 1, 0, 0, 0, 200, 201,
		1, 0, 0, 0, 201, 202, 6, 1, 0, 0, 202, 5, 1, 0, 0, 0, 203, 204, 5, 123,
		0, 0, 204, 7, 1, 0, 0, 0, 205, 206, 5, 125, 0, 0, 206, 9, 1, 0, 0, 0, 207,
		208, 5, 91, 0, 0, 208, 11, 1, 0, 0, 0, 209, 210, 5, 93, 0, 0, 210, 13,
		1, 0, 0, 0, 211, 212, 5, 40, 0, 0, 212, 15, 1, 0, 0, 0, 213, 214, 5, 41,
		0, 0, 214, 17, 1, 0, 0, 0, 215, 216, 5, 46, 0, 0, 216, 217, 1, 0, 0, 0,
		217, 218, 6, 8, 1, 0, 218, 19, 1, 0, 0, 0, 219, 220, 5, 63, 0, 0, 220,
		221, 5, 46, 0, 0, 221, 222, 1, 0, 0, 0, 222, 223, 6, 9, 1, 0, 223, 21,
		1, 0, 0, 0, 224, 225, 5, 44, 0, 0, 225, 23, 1, 0, 0, 0, 226, 227, 5, 59,
		0, 0, 227, 25, 1, 0, 0, 0, 228, 229, 5, 105, 0, 0, 229, 230, 5, 102, 0,
		0, 230, 27, 1, 0, 0, 0, 231, 232, 5, 105, 0, 0, 232, 233, 5, 110, 0, 0,
		233, 29, 1, 0, 0, 0, 234, 235, 5, 101, 0, 0, 235, 236, 5, 108, 0, 0, 236,
		237, 5, 115, 0, 0, 237, 238, 5, 101, 0, 0, 238, 31, 1, 0, 0, 0, 239, 240,
		5, 119, 0, 0, 240, 241, 5, 104, 0, 0, 241, 242, 5, 105, 0, 0, 242, 243,
		5, 108, 0, 0, 243, 244, 5, 101, 0, 0, 244, 33, 1, 0, 0, 0, 245, 246, 5,
		100, 0, 0, 246, 247, 5, 111, 0, 0, 247, 35, 1, 0, 0, 0, 248, 249, 5, 102,
		0, 0, 249, 250, 5, 111, 0, 0, 250, 251, 5, 114, 0, 0, 251, 37, 1, 0, 0,
		0, 252, 253, 5, 99, 0, 0, 253, 254, 5, 111, 0, 0, 254, 255, 5, 110, 0,
		0, 255, 256, 5, 116, 0, 0, 256, 257, 5, 105, 0, 0, 257, 258, 5, 110, 0,
		0, 258, 259, 5, 117, 0, 0, 259, 260, 5, 101, 0, 0, 260, 39, 1, 0, 0, 0,
		261, 262, 5, 98, 0, 0, 262, 263, 5, 114, 0, 0, 263, 264, 5, 101, 0, 0,
		264, 265, 5, 97, 0, 0, 265, 266, 5, 107, 0, 0, 266, 41, 1, 0, 0, 0, 267,
		268, 5, 114, 0, 0, 268, 269, 5, 101, 0, 0, 269, 270, 5, 116, 0, 0, 270,
		271, 5, 117, 0, 0, 271, 272, 5, 114, 0, 0, 272, 273, 5, 110, 0, 0, 273,
		43, 1, 0, 0, 0, 274, 275, 5, 110, 0, 0, 275, 276, 5, 101, 0, 0, 276, 277,
		5, 119, 0, 0, 277, 45, 1, 0, 0, 0, 278, 279, 5, 116, 0, 0, 279, 280, 5,
		114, 0, 0, 280, 281, 5, 121, 0, 0, 281, 47, 1, 0, 0, 0, 282, 283, 5, 99,
		0, 0, 283, 284, 5, 97, 0, 0, 284, 285, 5, 116, 0, 0, 285, 286, 5, 99, 0,
		0, 286, 287, 5, 104, 0, 0, 287, 49, 1, 0, 0, 0, 288, 289, 5, 116, 0, 0,
		289, 290, 5, 104, 0, 0, 290, 291, 5, 114, 0, 0, 291, 292, 5, 111, 0, 0,
		292, 293, 5, 119, 0, 0, 293, 51, 1, 0, 0, 0, 294, 295, 5, 116, 0, 0, 295,
		296, 5, 104, 0, 0, 296, 297, 5, 105, 0, 0, 297, 298, 5, 115, 0, 0, 298,
		53, 1, 0, 0, 0, 299, 300, 5, 105, 0, 0, 300, 301, 5, 110, 0, 0, 301, 302,
		5, 115, 0, 0, 302, 303, 5, 116, 0, 0, 303, 304, 5, 97, 0, 0, 304, 305,
		5, 110, 0, 0, 305, 306, 5, 99, 0, 0, 306, 307, 5, 101, 0, 0, 307, 308,
		5, 111, 0, 0, 308, 309, 5, 102, 0, 0, 309, 55, 1, 0, 0, 0, 310, 311, 5,
		33, 0, 0, 311, 57, 1, 0, 0, 0, 312, 313, 5, 126, 0, 0, 313, 59, 1, 0, 0,
		0, 314, 315, 5, 42, 0, 0, 315, 61, 1, 0, 0, 0, 316, 317, 5, 47, 0, 0, 317,
		318, 4, 30, 0, 0, 318, 63, 1, 0, 0, 0, 319, 320, 5, 37, 0, 0, 320, 65,
		1, 0, 0, 0, 321, 322, 5, 43, 0, 0, 322, 67, 1, 0, 0, 0, 323, 324, 5, 45,
		0, 0, 324, 69, 1, 0, 0, 0, 325, 326, 5, 60, 0, 0, 326, 327, 5, 60, 0, 0,
		327, 71, 1, 0, 0, 0, 328, 329, 5, 62, 0, 0, 329, 330, 5, 62, 0, 0, 330,
		73, 1, 0, 0, 0, 331, 332, 5, 62, 0, 0, 332, 333, 5, 62, 0, 0, 333, 334,
		5, 62, 0, 0, 334, 75, 1, 0, 0, 0, 335, 336, 5, 60, 0, 0, 336, 77, 1, 0,
		0, 0, 337, 338, 5, 60, 0, 0, 338, 339, 5, 61, 0, 0, 339, 79, 1, 0, 0, 0,
		340, 341, 5, 62, 0, 0, 341, 81, 1, 0, 0, 0, 342, 343, 5, 62, 0, 0, 343,
		344, 5, 61, 0, 0, 344, 83, 1, 0, 0, 0, 345, 346, 5, 61, 0, 0, 346, 347,
		5, 61, 0, 0, 347, 85, 1, 0, 0, 0, 348, 349, 5, 61, 0, 0, 349, 350, 5, 61,
		0, 0, 350, 351, 5, 61, 0, 0, 351, 87, 1, 0, 0, 0, 352, 353, 5, 33, 0, 0,
		353, 354, 5, 61, 0, 0, 354, 89, 1, 0, 0, 0, 355, 356, 5, 33, 0, 0, 356,
		357, 5, 61, 0, 0, 357, 358, 5, 61, 0, 0, 358, 91, 1, 0, 0, 0, 359, 360,
		5, 38, 0, 0, 360, 93, 1, 0, 0, 0, 361, 362, 5, 94, 0, 0, 362, 95, 1, 0,
		0, 0, 363, 364, 5, 124, 0, 0, 364, 97, 1, 0, 0, 0, 365, 366, 5, 38, 0,
		0, 366, 367, 5, 38, 0, 0, 367, 99, 1, 0, 0, 0, 368, 369, 5, 124, 0, 0,
		369, 370, 5, 124, 0, 0, 370, 101, 1, 0, 0, 0, 371, 372, 5, 63, 0, 0, 372,
		103, 1, 0, 0, 0, 373, 374, 5, 58, 0, 0, 374, 105, 1, 0, 0, 0, 375, 376,
		5, 63, 0, 0, 376, 377, 5, 58, 0, 0, 377, 107, 1, 0, 0, 0, 378, 379, 5,
		58, 0, 0, 379, 380, 5, 58, 0, 0, 380, 109, 1, 0, 0, 0, 381, 382, 5, 45,
		0, 0, 382, 383, 5, 62, 0, 0, 383, 111, 1, 0, 0, 0, 384, 385, 5, 61, 0,
		0, 385, 386, 5, 126, 0, 0, 386, 113, 1, 0, 0, 0, 387, 388, 5, 61, 0, 0,
		388, 389, 5, 61, 0, 0, 389, 390, 5, 126, 0, 0, 390, 115, 1, 0, 0, 0, 391,
		392, 5, 43, 0, 0, 392, 393, 5, 43, 0, 0, 393, 117, 1, 0, 0, 0, 394, 395,
		5, 45, 0, 0, 395, 396, 5, 45, 0, 0, 396, 119, 1, 0, 0, 0, 397, 398, 5,
		61, 0, 0, 398, 121, 1, 0, 0, 0, 399, 400, 5, 43, 0, 0, 400, 401, 5, 61,
		0, 0, 401, 123, 1, 0, 0, 0, 402, 403, 5, 45, 0, 0, 403, 404, 5, 61, 0,
		0, 404, 125, 1, 0, 0, 0, 405, 406, 5, 42, 0, 0, 406, 407, 5, 61, 0, 0,
		407, 127, 1, 0, 0, 0, 408, 409, 5, 47, 0, 0, 409, 410, 5, 61, 0, 0, 410,
		129, 1, 0, 0, 0, 411, 412, 5, 37, 0, 0, 412, 413, 5, 61, 0, 0, 413, 131,
		1, 0, 0, 0, 414, 415, 5, 38, 0, 0, 415, 416, 5, 61, 0, 0, 416, 133, 1,
		0, 0, 0, 417, 418, 5, 94, 0, 0, 418, 419, 5, 61, 0, 0, 419, 135, 1, 0,
		0, 0, 420, 421, 5, 124, 0, 0, 421, 422, 5, 61, 0, 0, 422, 137, 1, 0, 0,
		0, 423, 424, 5, 60, 0, 0, 424, 425, 5, 60, 0, 0, 425, 426, 5, 61, 0, 0,
		426, 139, 1, 0, 0, 0, 427, 428, 5, 62, 0, 0, 428, 429, 5, 62, 0, 0, 429,
		430, 5, 61, 0, 0, 430, 141, 1, 0, 0, 0, 431, 432, 5, 62, 0, 0, 432, 433,
		5, 62, 0, 0, 433, 434, 5, 62, 0, 0, 434, 435, 5, 61, 0, 0, 435, 143, 1,
		0, 0, 0, 436, 438, 5, 48, 0, 0, 437, 439, 7, 2, 0, 0, 438, 437, 1, 0, 0,
		0, 439, 440, 1, 0, 0, 0, 440, 438, 1, 0, 0, 0, 440, 441, 1, 0, 0, 0, 441,
		443, 1, 0, 0, 0, 442, 444, 7, 3, 0, 0, 443, 442, 1, 0, 0, 0, 443, 444,
		1, 0, 0, 0, 444, 145, 1, 0, 0, 0, 445, 446, 5, 48, 0, 0, 446, 448, 7, 4,
		0, 0, 447, 449, 7, 5, 0, 0, 448, 447, 1, 0, 0, 0, 449, 450, 1, 0, 0, 0,
		450, 448, 1, 0, 0, 0, 450, 451, 1, 0, 0, 0, 451, 453, 1, 0, 0, 0, 452,
		454, 7, 3, 0, 0, 453, 452, 1, 0, 0, 0, 453, 454, 1, 0, 0, 0, 454, 147,
		1, 0, 0, 0, 455, 464, 5, 48, 0, 0, 456, 460, 7, 6, 0, 0, 457, 459, 7, 7,
		0, 0, 458, 457, 1, 0, 0, 0, 459, 462, 1, 0, 0, 0, 460, 458, 1, 0, 0, 0,
		460, 461, 1, 0, 0, 0, 461, 464, 1, 0, 0, 0, 462, 460, 1, 0, 0, 0, 463,
		455, 1, 0, 0, 0, 463, 456, 1, 0, 0, 0, 464, 466, 1, 0, 0, 0, 465, 467,
		7, 8, 0, 0, 466, 465, 1, 0, 0, 0, 466, 467, 1, 0, 0, 0, 467, 149, 1, 0,
		0, 0, 468, 477, 5, 48, 0, 0, 469, 473, 7, 6, 0, 0, 470, 472, 7, 7, 0, 0,
		471, 470, 1, 0, 0, 0, 472, 475, 1, 0, 0, 0, 473, 471, 1, 0, 0, 0, 473,
		474, 1, 0, 0, 0, 474, 477, 1, 0, 0, 0, 475, 473, 1, 0, 0, 0, 476, 468,
		1, 0, 0, 0, 476, 469, 1, 0, 0, 0, 477, 484, 1, 0, 0, 0, 478, 480, 3, 18,
		8, 0, 479, 481, 7, 7, 0, 0, 480, 479, 1, 0, 0, 0, 481, 482, 1, 0, 0, 0,
		482, 480, 1, 0, 0, 0, 482, 483, 1, 0, 0, 0, 483, 485, 1, 0, 0, 0, 484,
		478, 1, 0, 0, 0, 484, 485, 1, 0, 0, 0, 485, 495, 1, 0, 0, 0, 486, 488,
		7, 9, 0, 0, 487, 489, 7, 10, 0, 0, 488, 487, 1, 0, 0, 0, 488, 489, 1, 0,
		0, 0, 489, 491, 1, 0, 0, 0, 490, 492, 7, 7, 0, 0, 491, 490, 1, 0, 0, 0,
		492, 493, 1, 0, 0, 0, 493, 491, 1, 0, 0, 0, 493, 494, 1, 0, 0, 0, 494,
		496, 1, 0, 0, 0, 495, 486, 1, 0, 0, 0, 495, 496, 1, 0, 0, 0, 496, 498,
		1, 0, 0, 0, 497, 499, 7, 11, 0, 0, 498, 497, 1, 0, 0, 0, 498, 499, 1, 0,
		0, 0, 499, 151, 1, 0, 0, 0, 500, 508, 5, 34, 0, 0, 501, 502, 5, 92, 0,
		0, 502, 507, 5, 34, 0, 0, 503, 504, 5, 92, 0, 0, 504, 507, 5, 92, 0, 0,
		505, 507, 8, 12, 0, 0, 506, 501, 1, 0, 0, 0, 506, 503, 1, 0, 0, 0, 506,
		505, 1, 0, 0, 0, 507, 510, 1, 0, 0, 0, 508, 509, 1, 0, 0, 0, 508, 506,
		1, 0, 0, 0, 509, 511, 1, 0, 0, 0, 510, 508, 1, 0, 0, 0, 511, 525, 5, 34,
		0, 0, 512, 520, 5, 39, 0, 0, 513, 514, 5, 92, 0, 0, 514, 519, 5, 39, 0,
		0, 515, 516, 5, 92, 0, 0, 516, 519, 5, 92, 0, 0, 517, 519, 8, 13, 0, 0,
		518, 513, 1, 0, 0, 0, 518, 515, 1, 0, 0, 0, 518, 517, 1, 0, 0, 0, 519,
		522, 1, 0, 0, 0, 520, 521, 1, 0, 0, 0, 520, 518, 1, 0, 0, 0, 521, 523,
		1, 0, 0, 0, 522, 520, 1, 0, 0, 0, 523, 525, 5, 39, 0, 0, 524, 500, 1, 0,
		0, 0, 524, 512, 1, 0, 0, 0, 525, 153, 1, 0, 0, 0, 526, 530, 5, 47, 0, 0,
		527, 528, 5, 92, 0, 0, 528, 531, 8, 14, 0, 0, 529, 531, 8, 15, 0, 0, 530,
		527, 1, 0, 0, 0, 530, 529, 1, 0, 0, 0, 531, 532, 1, 0, 0, 0, 532, 533,
		1, 0, 0, 0, 532, 530, 1, 0, 0, 0, 533, 534, 1, 0, 0, 0, 534, 538, 5, 47,
		0, 0, 535, 537, 7, 16, 0, 0, 536, 535, 1, 0, 0, 0, 537, 540, 1, 0, 0, 0,
		538, 536, 1, 0, 0, 0, 538, 539, 1, 0, 0, 0, 539, 541, 1, 0, 0, 0, 540,
		538, 1, 0, 0, 0, 541, 542, 4, 76, 1, 0, 542, 155, 1, 0, 0, 0, 543, 544,
		5, 116, 0, 0, 544, 545, 5, 114, 0, 0, 545, 546, 5, 117, 0, 0, 546, 547,
		5, 101, 0, 0, 547, 157, 1, 0, 0, 0, 548, 549, 5, 102, 0, 0, 549, 550, 5,
		97, 0, 0, 550, 551, 5, 108, 0, 0, 551, 552, 5, 115, 0, 0, 552, 553, 5,
		101, 0, 0, 553, 159, 1, 0, 0, 0, 554, 555, 5, 110, 0, 0, 555, 556, 5, 117,
		0, 0, 556, 557, 5, 108, 0, 0, 557, 558, 5, 108, 0, 0, 558, 161, 1, 0, 0,
		0, 559, 560, 5, 98, 0, 0, 560, 561, 5, 111, 0, 0, 561, 562, 5, 111, 0,
		0, 562, 563, 5, 108, 0, 0, 563, 564, 5, 101, 0, 0, 564, 565, 5, 97, 0,
		0, 565, 598, 5, 110, 0, 0, 566, 567, 5, 98, 0, 0, 567, 568, 5, 121, 0,
		0, 568, 569, 5, 116, 0, 0, 569, 598, 5, 101, 0, 0, 570, 571, 5, 115, 0,
		0, 571, 572, 5, 104, 0, 0, 572, 573, 5, 111, 0, 0, 573, 574, 5, 114, 0,
		0, 574, 598, 5, 116, 0, 0, 575, 576, 5, 99, 0, 0, 576, 577, 5, 104, 0,
		0, 577, 578, 5, 97, 0, 0, 578, 598, 5, 114, 0, 0, 579, 580, 5, 105, 0,
		0, 580, 581, 5, 110, 0, 0, 581, 598, 5, 116, 0, 0, 582, 583, 5, 108, 0,
		0, 583, 584, 5, 111, 0, 0, 584, 585, 5, 110, 0, 0, 585, 598, 5, 103, 0,
		0, 586, 587, 5, 102, 0, 0, 587, 588, 5, 108, 0, 0, 588, 589, 5, 111, 0,
		0, 589, 590, 5, 97, 0, 0, 590, 598, 5, 116, 0, 0, 591, 592, 5, 100, 0,
		0, 592, 593, 5, 111, 0, 0, 593, 594, 5, 117, 0, 0, 594, 595, 5, 98, 0,
		0, 595, 596, 5, 108, 0, 0, 596, 598, 5, 101, 0, 0, 597, 559, 1, 0, 0, 0,
		597, 566, 1, 0, 0, 0, 597, 570, 1, 0, 0, 0, 597, 575, 1, 0, 0, 0, 597,
		579, 1, 0, 0, 0, 597, 582, 1, 0, 0, 0, 597, 586, 1, 0, 0, 0, 597, 591,
		1, 0, 0, 0, 598, 163, 1, 0, 0, 0, 599, 600, 5, 100, 0, 0, 600, 601, 5,
		101, 0, 0, 601, 602, 5, 102, 0, 0, 602, 165, 1, 0, 0, 0, 603, 607, 7, 17,
		0, 0, 604, 606, 7, 18, 0, 0, 605, 604, 1, 0, 0, 0, 606, 609, 1, 0, 0, 0,
		607, 605, 1, 0, 0, 0, 607, 608, 1, 0, 0, 0, 608, 167, 1, 0, 0, 0, 609,
		607, 1, 0, 0, 0, 610, 619, 5, 48, 0, 0, 611, 615, 7, 6, 0, 0, 612, 614,
		7, 7, 0, 0, 613, 612, 1, 0, 0, 0, 614, 617, 1, 0, 0, 0, 615, 613, 1, 0,
		0, 0, 615, 616, 1, 0, 0, 0, 616, 619, 1, 0, 0, 0, 617, 615, 1, 0, 0, 0,
		618, 610, 1, 0, 0, 0, 618, 611, 1, 0, 0, 0, 619, 620, 1, 0, 0, 0, 620,
		621, 6, 83, 2, 0, 621, 169, 1, 0, 0, 0, 622, 626, 7, 17, 0, 0, 623, 625,
		7, 18, 0, 0, 624, 623, 1, 0, 0, 0, 625, 628, 1, 0, 0, 0, 626, 624, 1, 0,
		0, 0, 626, 627, 1, 0, 0, 0, 627, 629, 1, 0, 0, 0, 628, 626, 1, 0, 0, 0,
		629, 630, 6, 84, 2, 0, 630, 171, 1, 0, 0, 0, 34, 0, 1, 175, 185, 194, 199,
		440, 443, 450, 453, 460, 463, 466, 473, 476, 482, 484, 488, 493, 495, 498,
		506, 508, 518, 520, 524, 530, 532, 538, 597, 607, 615, 618, 626, 3, 6,
		0, 0, 2, 1, 0, 2, 0, 0,
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

// PainlessLexerInit initializes any static state used to implement PainlessLexer. By default the
// static state used to implement the lexer is lazily initialized during the first call to
// NewPainlessLexer(). You can call this function if you wish to initialize the static state ahead
// of time.
func PainlessLexerInit() {
	staticData := &PainlessLexerLexerStaticData
	staticData.once.Do(painlesslexerLexerInit)
}

// NewPainlessLexer produces a new lexer instance for the optional input antlr.CharStream.
func NewPainlessLexer(input antlr.CharStream) *PainlessLexer {
	PainlessLexerInit()
	l := new(PainlessLexer)
	l.BaseLexer = antlr.NewBaseLexer(input)
	staticData := &PainlessLexerLexerStaticData
	l.Interpreter = antlr.NewLexerATNSimulator(l, staticData.atn, staticData.decisionToDFA, staticData.PredictionContextCache)
	l.channelNames = staticData.ChannelNames
	l.modeNames = staticData.ModeNames
	l.RuleNames = staticData.RuleNames
	l.LiteralNames = staticData.LiteralNames
	l.SymbolicNames = staticData.SymbolicNames
	l.GrammarFileName = "PainlessLexer.g4"
	// TODO: l.EOF = antlr.TokenEOF

	return l
}

// PainlessLexer tokens.
const (
	PainlessLexerWS         = 1
	PainlessLexerCOMMENT    = 2
	PainlessLexerLBRACK     = 3
	PainlessLexerRBRACK     = 4
	PainlessLexerLBRACE     = 5
	PainlessLexerRBRACE     = 6
	PainlessLexerLP         = 7
	PainlessLexerRP         = 8
	PainlessLexerDOT        = 9
	PainlessLexerNSDOT      = 10
	PainlessLexerCOMMA      = 11
	PainlessLexerSEMICOLON  = 12
	PainlessLexerIF         = 13
	PainlessLexerIN         = 14
	PainlessLexerELSE       = 15
	PainlessLexerWHILE      = 16
	PainlessLexerDO         = 17
	PainlessLexerFOR        = 18
	PainlessLexerCONTINUE   = 19
	PainlessLexerBREAK      = 20
	PainlessLexerRETURN     = 21
	PainlessLexerNEW        = 22
	PainlessLexerTRY        = 23
	PainlessLexerCATCH      = 24
	PainlessLexerTHROW      = 25
	PainlessLexerTHIS       = 26
	PainlessLexerINSTANCEOF = 27
	PainlessLexerBOOLNOT    = 28
	PainlessLexerBWNOT      = 29
	PainlessLexerMUL        = 30
	PainlessLexerDIV        = 31
	PainlessLexerREM        = 32
	PainlessLexerADD        = 33
	PainlessLexerSUB        = 34
	PainlessLexerLSH        = 35
	PainlessLexerRSH        = 36
	PainlessLexerUSH        = 37
	PainlessLexerLT         = 38
	PainlessLexerLTE        = 39
	PainlessLexerGT         = 40
	PainlessLexerGTE        = 41
	PainlessLexerEQ         = 42
	PainlessLexerEQR        = 43
	PainlessLexerNE         = 44
	PainlessLexerNER        = 45
	PainlessLexerBWAND      = 46
	PainlessLexerXOR        = 47
	PainlessLexerBWOR       = 48
	PainlessLexerBOOLAND    = 49
	PainlessLexerBOOLOR     = 50
	PainlessLexerCOND       = 51
	PainlessLexerCOLON      = 52
	PainlessLexerELVIS      = 53
	PainlessLexerREF        = 54
	PainlessLexerARROW      = 55
	PainlessLexerFIND       = 56
	PainlessLexerMATCH      = 57
	PainlessLexerINCR       = 58
	PainlessLexerDECR       = 59
	PainlessLexerASSIGN     = 60
	PainlessLexerAADD       = 61
	PainlessLexerASUB       = 62
	PainlessLexerAMUL       = 63
	PainlessLexerADIV       = 64
	PainlessLexerAREM       = 65
	PainlessLexerAAND       = 66
	PainlessLexerAXOR       = 67
	PainlessLexerAOR        = 68
	PainlessLexerALSH       = 69
	PainlessLexerARSH       = 70
	PainlessLexerAUSH       = 71
	PainlessLexerOCTAL      = 72
	PainlessLexerHEX        = 73
	PainlessLexerINTEGER    = 74
	PainlessLexerDECIMAL    = 75
	PainlessLexerSTRING     = 76
	PainlessLexerREGEX      = 77
	PainlessLexerTRUE       = 78
	PainlessLexerFALSE      = 79
	PainlessLexerNULL       = 80
	PainlessLexerPRIMITIVE  = 81
	PainlessLexerDEF        = 82
	PainlessLexerID         = 83
	PainlessLexerDOTINTEGER = 84
	PainlessLexerDOTID      = 85
)

// PainlessLexerAFTER_DOT is the PainlessLexer mode.
const PainlessLexerAFTER_DOT = 1

/** Is the preceding {@code /} a the beginning of a regex (true) or a division (false). */
func isSlashRegex() bool { return false }

func (l *PainlessLexer) Sempred(localctx antlr.RuleContext, ruleIndex, predIndex int) bool {
	switch ruleIndex {
	case 30:
		return l.DIV_Sempred(localctx, predIndex)

	case 76:
		return l.REGEX_Sempred(localctx, predIndex)

	default:
		panic("No registered predicate for: " + fmt.Sprint(ruleIndex))
	}
}

func (p *PainlessLexer) DIV_Sempred(localctx antlr.RuleContext, predIndex int) bool {
	switch predIndex {
	case 0:
		return isSlashRegex() == false

	default:
		panic("No predicate with index: " + fmt.Sprint(predIndex))
	}
}

func (p *PainlessLexer) REGEX_Sempred(localctx antlr.RuleContext, predIndex int) bool {
	switch predIndex {
	case 1:
		return isSlashRegex()

	default:
		panic("No predicate with index: " + fmt.Sprint(predIndex))
	}
}
