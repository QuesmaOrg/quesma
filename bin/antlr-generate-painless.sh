#!/bin/bash


#
# This is way to build painless AST in our build system
#
# It will be replaces by docker image with all required tools
#
# It requires to install antlr4 and GNU sed
#
# brew install antrl


# clean up
rm -f quesma/queryparser/painless/antlr/*.go
rm -f quesma/queryparser/painless/antlr/*.interp
rm -f quesma/eql/parser/PainlessParser.tokens

antlr -Dlanguage=Go -visitor -listener -package parser quesma/queryparser/painless/antlr/PainlessParser.g4