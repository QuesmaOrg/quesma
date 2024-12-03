#!/bin/bash


#
# This is hack in a build system.
#
# It will be replaces by docker image with all required tools
#
# It requires to install antlr4 and GNU sed
#
# brew install antrl
# brew install gsed


# clean up
rm -f quesma/eql/parser/*.go
rm -f quesma/eql/parser/*.interp
rm -f quesma/eql/parser/*.tokens



antlr -Dlanguage=Go -visitor -listener -package parser quesma/eql/parser/EQL.g4

# This is a  bigger HACK even
#
# antlr4 generates code that is not compilable in Go
# https://github.com/antlr/antlr4/pull/4445
#

for i in quesma/eql/parser/*.go
do 
    mv $i $i.orig
    cat $i.orig  | \
    tr '\n' '\f' | \
    gsed "s|return localctx\f\tgoto errorExit // Trick to prevent compiler error if the label is not used|if false {\f          goto errorExit // Trick to prevent compiler error if the label is not used\f    }\f    return localctx|g" | \
    tr '\f' '\n' > $i
    rm $i.orig
done


# sed rule above doesnt format the code well
gofmt -w quesma/eql/parser/