
This package contains:
- PainlessParser.g4: The ANTLR4 grammar for Painless from OpenSearch
- *.go: generated parser code - do not edit nor review this code


Code generation is done by running `./bin/anltrl-genereate-painless.sh` from the root of the project.
Script requires ANTLR4 and GNU sed to be installed on the system. To install them on MacOS run:
```
brew install antrl
```