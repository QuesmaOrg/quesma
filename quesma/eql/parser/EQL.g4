grammar EQL;

query: ( simpleQuery
     | sequenceQuery
     | sampleQuery ) EOF
    ;

simpleQuery: category 'where' condition
// TODO add support for pipe opertor  '|'
;

sequenceQuery: 'sequence' ( 'by' fieldList )? ( 'with' 'maxspan' '=' interval )?
        ( '[' simpleQuery ']' ('by' fieldList)?  )+
      ;

sampleQuery: 'sample' 'by' fieldList
    ( '[' simpleQuery ']' )+
    ;

condition: BOOLEAN #ConditionBoolean
    | 'not' condition #ConditionNot
    | '(' condition ')' #ConditionGroup
    | left=value op=('==' | '!=' | '>' | '<'  | '>=' | '<=' | ':' | 'like' | 'like~' | 'regex' | 'regex~' ) right=value #ComparisonOp
    | field 'not' op=('in' | 'in~')   list=literalList #LookupNotOpList
    | field op=(':' | 'in' | 'in~'  | 'like' | 'like~' | 'regex' | 'regex~') list=literalList #LookupOpList
    | left=condition op=('and' | 'or') right=condition #ConditionLogicalOp
    | funcall #ConditionFuncall
    | 'not' funcall #ConditionNotFuncall
;


category
       : ANY
       | ID
       | STRING
       ;

field: ID;
// TODO add optional field names: `?field_name`
// TODO add backtick escape for field names

fieldList : field (',' field)*;

// TODO add floats
literal: STRING | NUMBER | BOOLEAN;
literalList: '(' literal (',' literal)* ')';

value:
     'null'   #ValueNull
    | literal #ValueLiteral
    | field    #ValueField
    | funcall  #ValueFuncall
    | '(' value ')' #ValueGroup
    | left=value op=('*' | '/' |  '%') right=value #ValueMulDiv
    | left=value op=('+' | '-')  right=value #ValueAddSub
;



funcall: funcName '(' value (',' value)* ')';
funcName: ID | ID '~';


interval: INTERVAL;

ANY: 'any';

MULTILINE_COMMENT: '/*' .*? '*/' -> channel(HIDDEN);
ONELINE_COMMNET: '//' ~[\r\n]* -> channel(HIDDEN);
BOOLEAN: 'true' | 'false';
INTERVAL: [0-9]+[a-z];
NUMBER: [0-9]+;

ESC: '\\' .;
STRING: '"' ('\\' . | '""' | ~["\\])*  '"' | '"""' .*? '"""';

WS: [ \t\n\r\f]+ -> skip ;

ID: [a-zA-Z_][.a-zA-Z0-9_-]*;
