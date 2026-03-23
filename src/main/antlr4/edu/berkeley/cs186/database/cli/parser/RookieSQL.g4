grammar RookieSQL;

// ========== Parser Rules ==========

sqlStmtList
    : SCOL* sqlStmt (SCOL+ sqlStmt)* SCOL+ EOF
    ;

executableStmt
    : (selectStmt | createTableStmt | createIndexStmt | dropTableStmt
       | dropIndexStmt | insertStmt | deleteStmt | updateStmt
       | commitStmt | rollbackStmt | savepointStmt | releaseStmt
       | explainStmt) SCOL?
    ;

sqlStmt
    : selectStmt
    | createTableStmt
    | createIndexStmt
    | dropTableStmt
    | dropIndexStmt
    | insertStmt
    | deleteStmt
    | updateStmt
    | beginStmt
    | commitStmt
    | rollbackStmt
    | savepointStmt
    | releaseStmt
    | explainStmt
    ;

explainStmt : K_EXPLAIN selectStmt ;

dropTableStmt : K_DROP K_TABLE identifier ;

dropIndexStmt : K_DROP K_INDEX identifier OPEN_PAR columnName CLOSE_PAR ;

releaseStmt : K_RELEASE K_SAVEPOINT? identifier ;

savepointStmt : K_SAVEPOINT identifier ;

rollbackStmt : K_ROLLBACK K_TRANSACTION? (K_TO K_SAVEPOINT? identifier)? ;

beginStmt : K_BEGIN K_TRANSACTION? ;

commitStmt : (K_COMMIT | K_END) K_TRANSACTION? ;

insertStmt : K_INSERT K_INTO identifier K_VALUES insertValues (COMMA insertValues)* ;

insertValues : OPEN_PAR literal (COMMA literal)* CLOSE_PAR ;

updateStmt : K_UPDATE identifier K_SET columnName EQ expression (K_WHERE expression)? ;

selectStmt
    : (K_WITH commonTableExpression (COMMA commonTableExpression)*)?
      selectClause
      fromClause
      (K_WHERE columnValueComparison (K_AND columnValueComparison)*)?
      (K_GROUP K_BY columnName (COMMA columnName)*)?
      orderClause?
      limitClause?
    ;

commonTableExpression
    : identifier (OPEN_PAR columnName (COMMA columnName)* CLOSE_PAR)? K_AS OPEN_PAR selectStmt CLOSE_PAR
    ;

deleteStmt : K_DELETE K_FROM identifier K_WHERE expression ;

createTableStmt
    : K_CREATE K_TABLE identifier
      ( OPEN_PAR columnDef (COMMA columnDef)* CLOSE_PAR
      | K_AS selectStmt
      )
    ;

createIndexStmt : K_CREATE K_INDEX K_ON identifier OPEN_PAR columnName CLOSE_PAR ;

columnDef : identifier identifier (OPEN_PAR NUMERIC_LITERAL CLOSE_PAR)? ;

selectClause : K_SELECT selectColumn (COMMA selectColumn)* ;

limitClause : K_LIMIT integerLiteral ;

fromClause : K_FROM aliasedTableName joinedTable* ;

orderClause : K_ORDER K_BY columnName ;

joinedTable : K_INNER? K_JOIN aliasedTableName K_ON columnName (EQ | EQEQ) columnName ;

selectColumn
    : STAR                                          # selectColumnStar
    | IDENTIFIER DOT STAR                           # selectColumnTableStar
    | expression (K_AS identifier)?                 # selectColumnExpr
    ;

columnName : IDENTIFIER (DOT IDENTIFIER)? ;

identifier : IDENTIFIER ;

aliasedTableName : identifier (K_AS identifier)? ;

columnValueComparison
    : columnName comparisonOperator literal         # cvcColumnFirst
    | literal comparisonOperator columnName         # cvcLiteralFirst
    ;

numericLiteral : (PLUS | MINUS)? NUMERIC_LITERAL ;

integerLiteral : numericLiteral ;

literal
    : STRING_LITERAL
    | numericLiteral
    | K_TRUE
    | K_FALSE
    ;

comparisonOperator : EQ | EQEQ | NOT_EQ1 | NOT_EQ2 | LT_EQ | LT | GT_EQ | GT ;

// ========== Expression Rules ==========

expression : orExpr ;

orExpr : andExpr (orOperator andExpr)* ;

andExpr : notExpr (andOperator notExpr)* ;

notExpr : notOperator* comparisonExpr ;

comparisonExpr : additiveExpr (comparisonOperator additiveExpr)* ;

additiveExpr : multiplicativeExpr (additiveOperator multiplicativeExpr)* ;

multiplicativeExpr : primaryExpr (multiplicativeOperator primaryExpr)* ;

primaryExpr
    : literal                                       # primaryLiteral
    | functionCallExpr                              # primaryFunctionCall
    | columnName                                    # primaryColumn
    | OPEN_PAR expression CLOSE_PAR                 # primaryParen
    | additiveOperator primaryExpr                  # primaryUnary
    ;

functionCallExpr
    : identifier OPEN_PAR (STAR | expression (COMMA expression)*)? CLOSE_PAR
    ;

andOperator : K_AND | AND ;
orOperator : K_OR | OR ;
notOperator : K_NOT | NOT ;
multiplicativeOperator : STAR | FSLASH | PERCENT ;
additiveOperator : PLUS | MINUS ;

// ========== Lexer Rules ==========

SCOL     : ';' ;
DOT      : '.' ;
OPEN_PAR : '(' ;
CLOSE_PAR: ')' ;
COMMA    : ',' ;
STAR     : '*' ;
FSLASH   : '/' ;
PERCENT  : '%' ;
PLUS     : '+' ;
MINUS    : '-' ;
LT_EQ    : '<=' ;
GT_EQ    : '>=' ;
LT       : '<' ;
GT       : '>' ;
EQEQ     : '==' ;
EQ       : '=' ;
NOT_EQ1  : '!=' ;
NOT_EQ2  : '<>' ;
NOT      : '!' ;
AND      : '&&' ;
OR       : '||' ;

K_WITH        : W I T H ;
K_AS          : A S ;
K_DELETE      : D E L E T E ;
K_INSERT      : I N S E R T ;
K_INTO        : I N T O ;
K_UPDATE      : U P D A T E ;
K_SET         : S E T ;
K_SELECT      : S E L E C T ;
K_FROM        : F R O M ;
K_INNER       : I N N E R ;
K_JOIN        : J O I N ;
K_ON          : O N ;
K_WHERE       : W H E R E ;
K_AND         : A N D ;
K_OR          : O R ;
K_NOT         : N O T ;
K_GROUP       : G R O U P ;
K_BY          : B Y ;
K_LIMIT       : L I M I T ;
K_CREATE      : C R E A T E ;
K_DROP        : D R O P ;
K_TABLE       : T A B L E ;
K_VALUES      : V A L U E S ;
K_INDEX       : I N D E X ;
K_BEGIN       : B E G I N ;
K_END         : E N D ;
K_COMMIT      : C O M M I T ;
K_TRANSACTION : T R A N S A C T I O N ;
K_SAVEPOINT   : S A V E P O I N T ;
K_ROLLBACK    : R O L L B A C K ;
K_TO          : T O ;
K_RELEASE     : R E L E A S E ;
K_EXPLAIN     : E X P L A I N ;
K_QUERY       : Q U E R Y ;
K_PLAN        : P L A N ;
K_ANALYZE     : A N A L Y Z E ;
K_ORDER       : O R D E R ;
K_TRUE        : T R U E ;
K_FALSE       : F A L S E ;

NUMERIC_LITERAL
    : DIGIT+ ('.' DIGIT*)? ('E' [+\-]? DIGIT+)?
    | '.' DIGIT+ ('E' [+\-]? DIGIT+)?
    ;

STRING_LITERAL : '\'' ( ~'\'' | '\'\'' )* '\'' ;

IDENTIFIER
    : [A-Za-z_] [A-Za-z0-9_]*
    | '"' ( ~[\n\r"] | '""' )+ '"'
    | '`' ( ~[\n\r`] | '``' )+ '`'
    | '[' ~[\n\r\]]+ ']'
    ;

WS : [ \t\r\n]+ -> skip ;

fragment A : [aA] ;
fragment B : [bB] ;
fragment C : [cC] ;
fragment D : [dD] ;
fragment E : [eE] ;
fragment F : [fF] ;
fragment G : [gG] ;
fragment H : [hH] ;
fragment I : [iI] ;
fragment J : [jJ] ;
fragment K : [kK] ;
fragment L : [lL] ;
fragment M : [mM] ;
fragment N : [nN] ;
fragment O : [oO] ;
fragment P : [pP] ;
fragment Q : [qQ] ;
fragment R : [rR] ;
fragment S : [sS] ;
fragment T : [tT] ;
fragment U : [uU] ;
fragment V : [vV] ;
fragment W : [wW] ;
fragment X : [xX] ;
fragment Y : [yY] ;
fragment Z : [zZ] ;
fragment DIGIT : [0-9] ;
