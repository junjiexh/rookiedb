# ANTLR4 Parser Migration Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Replace JavaCC/JJTree parser with ANTLR4, 1:1 migration, all 326 tests passing.

**Architecture:** Write a single combined ANTLR4 grammar `RookieSQL.g4`, generate Lexer/Parser/Visitor base classes via Maven plugin, rewrite all visitor classes to use ANTLR4's `ParseTreeVisitor` API instead of JJTree's `jjtAccept`/`jjtGetValue` pattern.

**Tech Stack:** ANTLR4 4.13.2, antlr4-maven-plugin, Java 25

---

### Task 1: Add ANTLR4 Maven dependencies and plugin

**Files:**
- Modify: `pom.xml`

**Step 1: Add ANTLR4 runtime dependency and maven plugin**

Add to `<dependencies>`:
```xml
<dependency>
    <groupId>org.antlr</groupId>
    <artifactId>antlr4-runtime</artifactId>
    <version>4.13.2</version>
</dependency>
```

Add to `<build><plugins>`:
```xml
<plugin>
    <groupId>org.antlr</groupId>
    <artifactId>antlr4-maven-plugin</artifactId>
    <version>4.13.2</version>
    <executions>
        <execution>
            <goals>
                <goal>antlr4</goal>
            </goals>
            <configuration>
                <visitor>true</visitor>
                <listener>false</listener>
            </configuration>
        </execution>
    </executions>
</plugin>
```

**Step 2: Verify Maven recognizes the plugin**

Run: `mvn validate`
Expected: BUILD SUCCESS

**Step 3: Commit**

```bash
git add pom.xml
git commit -m "feat: add ANTLR4 runtime dependency and maven plugin"
```

---

### Task 2: Write the ANTLR4 grammar file

**Files:**
- Create: `src/main/antlr4/edu/berkeley/cs186/database/cli/parser/RookieSQL.g4`

**Step 1: Create the grammar file**

The grammar must exactly reproduce the syntax defined in `RookieParser.jjt`. Key mapping:

```antlr
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

columnDef : identifier identifier (OPEN_PAR DIGITS CLOSE_PAR)? ;

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

// ========== Expression Rules (operator precedence via nesting) ==========

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

// Symbols
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

// Keywords (case-insensitive)
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

// Literals
NUMERIC_LITERAL
    : DIGIT+ ('.' DIGIT*)? ('E' [+\-]? DIGIT+)?
    | '.' DIGIT+ ('E' [+\-]? DIGIT+)?
    ;

DIGITS : DIGIT+ ;

STRING_LITERAL : '\'' ( ~'\'' | '\'\'' )* '\'' ;

IDENTIFIER
    : [A-Za-z_] [A-Za-z0-9_]*
    | '"' ( ~[\n\r"] | '""' )+ '"'
    | '`' ( ~[\n\r`] | '``' )+ '`'
    | '[' ~[\n\r\]]+ ']'
    ;

// Whitespace
WS : [ \t\r\n]+ -> skip ;

// Case-insensitive character fragments
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
```

**Step 2: Verify ANTLR4 generates code**

Run: `mvn generate-sources`
Expected: BUILD SUCCESS, generated files in `target/generated-sources/antlr4/edu/berkeley/cs186/database/cli/parser/`

**Step 3: Verify generated classes compile**

Run: `mvn compile -pl . -am 2>&1 | tail -5`
Expected: Compilation may have errors from existing code that still imports old parser — that's OK at this stage. The generated ANTLR4 classes themselves should compile.

**Step 4: Commit**

```bash
git add src/main/antlr4/
git commit -m "feat: add ANTLR4 grammar file for SQL parser"
```

---

### Task 3: Rewrite StatementVisitor base class and StatementType

**Files:**
- Modify: `src/main/java/edu/berkeley/cs186/database/cli/visitor/StatementVisitor.java`
- Modify: `src/main/java/edu/berkeley/cs186/database/cli/visitor/StatementType.java` (no change expected, but verify)

**Step 1: Remove JJTree dependency from StatementVisitor**

Current code extends `RookieParserDefaultVisitor`. Change to standalone abstract class:

```java
package edu.berkeley.cs186.database.cli.visitor;

import edu.berkeley.cs186.database.Transaction;
import edu.berkeley.cs186.database.query.QueryPlan;

import java.io.PrintStream;
import java.util.Optional;

abstract class StatementVisitor {
    public void execute(Transaction transaction, PrintStream out) {
        throw new UnsupportedOperationException("Statement is not executable.");
    }

    public Optional<String> getSavepointName() {
        return Optional.empty();
    }

    public Optional<QueryPlan> getQueryPlan(Transaction transaction) {
        return Optional.empty();
    }

    public abstract StatementType getType();
}
```

**Step 2: Commit**

```bash
git add src/main/java/edu/berkeley/cs186/database/cli/visitor/StatementVisitor.java
git commit -m "refactor: decouple StatementVisitor from JJTree"
```

---

### Task 4: Rewrite simple transaction visitors (Begin, Commit, Rollback, Savepoint, Release)

**Files:**
- Modify: `src/main/java/edu/berkeley/cs186/database/cli/visitor/BeginStatementVisitor.java`
- Modify: `src/main/java/edu/berkeley/cs186/database/cli/visitor/CommitStatementVisitor.java`
- Modify: `src/main/java/edu/berkeley/cs186/database/cli/visitor/RollbackStatementVisitor.java`
- Modify: `src/main/java/edu/berkeley/cs186/database/cli/visitor/SavepointStatementVisitor.java`
- Modify: `src/main/java/edu/berkeley/cs186/database/cli/visitor/ReleaseStatementVisitor.java`

**Step 1: Rewrite all five visitors**

These are the simplest — they extend StatementVisitor and accept ANTLR4 context in constructor.

BeginStatementVisitor:
```java
package edu.berkeley.cs186.database.cli.visitor;

class BeginStatementVisitor extends StatementVisitor {
    @Override
    public StatementType getType() {
        return StatementType.BEGIN;
    }
}
```

CommitStatementVisitor:
```java
package edu.berkeley.cs186.database.cli.visitor;

class CommitStatementVisitor extends StatementVisitor {
    @Override
    public StatementType getType() {
        return StatementType.COMMIT;
    }
}
```

RollbackStatementVisitor:
```java
package edu.berkeley.cs186.database.cli.visitor;

import edu.berkeley.cs186.database.cli.parser.RookieSQLParser;

import java.util.Optional;

class RollbackStatementVisitor extends StatementVisitor {
    public String savepointName;

    public RollbackStatementVisitor(RookieSQLParser.RollbackStmtContext ctx) {
        if (ctx.identifier() != null) {
            this.savepointName = ctx.identifier().getText();
        }
    }

    @Override
    public StatementType getType() {
        return StatementType.ROLLBACK;
    }

    @Override
    public Optional<String> getSavepointName() {
        if (savepointName != null) {
            return Optional.of(this.savepointName);
        }
        return Optional.empty();
    }
}
```

SavepointStatementVisitor:
```java
package edu.berkeley.cs186.database.cli.visitor;

import edu.berkeley.cs186.database.Transaction;
import edu.berkeley.cs186.database.cli.parser.RookieSQLParser;

import java.io.PrintStream;

class SavepointStatementVisitor extends StatementVisitor {
    public String savepointName;

    public SavepointStatementVisitor(RookieSQLParser.SavepointStmtContext ctx) {
        this.savepointName = ctx.identifier().getText();
    }

    @Override
    public void execute(Transaction transaction, PrintStream out) {
        transaction.savepoint(savepointName);
    }

    @Override
    public StatementType getType() {
        return StatementType.SAVEPOINT;
    }
}
```

ReleaseStatementVisitor:
```java
package edu.berkeley.cs186.database.cli.visitor;

import edu.berkeley.cs186.database.Transaction;
import edu.berkeley.cs186.database.cli.parser.RookieSQLParser;

import java.io.PrintStream;

class ReleaseStatementVisitor extends StatementVisitor {
    public String savepointName;

    public ReleaseStatementVisitor(RookieSQLParser.ReleaseStmtContext ctx) {
        this.savepointName = ctx.identifier().getText();
    }

    @Override
    public void execute(Transaction transaction, PrintStream out) {
        transaction.releaseSavepoint(savepointName);
    }

    @Override
    public StatementType getType() {
        return StatementType.RELEASE;
    }
}
```

**Step 2: Commit**

```bash
git add src/main/java/edu/berkeley/cs186/database/cli/visitor/
git commit -m "refactor: rewrite transaction control visitors for ANTLR4"
```

---

### Task 5: Rewrite simple DDL/DML visitors (DropTable, CreateIndex, DropIndex, Insert, Delete)

**Files:**
- Modify: `src/main/java/edu/berkeley/cs186/database/cli/visitor/DropTableStatementVisitor.java`
- Modify: `src/main/java/edu/berkeley/cs186/database/cli/visitor/CreateIndexStatementVisitor.java`
- Modify: `src/main/java/edu/berkeley/cs186/database/cli/visitor/DropIndexStatementVisitor.java`
- Modify: `src/main/java/edu/berkeley/cs186/database/cli/visitor/InsertStatementVisitor.java`
- Modify: `src/main/java/edu/berkeley/cs186/database/cli/visitor/DeleteStatementVisitor.java`

**Step 1: Rewrite all five visitors**

DropTableStatementVisitor:
```java
package edu.berkeley.cs186.database.cli.visitor;

import edu.berkeley.cs186.database.Transaction;
import edu.berkeley.cs186.database.cli.parser.RookieSQLParser;

import java.io.PrintStream;

class DropTableStatementVisitor extends StatementVisitor {
    public String tableName;

    public DropTableStatementVisitor(RookieSQLParser.DropTableStmtContext ctx) {
        this.tableName = ctx.identifier().getText();
    }

    @Override
    public void execute(Transaction transaction, PrintStream out) {
        transaction.dropTable(tableName);
        out.println("DROP TABLE " + tableName);
    }

    @Override
    public StatementType getType() {
        return StatementType.DROP_TABLE;
    }
}
```

CreateIndexStatementVisitor:
```java
package edu.berkeley.cs186.database.cli.visitor;

import edu.berkeley.cs186.database.Transaction;
import edu.berkeley.cs186.database.cli.parser.RookieSQLParser;

import java.io.PrintStream;

class CreateIndexStatementVisitor extends StatementVisitor {
    public String tableName;
    public String columnName;

    public CreateIndexStatementVisitor(RookieSQLParser.CreateIndexStmtContext ctx) {
        this.tableName = ctx.identifier().getText();
        this.columnName = ctx.columnName().getText();
    }

    @Override
    public void execute(Transaction transaction, PrintStream out) {
        transaction.createIndex(tableName, columnName, false);
        out.println("CREATE INDEX");
    }

    @Override
    public StatementType getType() {
        return StatementType.CREATE_INDEX;
    }
}
```

DropIndexStatementVisitor:
```java
package edu.berkeley.cs186.database.cli.visitor;

import edu.berkeley.cs186.database.Transaction;
import edu.berkeley.cs186.database.cli.parser.RookieSQLParser;

import java.io.PrintStream;

class DropIndexStatementVisitor extends StatementVisitor {
    public String tableName;
    public String columnName;

    public DropIndexStatementVisitor(RookieSQLParser.DropIndexStmtContext ctx) {
        this.tableName = ctx.identifier().getText();
        this.columnName = ctx.columnName().getText();
    }

    @Override
    public void execute(Transaction transaction, PrintStream out) {
        transaction.dropIndex(tableName, columnName);
        out.println("DROP INDEX");
    }

    @Override
    public StatementType getType() {
        return StatementType.DROP_INDEX;
    }
}
```

InsertStatementVisitor:
```java
package edu.berkeley.cs186.database.cli.visitor;

import edu.berkeley.cs186.database.Transaction;
import edu.berkeley.cs186.database.cli.PrettyPrinter;
import edu.berkeley.cs186.database.cli.parser.RookieSQLParser;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.table.Record;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;

class InsertStatementVisitor extends StatementVisitor {
    public String tableName;
    public List<Record> values = new ArrayList<>();

    public InsertStatementVisitor(RookieSQLParser.InsertStmtContext ctx) {
        this.tableName = ctx.identifier().getText();
        for (RookieSQLParser.InsertValuesContext valuesCtx : ctx.insertValues()) {
            List<DataBox> row = new ArrayList<>();
            for (RookieSQLParser.LiteralContext litCtx : valuesCtx.literal()) {
                row.add(PrettyPrinter.parseLiteral(litCtx.getText()));
            }
            values.add(new Record(row));
        }
    }

    @Override
    public void execute(Transaction transaction, PrintStream out) {
        try {
            for (Record record : values) {
                transaction.insert(this.tableName, record);
            }
            out.println("INSERT");
        } catch (Exception e) {
            out.println(e.getMessage());
            out.println("Failed to execute INSERT.");
        }
    }

    @Override
    public StatementType getType() {
        return StatementType.INSERT;
    }
}
```

DeleteStatementVisitor:
```java
package edu.berkeley.cs186.database.cli.visitor;

import edu.berkeley.cs186.database.Transaction;
import edu.berkeley.cs186.database.cli.parser.RookieSQLParser;
import edu.berkeley.cs186.database.query.expr.Expression;
import edu.berkeley.cs186.database.table.Schema;

import java.io.PrintStream;

class DeleteStatementVisitor extends StatementVisitor {
    public String tableName;
    public RookieSQLParser.ExpressionContext exprCtx;

    public DeleteStatementVisitor(RookieSQLParser.DeleteStmtContext ctx) {
        this.tableName = ctx.identifier().getText();
        this.exprCtx = ctx.expression();
    }

    @Override
    public void execute(Transaction transaction, PrintStream out) {
        try {
            Schema schema = transaction.getSchema(tableName);
            Expression cond = ExpressionBuilder.build(exprCtx);
            cond.setSchema(schema);
            transaction.delete(tableName, cond::evaluate);
            out.println("DELETE");
        } catch (Exception e) {
            out.println(e.getMessage());
            out.println("Failed to execute DELETE.");
        }
    }

    @Override
    public StatementType getType() {
        return StatementType.DELETE;
    }
}
```

**Step 2: Commit**

```bash
git add src/main/java/edu/berkeley/cs186/database/cli/visitor/
git commit -m "refactor: rewrite DDL/DML visitors for ANTLR4"
```

---

### Task 6: Write ExpressionBuilder (replaces ExpressionVisitor)

**Files:**
- Create: `src/main/java/edu/berkeley/cs186/database/query/expr/ExpressionBuilder.java`

**Step 1: Write ExpressionBuilder**

This replaces `ExpressionVisitor` with a direct recursive descent over ANTLR4 context objects. Instead of the visitor pattern with 8 inner classes, use static methods:

```java
package edu.berkeley.cs186.database.query.expr;

import edu.berkeley.cs186.database.cli.PrettyPrinter;
import edu.berkeley.cs186.database.cli.parser.RookieSQLParser;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.databox.IntDataBox;

import java.util.ArrayList;
import java.util.List;

public class ExpressionBuilder {

    public static Expression build(RookieSQLParser.ExpressionContext ctx) {
        return buildOr(ctx.orExpr());
    }

    private static Expression buildOr(RookieSQLParser.OrExprContext ctx) {
        List<RookieSQLParser.AndExprContext> children = ctx.andExpr();
        if (children.size() == 1) return buildAnd(children.get(0));
        Expression[] operands = new Expression[children.size()];
        for (int i = 0; i < children.size(); i++) {
            operands[i] = buildAnd(children.get(i));
        }
        return Expression.or(operands);
    }

    private static Expression buildAnd(RookieSQLParser.AndExprContext ctx) {
        List<RookieSQLParser.NotExprContext> children = ctx.notExpr();
        if (children.size() == 1) return buildNot(children.get(0));
        Expression[] operands = new Expression[children.size()];
        for (int i = 0; i < children.size(); i++) {
            operands[i] = buildNot(children.get(i));
        }
        return Expression.and(operands);
    }

    private static Expression buildNot(RookieSQLParser.NotExprContext ctx) {
        boolean hasNot = ctx.notOperator().size() % 2 == 1;
        Expression expr = buildComparison(ctx.comparisonExpr());
        return hasNot ? Expression.not(expr) : expr;
    }

    private static Expression buildComparison(RookieSQLParser.ComparisonExprContext ctx) {
        List<RookieSQLParser.AdditiveExprContext> operands = ctx.additiveExpr();
        List<RookieSQLParser.ComparisonOperatorContext> operators = ctx.comparisonOperator();
        if (operands.size() == 1) return buildAdditive(operands.get(0));
        Expression prev = buildAdditive(operands.get(0));
        for (int i = 0; i < operators.size(); i++) {
            Expression curr = buildAdditive(operands.get(i + 1));
            prev = Expression.compare(operators.get(i).getText(), prev, curr);
        }
        return prev;
    }

    private static Expression buildAdditive(RookieSQLParser.AdditiveExprContext ctx) {
        List<RookieSQLParser.MultiplicativeExprContext> operands = ctx.multiplicativeExpr();
        List<RookieSQLParser.AdditiveOperatorContext> operators = ctx.additiveOperator();
        if (operands.size() == 1) return buildMultiplicative(operands.get(0));
        Expression[] children = new Expression[operands.size()];
        List<Character> ops = new ArrayList<>();
        for (int i = 0; i < operands.size(); i++) {
            children[i] = buildMultiplicative(operands.get(i));
        }
        for (RookieSQLParser.AdditiveOperatorContext op : operators) {
            ops.add(op.getText().charAt(0));
        }
        return Expression.additive(ops, children);
    }

    private static Expression buildMultiplicative(RookieSQLParser.MultiplicativeExprContext ctx) {
        List<RookieSQLParser.PrimaryExprContext> operands = ctx.primaryExpr();
        List<RookieSQLParser.MultiplicativeOperatorContext> operators = ctx.multiplicativeOperator();
        if (operands.size() == 1) return buildPrimary(operands.get(0));
        Expression[] children = new Expression[operands.size()];
        List<Character> ops = new ArrayList<>();
        for (int i = 0; i < operands.size(); i++) {
            children[i] = buildPrimary(operands.get(i));
        }
        for (RookieSQLParser.MultiplicativeOperatorContext op : operators) {
            ops.add(op.getText().charAt(0));
        }
        return Expression.multiplicative(ops, children);
    }

    private static Expression buildPrimary(RookieSQLParser.PrimaryExprContext ctx) {
        if (ctx instanceof RookieSQLParser.PrimaryLiteralContext litCtx) {
            DataBox value = PrettyPrinter.parseLiteral(litCtx.literal().getText());
            return Expression.literal(value);
        } else if (ctx instanceof RookieSQLParser.PrimaryColumnContext colCtx) {
            return Expression.column(colCtx.columnName().getText());
        } else if (ctx instanceof RookieSQLParser.PrimaryParenContext parenCtx) {
            Expression expr = build(parenCtx.expression());
            expr.needsParentheses = true;
            return expr;
        } else if (ctx instanceof RookieSQLParser.PrimaryUnaryContext unaryCtx) {
            Expression child = buildPrimary(unaryCtx.primaryExpr());
            if (unaryCtx.additiveOperator().getText().equals("-")) {
                return Expression.negate(child);
            }
            return child;
        } else if (ctx instanceof RookieSQLParser.PrimaryFunctionCallContext fcCtx) {
            return buildFunctionCall(fcCtx.functionCallExpr());
        }
        throw new UnsupportedOperationException("Unknown primary expression type");
    }

    private static Expression buildFunctionCall(RookieSQLParser.FunctionCallExprContext ctx) {
        String functionName = ctx.identifier().getText();
        List<Expression> operands = new ArrayList<>();
        for (RookieSQLParser.ExpressionContext exprCtx : ctx.expression()) {
            operands.add(build(exprCtx));
        }

        if (ctx.STAR() != null) {
            // COUNT(*)
            Expression child = Expression.literal(new IntDataBox(0));
            Expression agg = Expression.function(functionName, child);
            if (agg != null) return agg;
        }

        if (operands.size() == 1 || functionName.toUpperCase().equals("COUNT")) {
            Expression child;
            if (operands.size() == 1) child = operands.get(0);
            else child = Expression.literal(new IntDataBox(0));
            Expression agg = Expression.function(functionName, child);
            if (agg != null) return agg;
        }
        Expression[] children = operands.toArray(new Expression[0]);
        Expression reg = Expression.function(functionName, children);
        if (reg != null) return reg;
        throw new UnsupportedOperationException(
            "Unknown function `" + functionName + "` (with " + operands.size() + " argument(s))"
        );
    }
}
```

**Step 2: Commit**

```bash
git add src/main/java/edu/berkeley/cs186/database/query/expr/ExpressionBuilder.java
git commit -m "feat: add ExpressionBuilder for ANTLR4 expression parsing"
```

---

### Task 7: Rewrite medium-complexity visitors (Update, CreateTable, ColumnValueComparison, CommonTableExpression, Explain)

**Files:**
- Modify: `src/main/java/edu/berkeley/cs186/database/cli/visitor/UpdateStatementVisitor.java`
- Modify: `src/main/java/edu/berkeley/cs186/database/cli/visitor/CreateTableStatementVisitor.java`
- Modify: `src/main/java/edu/berkeley/cs186/database/cli/visitor/ColumnValueComparisonVisitor.java`
- Modify: `src/main/java/edu/berkeley/cs186/database/cli/visitor/CommonTableExpressionVisitor.java`
- Modify: `src/main/java/edu/berkeley/cs186/database/cli/visitor/ExplainStatementVisitor.java`

**Step 1: Rewrite UpdateStatementVisitor**

```java
package edu.berkeley.cs186.database.cli.visitor;

import edu.berkeley.cs186.database.Transaction;
import edu.berkeley.cs186.database.cli.parser.RookieSQLParser;
import edu.berkeley.cs186.database.databox.BoolDataBox;
import edu.berkeley.cs186.database.query.expr.Expression;
import edu.berkeley.cs186.database.query.expr.ExpressionBuilder;
import edu.berkeley.cs186.database.table.Schema;

import java.io.PrintStream;

public class UpdateStatementVisitor extends StatementVisitor {
    public String tableName;
    public String updateColumnName;
    public RookieSQLParser.ExpressionContext exprCtx;
    public RookieSQLParser.ExpressionContext condCtx;

    public UpdateStatementVisitor(RookieSQLParser.UpdateStmtContext ctx) {
        this.tableName = ctx.identifier().getText();
        this.updateColumnName = ctx.columnName().getText();
        List<RookieSQLParser.ExpressionContext> expressions = ctx.expression();
        this.exprCtx = expressions.get(0);
        this.condCtx = expressions.size() > 1 ? expressions.get(1) : null;
    }

    @Override
    public void execute(Transaction transaction, PrintStream out) {
        try {
            Schema schema = transaction.getSchema(tableName);
            Expression exprFunc = ExpressionBuilder.build(this.exprCtx);
            Expression condFunc;
            if (this.condCtx == null) {
                condFunc = Expression.literal(new BoolDataBox(true));
            } else {
                condFunc = ExpressionBuilder.build(this.condCtx);
            }
            exprFunc.setSchema(schema);
            condFunc.setSchema(schema);
            transaction.update(this.tableName, this.updateColumnName, exprFunc::evaluate, condFunc::evaluate);
            out.println("UPDATE");
        } catch (Exception e) {
            e.printStackTrace();
            out.println("Failed to execute UPDATE.");
        }
    }

    @Override
    public StatementType getType() {
        return StatementType.UPDATE;
    }
}
```

Note: add `import java.util.List;` at the top.

**Step 2: Rewrite CreateTableStatementVisitor**

```java
package edu.berkeley.cs186.database.cli.visitor;

import edu.berkeley.cs186.database.Transaction;
import edu.berkeley.cs186.database.cli.parser.RookieSQLParser;
import edu.berkeley.cs186.database.databox.Type;
import edu.berkeley.cs186.database.query.QueryOperator;
import edu.berkeley.cs186.database.query.QueryPlan;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.Schema;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

class CreateTableStatementVisitor extends StatementVisitor {
    public String tableName;
    public List<String> errorMessages = new ArrayList<>();
    public Schema schema = new Schema();
    public SelectStatementVisitor selectStatementVisitor = null;

    public CreateTableStatementVisitor(RookieSQLParser.CreateTableStmtContext ctx) {
        this.tableName = ctx.identifier().getText();
        if (ctx.selectStmt() != null) {
            selectStatementVisitor = new SelectStatementVisitor(ctx.selectStmt());
        } else {
            for (RookieSQLParser.ColumnDefContext colDef : ctx.columnDef()) {
                List<RookieSQLParser.IdentifierContext> ids = colDef.identifier();
                String fieldName = ids.get(0).getText();
                String fieldTypeStr = ids.get(1).getText();
                String param = colDef.DIGITS() != null ? colDef.DIGITS().getText() : null;
                Type fieldType = Type.intType();
                switch (fieldTypeStr.toLowerCase()) {
                    case "int":
                    case "integer":
                        fieldType = Type.intType();
                        break;
                    case "char":
                    case "varchar":
                    case "string":
                        if (param == null) {
                            errorMessages.add(String.format("Missing length for %s(n).", fieldType));
                            continue;
                        }
                        if (param.contains(".")) {
                            errorMessages.add(String.format("Length of %s(n) must be integer, not `%s`.", fieldType, param));
                            continue;
                        }
                        fieldType = Type.stringType(Integer.parseInt(param));
                        break;
                    case "float":
                        fieldType = Type.floatType();
                        break;
                    case "long":
                        fieldType = Type.longType();
                        break;
                    case "bool":
                    case "boolean":
                        fieldType = Type.boolType();
                        break;
                    default:
                        assert false : String.format("Invalid field type \"%s\"", fieldTypeStr);
                }
                schema.add(fieldName, fieldType);
            }
        }
    }

    @Override
    public void execute(Transaction transaction, PrintStream out) {
        if (this.errorMessages.size() > 0) {
            for (String msg : errorMessages) {
                out.println(msg);
            }
            out.println("Failed to execute CREATE TABLE.");
        } else {
            if (selectStatementVisitor != null) {
                QueryPlan p = selectStatementVisitor.getQueryPlan(transaction).get();
                p.execute();
                QueryOperator op = p.getFinalOperator();
                Schema s = op.getSchema();
                for (int i = 0; i < s.size(); i++) {
                    if (s.getFieldName(i).contains(".")) {
                        throw new UnsupportedOperationException("Cannot have `.` in table field name.");
                    }
                }
                transaction.createTable(s, this.tableName);
                Iterator<Record> records = op.iterator();
                while (records.hasNext()) {
                    transaction.insert(this.tableName, records.next());
                }
            } else {
                transaction.createTable(this.schema, this.tableName);
            }
            out.println("CREATE TABLE " + tableName);
        }
    }

    @Override
    public StatementType getType() {
        return StatementType.CREATE_TABLE;
    }
}
```

**Step 3: Rewrite ColumnValueComparisonVisitor (now a utility class)**

```java
package edu.berkeley.cs186.database.cli.visitor;

import edu.berkeley.cs186.database.cli.PrettyPrinter;
import edu.berkeley.cs186.database.cli.parser.RookieSQLParser;
import edu.berkeley.cs186.database.common.PredicateOperator;
import edu.berkeley.cs186.database.databox.DataBox;

class ColumnValueComparisonVisitor {
    PredicateOperator op;
    String columnName;
    DataBox value;

    public ColumnValueComparisonVisitor(RookieSQLParser.ColumnValueComparisonContext ctx) {
        if (ctx instanceof RookieSQLParser.CvcColumnFirstContext cfCtx) {
            this.columnName = cfCtx.columnName().getText();
            this.op = PredicateOperator.fromSymbol(cfCtx.comparisonOperator().getText());
            this.value = PrettyPrinter.parseLiteral(cfCtx.literal().getText());
        } else if (ctx instanceof RookieSQLParser.CvcLiteralFirstContext lfCtx) {
            this.columnName = lfCtx.columnName().getText();
            this.op = PredicateOperator.fromSymbol(lfCtx.comparisonOperator().getText()).reverse();
            this.value = PrettyPrinter.parseLiteral(lfCtx.literal().getText());
        }
    }
}
```

**Step 4: Rewrite CommonTableExpressionVisitor**

```java
package edu.berkeley.cs186.database.cli.visitor;

import edu.berkeley.cs186.database.Transaction;
import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.cli.parser.RookieSQLParser;
import edu.berkeley.cs186.database.common.Pair;
import edu.berkeley.cs186.database.query.QueryPlan;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.Schema;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

class CommonTableExpressionVisitor {
    String name = null;
    String alias = null;
    List<String> columns = new ArrayList<>();
    RookieSQLParser.SelectStmtContext selectCtx;
    SelectStatementVisitor visitor = null;

    public CommonTableExpressionVisitor(RookieSQLParser.CommonTableExpressionContext ctx) {
        List<RookieSQLParser.IdentifierContext> ids = new ArrayList<>();
        // First identifier is the CTE name
        this.name = ctx.identifier().getText();
        // Column names from columnName rules
        for (RookieSQLParser.ColumnNameContext colCtx : ctx.columnName()) {
            this.columns.add(colCtx.getText());
        }
        this.selectCtx = ctx.selectStmt();
    }

    public String createTable(Transaction transaction, List<Pair<String, String>> outerContext) {
        this.visitor = new SelectStatementVisitor(this.selectCtx);
        this.visitor.setContext(new ArrayList<>(outerContext));
        QueryPlan p = this.visitor.getQueryPlan(transaction).get();
        p.execute();
        Schema schema = p.getFinalOperator().getSchema();
        if (this.columns.size() != 0) {
            if (schema.size() != this.columns.size()) {
                throw new UnsupportedOperationException(
                    "Number of columns in WITH statement doesn't match number of columns in subquery."
                );
            }
            Schema prev = schema;
            schema = new Schema();
            for (int i = 0; i < prev.size(); i++) {
                schema.add(columns.get(i), prev.getFieldType(i));
            }
        }
        TransactionContext tc = transaction.getTransactionContext();
        this.alias = tc.createTempTable(schema);
        return this.alias;
    }

    public void populateTable(Transaction transaction) {
        Iterator<Record> records = this.visitor.getQueryPlan(transaction).get().execute();
        while (records.hasNext()) {
            transaction.insert(this.alias, records.next());
        }
    }
}
```

**Step 5: Rewrite ExplainStatementVisitor**

```java
package edu.berkeley.cs186.database.cli.visitor;

import edu.berkeley.cs186.database.Transaction;
import edu.berkeley.cs186.database.cli.parser.RookieSQLParser;
import edu.berkeley.cs186.database.query.QueryPlan;

import java.io.PrintStream;

class ExplainStatementVisitor extends StatementVisitor {
    SelectStatementVisitor selectVisitor;

    public ExplainStatementVisitor(RookieSQLParser.ExplainStmtContext ctx) {
        this.selectVisitor = new SelectStatementVisitor(ctx.selectStmt());
    }

    @Override
    public void execute(Transaction transaction, PrintStream out) {
        QueryPlan query = this.selectVisitor.getQueryPlan(transaction).get();
        query.execute();
        out.println(query.getFinalOperator());
    }

    @Override
    public StatementType getType() {
        return StatementType.EXPLAIN;
    }
}
```

**Step 6: Commit**

```bash
git add src/main/java/edu/berkeley/cs186/database/cli/visitor/
git commit -m "refactor: rewrite medium-complexity visitors for ANTLR4"
```

---

### Task 8: Rewrite SelectStatementVisitor (most complex)

**Files:**
- Modify: `src/main/java/edu/berkeley/cs186/database/cli/visitor/SelectStatementVisitor.java`

**Step 1: Rewrite SelectStatementVisitor**

```java
package edu.berkeley.cs186.database.cli.visitor;

import edu.berkeley.cs186.database.Transaction;
import edu.berkeley.cs186.database.cli.PrettyPrinter;
import edu.berkeley.cs186.database.cli.parser.RookieSQLParser;
import edu.berkeley.cs186.database.common.Pair;
import edu.berkeley.cs186.database.common.PredicateOperator;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.query.QueryPlan;
import edu.berkeley.cs186.database.query.expr.Expression;
import edu.berkeley.cs186.database.query.expr.ExpressionBuilder;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.Schema;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

class SelectStatementVisitor extends StatementVisitor {
    List<String> selectColumns = new ArrayList<>();
    List<String> selectAliases = new ArrayList<>();
    List<Expression> selectFunctions = new ArrayList<>();
    List<String> tableNames = new ArrayList<>();
    List<String> tableAliases = new ArrayList<>();
    List<String> joinedTableLeftCols = new ArrayList<>();
    List<String> joinedTableRightCols = new ArrayList<>();
    List<PredicateOperator> predicateOperators = new ArrayList<>();
    List<String> predicateColumns = new ArrayList<>();
    List<DataBox> predicateValues = new ArrayList<>();
    List<String> groupByColumns = new ArrayList<>();
    List<Pair<String, String>> contextAliases = new ArrayList<>();
    List<CommonTableExpressionVisitor> withExpressions = new ArrayList<>();
    String orderColumnName;
    int limit = -1;
    int offset = 0;

    public SelectStatementVisitor(RookieSQLParser.SelectStmtContext ctx) {
        // WITH clause
        for (RookieSQLParser.CommonTableExpressionContext cteCtx : ctx.commonTableExpression()) {
            withExpressions.add(new CommonTableExpressionVisitor(cteCtx));
        }

        // SELECT columns
        RookieSQLParser.SelectClauseContext selectClause = ctx.selectClause();
        for (RookieSQLParser.SelectColumnContext colCtx : selectClause.selectColumn()) {
            if (colCtx instanceof RookieSQLParser.SelectColumnStarContext) {
                selectColumns.add("*");
                selectFunctions.add(null);
                selectAliases.add(null);
            } else if (colCtx instanceof RookieSQLParser.SelectColumnTableStarContext tsCtx) {
                selectColumns.add(tsCtx.IDENTIFIER().getText() + ".*");
                selectFunctions.add(null);
                selectAliases.add(null);
            } else if (colCtx instanceof RookieSQLParser.SelectColumnExprContext exprCtx) {
                Expression expr = ExpressionBuilder.build(exprCtx.expression());
                selectFunctions.add(expr);
                selectColumns.add(expr.toString());
                if (exprCtx.identifier() != null) {
                    selectAliases.add(exprCtx.identifier().getText());
                } else {
                    selectAliases.add(null);
                }
            }
        }

        // FROM clause
        RookieSQLParser.FromClauseContext fromClause = ctx.fromClause();
        processAliasedTableName(fromClause.aliasedTableName());
        for (RookieSQLParser.JoinedTableContext joinCtx : fromClause.joinedTable()) {
            List<RookieSQLParser.ColumnNameContext> joinCols = joinCtx.columnName();
            joinedTableLeftCols.add(joinCols.get(0).getText());
            joinedTableRightCols.add(joinCols.get(1).getText());
            processAliasedTableName(joinCtx.aliasedTableName());
        }

        // WHERE clause
        for (RookieSQLParser.ColumnValueComparisonContext cvcCtx : ctx.columnValueComparison()) {
            ColumnValueComparisonVisitor cvc = new ColumnValueComparisonVisitor(cvcCtx);
            predicateColumns.add(cvc.columnName);
            predicateOperators.add(cvc.op);
            predicateValues.add(cvc.value);
        }

        // GROUP BY
        // The grammar has: (K_GROUP K_BY columnName (COMMA columnName)*)?
        // ctx.columnName() returns column names from GROUP BY clause
        // But we need to distinguish GROUP BY column names from WHERE clause column names.
        // In the grammar, GROUP BY columnNames are direct children of selectStmt.
        List<RookieSQLParser.ColumnNameContext> groupByCols = ctx.columnName();
        for (RookieSQLParser.ColumnNameContext col : groupByCols) {
            groupByColumns.add(col.getText());
        }

        // ORDER BY
        if (ctx.orderClause() != null) {
            orderColumnName = ctx.orderClause().columnName().getText();
        }

        // LIMIT
        if (ctx.limitClause() != null) {
            String numStr = ctx.limitClause().integerLiteral().numericLiteral().getText();
            limit = Integer.parseInt(numStr);
        }
    }

    private void processAliasedTableName(RookieSQLParser.AliasedTableNameContext ctx) {
        List<RookieSQLParser.IdentifierContext> ids = ctx.identifier();
        String tableName = ids.get(0).getText();
        tableNames.add(tableName);
        if (ids.size() > 1) {
            tableAliases.add(ids.get(1).getText());
        } else {
            tableAliases.add(tableName);
        }
    }

    public void setContext(List<Pair<String, String>> context) {
        this.contextAliases = context;
    }

    @Override
    public void execute(Transaction transaction, PrintStream out) {
        QueryPlan query = getQueryPlan(transaction).get();
        Iterator<Record> records = query.execute();
        new PrettyPrinter(out).printRecords(selectColumns, records);
    }

    @Override
    public Optional<QueryPlan> getQueryPlan(Transaction transaction) {
        List<Pair<String, String>> currentAliases = new ArrayList<>(contextAliases);
        for (CommonTableExpressionVisitor visitor : this.withExpressions) {
            String tempTableName = visitor.createTable(transaction, currentAliases);
            currentAliases.add(new Pair<>(tempTableName, visitor.name));
        }

        QueryPlan query = transaction.query(tableNames.get(0), tableAliases.get(0));
        for (Pair<String, String> tempTableName : currentAliases) {
            query.addTempTableAlias(tempTableName.getFirst(), tempTableName.getSecond());
        }
        for (int i = 1; i < tableNames.size(); i++) {
            query.join(
                tableNames.get(i),
                tableAliases.get(i),
                joinedTableLeftCols.get(i - 1),
                joinedTableRightCols.get(i - 1)
            );
        }
        for (int i = 0; i < predicateColumns.size(); i++) {
            query.select(
                predicateColumns.get(i),
                predicateOperators.get(i),
                predicateValues.get(i)
            );
        }
        ArrayList<String> expandedColumns = new ArrayList<>();
        ArrayList<Expression> expandedFunctions = new ArrayList<>();
        ArrayList<String> expandedAliases = new ArrayList<>();
        for (int i = 0; i < selectColumns.size(); i++) {
            String name = selectColumns.get(i);
            if (!name.contains("*") || selectFunctions.get(i) != null) {
                expandedColumns.add(name);
                expandedFunctions.add(selectFunctions.get(i));
                expandedAliases.add(selectAliases.get(i));
            } else if (name.equals("*")) {
                for (String alias : tableAliases) {
                    Schema s = transaction.getSchema(alias);
                    for (String colName : s.getFieldNames()) {
                        String qualifiedName = (tableAliases.size() > 1 ? alias + "." : "") + colName;
                        expandedColumns.add(qualifiedName);
                        expandedFunctions.add(Expression.column(qualifiedName));
                        expandedAliases.add(null);
                    }
                }
            } else {
                String[] s = name.split("\\.", 2);
                s[0] = s[0].trim();
                boolean found = false;
                for (String alias : tableAliases) {
                    if (!alias.toLowerCase().trim().equals(s[0].toLowerCase()))
                        continue;
                    Schema schema = transaction.getSchema(s[0]);
                    found = true;
                    for (String colName : schema.getFieldNames()) {
                        String qualifiedName = s[0] + "." + colName;
                        expandedColumns.add(qualifiedName);
                        expandedFunctions.add(Expression.column(qualifiedName));
                        expandedAliases.add(null);
                    }
                }
                if (!found) throw new UnsupportedOperationException("Unknown table `" + s[0] + "`");
            }
        }
        for (int i = 0; i < expandedColumns.size(); i++) {
            if (expandedAliases.get(i) != null) expandedColumns.set(i, expandedAliases.get(i));
        }
        query.project(expandedColumns, expandedFunctions);
        selectColumns = expandedColumns;
        selectFunctions = expandedFunctions;
        selectAliases = expandedAliases;

        if (groupByColumns.size() > 0) {
            query.groupBy(groupByColumns);
        }
        if (orderColumnName != null) {
            query.sort(orderColumnName);
        }
        query.limit(limit, offset);
        for (CommonTableExpressionVisitor visitor : this.withExpressions) {
            visitor.populateTable(transaction);
        }
        return Optional.of(query);
    }

    @Override
    public StatementType getType() {
        return StatementType.SELECT;
    }
}
```

**Step 2: Commit**

```bash
git add src/main/java/edu/berkeley/cs186/database/cli/visitor/SelectStatementVisitor.java
git commit -m "refactor: rewrite SelectStatementVisitor for ANTLR4"
```

---

### Task 9: Rewrite StatementListVisitor and ExecutableStatementVisitor

**Files:**
- Modify: `src/main/java/edu/berkeley/cs186/database/cli/visitor/StatementListVisitor.java`
- Modify: `src/main/java/edu/berkeley/cs186/database/cli/visitor/ExecutableStatementVisitor.java`

**Step 1: Rewrite StatementListVisitor**

```java
package edu.berkeley.cs186.database.cli.visitor;

import edu.berkeley.cs186.database.Database;
import edu.berkeley.cs186.database.Transaction;
import edu.berkeley.cs186.database.cli.parser.RookieSQLParser;
import edu.berkeley.cs186.database.cli.parser.RookieSQLBaseVisitor;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class StatementListVisitor extends RookieSQLBaseVisitor<Void> {
    private Database database;
    private PrintStream out;
    public List<StatementVisitor> statementVisitors;

    public StatementListVisitor(Database database, PrintStream out) {
        this.database = database;
        this.out = out;
        this.statementVisitors = new ArrayList<>();
    }

    public Transaction execute(Transaction currTransaction) {
        for (StatementVisitor visitor : this.statementVisitors) {
            switch (visitor.getType()) {
                case BEGIN:
                    if (currTransaction != null) {
                        this.out.println("WARNING: Transaction already in progress. Ignoring.");
                    } else {
                        currTransaction = this.database.beginTransaction();
                    }
                    break;
                case COMMIT:
                    if (currTransaction == null) {
                        this.out.println("WARNING: No transaction in progress");
                    } else {
                        currTransaction.commit();
                        currTransaction.close();
                        currTransaction = null;
                    }
                    this.out.println("COMMIT");
                    break;
                case ROLLBACK:
                    if (currTransaction == null) {
                        this.out.println("WARNING: No transaction in progress");
                    } else {
                        Optional<String> savepointName = visitor.getSavepointName();
                        if (savepointName.isPresent()) {
                            try {
                                currTransaction.rollbackToSavepoint(savepointName.get());
                            } catch (Exception e) {
                                e.printStackTrace(this.out);
                                this.out.println("ROLLBACK TO SAVEPOINT failed.");
                                continue;
                            }
                        } else {
                            currTransaction.rollback();
                            currTransaction = null;
                        }
                    }
                    this.out.println("ROLLBACK");
                    break;
                default:
                    if (currTransaction == null) {
                        try (Transaction tmp = database.beginTransaction()) {
                            visitor.execute(tmp, out);
                            tmp.commit();
                        } catch (Exception e) {
                            e.printStackTrace(this.out);
                            this.out.println("Operation failed.");
                        }
                    } else {
                        try {
                            visitor.execute(currTransaction, out);
                        } catch (Exception e) {
                            e.printStackTrace(this.out);
                            this.out.println("Operation failed.");
                        }
                    }
                    break;
            }
        }
        return currTransaction;
    }

    @Override
    public Void visitSqlStmt(RookieSQLParser.SqlStmtContext ctx) {
        StatementVisitor visitor = createVisitorForStatement(ctx);
        if (visitor != null) {
            this.statementVisitors.add(visitor);
        }
        return null;
    }

    private StatementVisitor createVisitorForStatement(RookieSQLParser.SqlStmtContext ctx) {
        if (ctx.selectStmt() != null) {
            try {
                return new SelectStatementVisitor(ctx.selectStmt());
            } catch (UnsupportedOperationException e) {
                this.out.println("Failed to execute SELECT");
                this.out.println(e.getMessage());
                return null;
            }
        } else if (ctx.createTableStmt() != null) {
            return new CreateTableStatementVisitor(ctx.createTableStmt());
        } else if (ctx.dropTableStmt() != null) {
            return new DropTableStatementVisitor(ctx.dropTableStmt());
        } else if (ctx.createIndexStmt() != null) {
            return new CreateIndexStatementVisitor(ctx.createIndexStmt());
        } else if (ctx.dropIndexStmt() != null) {
            return new DropIndexStatementVisitor(ctx.dropIndexStmt());
        } else if (ctx.insertStmt() != null) {
            return new InsertStatementVisitor(ctx.insertStmt());
        } else if (ctx.deleteStmt() != null) {
            return new DeleteStatementVisitor(ctx.deleteStmt());
        } else if (ctx.updateStmt() != null) {
            return new UpdateStatementVisitor(ctx.updateStmt());
        } else if (ctx.beginStmt() != null) {
            return new BeginStatementVisitor();
        } else if (ctx.commitStmt() != null) {
            return new CommitStatementVisitor();
        } else if (ctx.rollbackStmt() != null) {
            return new RollbackStatementVisitor(ctx.rollbackStmt());
        } else if (ctx.savepointStmt() != null) {
            return new SavepointStatementVisitor(ctx.savepointStmt());
        } else if (ctx.releaseStmt() != null) {
            return new ReleaseStatementVisitor(ctx.releaseStmt());
        } else if (ctx.explainStmt() != null) {
            return new ExplainStatementVisitor(ctx.explainStmt());
        }
        return null;
    }
}
```

**Step 2: Rewrite ExecutableStatementVisitor**

```java
package edu.berkeley.cs186.database.cli.visitor;

import edu.berkeley.cs186.database.Transaction;
import edu.berkeley.cs186.database.cli.parser.RookieSQLParser;
import edu.berkeley.cs186.database.query.QueryPlan;

import java.io.PrintStream;
import java.util.Optional;

public class ExecutableStatementVisitor {
    StatementVisitor visitor;

    public ExecutableStatementVisitor(RookieSQLParser.ExecutableStmtContext ctx) {
        if (ctx.selectStmt() != null) {
            this.visitor = new SelectStatementVisitor(ctx.selectStmt());
        } else if (ctx.createTableStmt() != null) {
            this.visitor = new CreateTableStatementVisitor(ctx.createTableStmt());
        } else if (ctx.dropTableStmt() != null) {
            this.visitor = new DropTableStatementVisitor(ctx.dropTableStmt());
        } else if (ctx.createIndexStmt() != null) {
            this.visitor = new CreateIndexStatementVisitor(ctx.createIndexStmt());
        } else if (ctx.dropIndexStmt() != null) {
            this.visitor = new DropIndexStatementVisitor(ctx.dropIndexStmt());
        } else if (ctx.insertStmt() != null) {
            this.visitor = new InsertStatementVisitor(ctx.insertStmt());
        } else if (ctx.deleteStmt() != null) {
            this.visitor = new DeleteStatementVisitor(ctx.deleteStmt());
        } else if (ctx.updateStmt() != null) {
            this.visitor = new UpdateStatementVisitor(ctx.updateStmt());
        } else if (ctx.commitStmt() != null) {
            this.visitor = new CommitStatementVisitor();
        } else if (ctx.rollbackStmt() != null) {
            this.visitor = new RollbackStatementVisitor(ctx.rollbackStmt());
        } else if (ctx.savepointStmt() != null) {
            this.visitor = new SavepointStatementVisitor(ctx.savepointStmt());
        } else if (ctx.releaseStmt() != null) {
            this.visitor = new ReleaseStatementVisitor(ctx.releaseStmt());
        } else if (ctx.explainStmt() != null) {
            this.visitor = new ExplainStatementVisitor(ctx.explainStmt());
        }
    }

    public Optional<QueryPlan> execute(Transaction transaction, PrintStream out) {
        Optional<QueryPlan> qp = visitor.getQueryPlan(transaction);
        if (qp.isPresent()) return qp;
        visitor.execute(transaction, out);
        return Optional.empty();
    }
}
```

**Step 3: Commit**

```bash
git add src/main/java/edu/berkeley/cs186/database/cli/visitor/
git commit -m "refactor: rewrite StatementListVisitor and ExecutableStatementVisitor for ANTLR4"
```

---

### Task 10: Update entry points (CommandLineInterface, Database)

**Files:**
- Modify: `src/main/java/edu/berkeley/cs186/database/cli/CommandLineInterface.java`
- Modify: `src/main/java/edu/berkeley/cs186/database/Database.java`

**Step 1: Update CommandLineInterface.java**

Replace parser imports and the parsing block (lines 7-10 → new imports, lines 102-117 → new parsing code):

Old imports:
```java
import edu.berkeley.cs186.database.cli.parser.ASTSQLStatementList;
import edu.berkeley.cs186.database.cli.parser.ParseException;
import edu.berkeley.cs186.database.cli.parser.RookieParser;
import edu.berkeley.cs186.database.cli.parser.TokenMgrError;
```

New imports:
```java
import edu.berkeley.cs186.database.cli.parser.RookieSQLLexer;
import edu.berkeley.cs186.database.cli.parser.RookieSQLParser;
import org.antlr.v4.runtime.*;
```

Old parsing block (lines 102-117):
```java
ByteArrayInputStream stream = new ByteArrayInputStream(input.getBytes(StandardCharsets.UTF_8));
RookieParser parser = new RookieParser(stream);
ASTSQLStatementList node;
try {
    node = parser.sql_stmt_list();
} catch (ParseException | TokenMgrError e) {
    this.out.println("Parser exception: " + e.getMessage());
    continue;
}
StatementListVisitor visitor = new StatementListVisitor(db, this.out);
try {
    node.jjtAccept(visitor, null);
    currTransaction = visitor.execute(currTransaction);
} catch (DatabaseException e) {
    this.out.println("Database exception: " + e.getMessage());
}
```

New parsing block:
```java
CharStream charStream = CharStreams.fromString(input);
RookieSQLLexer lexer = new RookieSQLLexer(charStream);
CommonTokenStream tokens = new CommonTokenStream(lexer);
RookieSQLParser parser = new RookieSQLParser(tokens);
parser.removeErrorListeners();
parser.addErrorListener(new BaseErrorListener() {
    @Override
    public void syntaxError(Recognizer<?, ?> recognizer, Object offendingSymbol,
                            int line, int charPositionInLine, String msg,
                            RecognitionException e) {
        throw new RuntimeException("line " + line + ":" + charPositionInLine + " " + msg);
    }
});
RookieSQLParser.SqlStmtListContext tree;
try {
    tree = parser.sqlStmtList();
} catch (RuntimeException e) {
    this.out.println("Parser exception: " + e.getMessage());
    continue;
}
StatementListVisitor visitor = new StatementListVisitor(db, this.out);
try {
    visitor.visit(tree);
    currTransaction = visitor.execute(currTransaction);
} catch (DatabaseException e) {
    this.out.println("Database exception: " + e.getMessage());
}
```

Also remove unused import: `import java.io.InputStream;` can stay (used for `this.in`).

**Step 2: Update Database.java**

Find the `execute(String statement)` method (~line 1026) and replace:

Old:
```java
import edu.berkeley.cs186.database.cli.parser.ASTExecutableStatement;
import edu.berkeley.cs186.database.cli.parser.ParseException;
import edu.berkeley.cs186.database.cli.parser.RookieParser;
```

New:
```java
import edu.berkeley.cs186.database.cli.parser.RookieSQLLexer;
import edu.berkeley.cs186.database.cli.parser.RookieSQLParser;
import org.antlr.v4.runtime.*;
```

Old method body:
```java
RookieParser parser = new RookieParser(new ByteArrayInputStream(statement.getBytes()));
ASTExecutableStatement stmt;
try {
    stmt = parser.executable_stmt();
} catch (ParseException p) {
    throw new DatabaseException(p.getMessage());
}
ExecutableStatementVisitor visitor = new ExecutableStatementVisitor();
stmt.jjtAccept(visitor, null);
Optional<QueryPlan> qp = visitor.execute(this, System.out);
return qp;
```

New method body:
```java
CharStream charStream = CharStreams.fromString(statement);
RookieSQLLexer lexer = new RookieSQLLexer(charStream);
CommonTokenStream tokens = new CommonTokenStream(lexer);
RookieSQLParser parser = new RookieSQLParser(tokens);
parser.removeErrorListeners();
parser.addErrorListener(new BaseErrorListener() {
    @Override
    public void syntaxError(Recognizer<?, ?> recognizer, Object offendingSymbol,
                            int line, int charPositionInLine, String msg,
                            RecognitionException e) {
        throw new DatabaseException("line " + line + ":" + charPositionInLine + " " + msg);
    }
});
RookieSQLParser.ExecutableStmtContext stmt = parser.executableStmt();
ExecutableStatementVisitor visitor = new ExecutableStatementVisitor(stmt);
Optional<QueryPlan> qp = visitor.execute(this, System.out);
return qp;
```

**Step 3: Commit**

```bash
git add src/main/java/edu/berkeley/cs186/database/cli/CommandLineInterface.java src/main/java/edu/berkeley/cs186/database/Database.java
git commit -m "refactor: update parser entry points to use ANTLR4"
```

---

### Task 11: Update tests

**Files:**
- Modify: `src/test/java/edu/berkeley/cs186/database/cli/visitor/TestStatementList.java`
- Modify: `src/test/java/edu/berkeley/cs186/database/cli/visitor/TestSelectClause.java`

**Step 1: Update TestStatementList.java**

Replace the `parse` method:

Old:
```java
import edu.berkeley.cs186.database.cli.parser.ASTSQLStatementList;
import edu.berkeley.cs186.database.cli.parser.ParseException;
import edu.berkeley.cs186.database.cli.parser.RookieParser;
```

New:
```java
import edu.berkeley.cs186.database.cli.parser.RookieSQLLexer;
import edu.berkeley.cs186.database.cli.parser.RookieSQLParser;
import org.antlr.v4.runtime.*;
```

Old parse method:
```java
public StatementListVisitor parse(String input) {
    RookieParser p = new RookieParser(new ByteArrayInputStream(input.getBytes()));
    ASTSQLStatementList node;
    try {
        node = p.sql_stmt_list();
    } catch (ParseException e) {
        throw new RuntimeException(e.getMessage());
    }
    StatementListVisitor visitor = new StatementListVisitor(db, System.out);
    node.jjtAccept(visitor, null);
    return visitor;
}
```

New parse method:
```java
public StatementListVisitor parse(String input) {
    CharStream charStream = CharStreams.fromString(input);
    RookieSQLLexer lexer = new RookieSQLLexer(charStream);
    CommonTokenStream tokens = new CommonTokenStream(lexer);
    RookieSQLParser parser = new RookieSQLParser(tokens);
    StatementListVisitor visitor = new StatementListVisitor(db, System.out);
    visitor.visit(parser.sqlStmtList());
    return visitor;
}
```

Remove unused import `ByteArrayInputStream`.

**Step 2: Update TestSelectClause.java**

Same pattern — replace imports and parse method. Also remove unused `ByteArrayInputStream` import.

New parse method:
```java
public SelectStatementVisitor parse(String input) {
    CharStream charStream = CharStreams.fromString(input);
    RookieSQLLexer lexer = new RookieSQLLexer(charStream);
    CommonTokenStream tokens = new CommonTokenStream(lexer);
    RookieSQLParser parser = new RookieSQLParser(tokens);
    StatementListVisitor visitor = new StatementListVisitor(db, System.out);
    visitor.visit(parser.sqlStmtList());
    return (SelectStatementVisitor) visitor.statementVisitors.get(0);
}
```

**Step 3: Commit**

```bash
git add src/test/java/edu/berkeley/cs186/database/cli/visitor/
git commit -m "refactor: update parser tests for ANTLR4"
```

---

### Task 12: Delete old JavaCC files

**Files:**
- Delete: `src/main/java/edu/berkeley/cs186/database/cli/parser/` (all 61 files)
- Delete: `RookieParser.jjt`
- Delete: `parsergen.sh`
- Delete: `src/main/java/edu/berkeley/cs186/database/query/expr/ExpressionVisitor.java`

**Step 1: Remove old parser files**

```bash
git rm -r src/main/java/edu/berkeley/cs186/database/cli/parser/
git rm RookieParser.jjt
git rm parsergen.sh
git rm src/main/java/edu/berkeley/cs186/database/query/expr/ExpressionVisitor.java
```

**Step 2: Commit**

```bash
git commit -m "chore: remove old JavaCC parser and ExpressionVisitor"
```

---

### Task 13: Build and fix compilation errors

**Step 1: Run compilation**

Run: `mvn compile 2>&1`
Expected: May encounter compilation errors due to mismatches between grammar and visitor code.

**Step 2: Fix any errors iteratively**

Common issues to watch for:
- ANTLR4 generated class names not matching what visitors reference
- `selectColumn` alternatives may need label adjustments in the grammar
- `DIGITS` token may conflict with `NUMERIC_LITERAL` — may need to adjust lexer rule ordering
- `IDENTIFIER` matching keywords — ANTLR4 handles this differently from JavaCC (longest match + rule order)

**Step 3: Run compilation again**

Run: `mvn compile 2>&1`
Expected: BUILD SUCCESS

**Step 4: Commit**

```bash
git add -A
git commit -m "fix: resolve ANTLR4 compilation issues"
```

---

### Task 14: Run all tests and fix failures

**Step 1: Run all tests**

Run: `mvn test -P all 2>&1 | tail -30`
Expected: Tests run: 326, Failures: 0, Errors: 0

**Step 2: If tests fail, debug**

Common issues:
- Literal text extraction: `literal().getText()` may include surrounding quotes for STRING_LITERAL — check if `PrettyPrinter.parseLiteral()` handles this
- Case sensitivity: ANTLR4 keywords are case-insensitive but `getText()` preserves original case — check if visitor code does case-insensitive comparison where needed
- Token ordering: `NUMERIC_LITERAL` vs `DIGITS` — ensure the lexer doesn't eat digit-only tokens as NUMERIC_LITERAL when DIGITS is expected (e.g., in `columnDef`)

**Step 3: Fix and re-run until all 326 tests pass**

Run: `mvn test -P all 2>&1 | tail -30`
Expected: Tests run: 326, Failures: 0, Errors: 0, Skipped: 0

**Step 4: Commit**

```bash
git add -A
git commit -m "fix: all 326 tests passing with ANTLR4 parser"
```

---

### Task 15: Final cleanup and verification

**Step 1: Verify no references to old parser remain**

Run: `grep -r "RookieParser\|jjtAccept\|jjtGetValue\|RookieParserDefaultVisitor\|RookieParserVisitor" src/ --include="*.java"`
Expected: No matches

**Step 2: Verify clean build from scratch**

Run: `mvn clean test -P all 2>&1 | tail -30`
Expected: Tests run: 326, Failures: 0, Errors: 0, Skipped: 0

**Step 3: Commit**

```bash
git add -A
git commit -m "chore: final cleanup of ANTLR4 parser migration"
```
