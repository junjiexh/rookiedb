# ANTLR4 Parser Migration Design

## Goal

Replace the JavaCC/JJTree-based SQL parser with ANTLR4. 1:1 migration — same SQL syntax, no new features. Modernize the parser infrastructure while keeping all existing functionality intact.

## Context

Current parser stack:
- `RookieParser.jjt` → JavaCC generates 61 files into `cli/parser/`
- 17 visitor classes in `cli/visitor/` traverse the AST via `jjtAccept()`/`jjtGetValue()`
- 2 entry points: `CommandLineInterface.java` (multi-statement) and `Database.java` (single statement)
- 2 test files: `TestStatementList.java`, `TestSelectClause.java`

## Approach

Direct replacement — ANTLR4 Visitor mode replaces JJTree Visitor. No adapter layer.

## Design

### 1. Grammar File — `RookieSQL.g4`

Single combined grammar (lexer + parser). Placed at `src/main/antlr4/edu/berkeley/cs186/database/cli/parser/RookieSQL.g4`.

**Parser rules** (camelCase, ANTLR4 convention):
- `sqlStmtList`, `sqlStmt`, `executableStmt` — top-level routing
- `selectStmt`, `insertStmt`, `updateStmt`, `deleteStmt` — DML
- `createTableStmt`, `dropTableStmt`, `createIndexStmt`, `dropIndexStmt` — DDL
- `beginStmt`, `commitStmt`, `rollbackStmt`, `savepointStmt`, `releaseStmt` — transaction control
- `explainStmt` — EXPLAIN
- `expression`, `orExpr`, `andExpr`, `notExpr`, `comparisonExpr`, `additiveExpr`, `multiplicativeExpr`, `primaryExpr` — expression tree
- `literal`, `identifier`, `columnName`, `columnDef`, `aliasedTableName`, etc. — terminals

**Lexer rules**:
- Case-insensitive keywords via fragments (`SELECT: S E L E C T; fragment S: [sS];`)
- Operators: `EQ`, `LT`, `GT`, `PLUS`, `MINUS`, etc.
- Literals: `NUMERIC_LITERAL`, `STRING_LITERAL`, `IDENTIFIER`
- `WS: [ \t\r\n]+ -> skip;`

No LOOKAHEAD needed — ANTLR4's ALL(*) handles ambiguity automatically.

### 2. Visitor Layer Rewrite

All 17 visitors rewrite from `RookieParserVisitor` interface to extending `RookieSQLBaseVisitor<T>`.

Key change: `node.jjtGetValue()` eliminated. ANTLR4's `ctx` object exposes tokens and sub-rules directly (e.g., `ctx.columnName().getText()` instead of `(String) node.jjtGetValue()`).

Complexity breakdown:
- **High**: `SelectStatementVisitor` (JOIN/WHERE/GROUP BY/ORDER/LIMIT/CTE), `ExpressionVisitor` (expression tree)
- **Medium**: `CreateTableStatementVisitor` (column defs), `InsertStatementVisitor` (multi-row VALUES)
- **Low**: 13 others (simple identifier reads + Transaction API calls)

### 3. Entry Point Adaptation

```java
// New pattern (both entry points)
CharStream input = CharStreams.fromStream(stream);
RookieSQLLexer lexer = new RookieSQLLexer(input);
RookieSQLParser parser = new RookieSQLParser(new CommonTokenStream(lexer));
visitor.visit(parser.sqlStmtList());
```

Error handling: `ParseException`/`TokenMgrError` → ANTLR4 `RecognitionException` with custom `ErrorListener`.

### 4. Maven Integration

- Add `antlr4-runtime` dependency
- Add `antlr4-maven-plugin` to generate Lexer/Parser/Visitor base classes at compile time
- Generated code goes to `target/generated-sources/antlr4/`

### 5. File Changes

| Operation | Files |
|-----------|-------|
| Add | `RookieSQL.g4` (~200 lines) |
| Delete | `cli/parser/` 61 generated files |
| Delete | `RookieParser.jjt`, `parsergen.sh` |
| Rewrite | 17 visitors + `ExpressionVisitor` |
| Modify | `CommandLineInterface.java`, `Database.java` (entry adaptation) |
| Modify | `pom.xml` (ANTLR4 dependency + plugin) |
| Update | `TestStatementList.java`, `TestSelectClause.java` |

## Testing

All 326 existing tests must pass after migration. The parser change is transparent to the query engine, index, concurrency, and recovery modules — they are not affected.
