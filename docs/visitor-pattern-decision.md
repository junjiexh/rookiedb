# Visitor Pattern: When to Use and When Not To

本文档记录了 ANTLR4 迁移后，为什么 `ExpressionBuilder` 使用 Visitor 模式而 Handler 类不使用的设计决策。

## 核心判断标准

拿到节点时，**你知不知道它的具体类型？**

| | 知道类型吗 | 有递归吗 | 需要 Visitor 吗 |
|---|---|---|---|
| `primaryExpr` → Expression | 不知道（5 种可能） | 有 | 需要 |
| `SelectStmtContext` → QueryPlan | 知道（就是 SELECT） | 无 | 不需要 |

## ExpressionBuilder — 需要 Visitor

表达式 `sid * 3 + (1 - sid)` 的 parse tree：

```
additiveExpr
├── multiplicativeExpr
│   ├── primaryExpr(column)    ← sid
│   └── primaryExpr(literal)   ← 3
└── multiplicativeExpr
    └── primaryExpr(paren)     ← (1 - sid)
        └── additiveExpr       ← 递归回到 additiveExpr
            ├── primaryExpr(literal)  ← 1
            └── primaryExpr(column)   ← sid
```

处理 `primaryExpr` 时，它可能是 5 种类型中的任何一种：

```antlr
primaryExpr
    : literal                        # primaryLiteral
    | functionCallExpr               # primaryFunctionCall
    | columnName                     # primaryColumn
    | OPEN_PAR expression CLOSE_PAR  # primaryParen
    | additiveOperator primaryExpr   # primaryUnary
    ;
```

而且 `primaryParen` 里套着 `expression`，`expression` 里又会出现 `primaryExpr` — **递归且异构**。

没有 Visitor 的话，你必须写 instanceof 链：

```java
// 手动分派 — 新增 alternative 时编译器不会提醒你
if (ctx instanceof PrimaryLiteralContext litCtx) { ... }
else if (ctx instanceof PrimaryColumnContext colCtx) { ... }
else if (ctx instanceof PrimaryParenContext parenCtx) { ... }
else if (ctx instanceof PrimaryUnaryContext unaryCtx) { ... }
else if (ctx instanceof PrimaryFunctionCallContext fcCtx) { ... }
```

用 Visitor 后，ANTLR4 根据 alternative label 自动分派到对应方法：

```java
class ExpressionBuilder extends RookieSQLBaseVisitor<Expression> {
    @Override public Expression visitPrimaryLiteral(PrimaryLiteralContext ctx) { ... }
    @Override public Expression visitPrimaryColumn(PrimaryColumnContext ctx) { ... }
    @Override public Expression visitPrimaryParen(PrimaryParenContext ctx) { ... }
    // 新增 alternative 时，如果忘了实现，默认返回 null — 容易发现
}
```

## Handler — 不需要 Visitor

`SelectStatementHandler` 收到的**永远是 `SelectStmtContext`**，不会是别的类型。内部结构也是固定的：

```java
public SelectStatementHandler(SelectStmtContext ctx) {
    ctx.selectClause()           // 一定有，类型确定
    ctx.fromClause()             // 一定有，类型确定
    ctx.columnValueComparison()  // 0 或多个，但类型确定
    ctx.orderClause()            // 0 或 1 个，类型确定
    ctx.limitClause()            // 0 或 1 个，类型确定
}
```

**没有异构**：selectClause 就是 selectClause，不会变成 fromClause。

**没有递归**：SELECT 的 FROM 子句里不会再嵌套另一个完整的 SELECT。

就像读一个表单：姓名在第一格，电话在第二格，地址在第三格。每个格子的类型是固定的，不需要分派机制。

如果强行使用 Visitor，反而需要维护遍历过程中的临时状态，代码更分散：

```java
// 强行用 Visitor — 逻辑被拆散到各个 visit 方法中
class InsertStatementHandler extends RookieSQLBaseVisitor<Void> {
    String tableName;
    List<DataBox> currentRow;  // 遍历时的临时状态

    @Override public Void visitIdentifier(IdentifierContext ctx) {
        if (this.tableName == null) this.tableName = ctx.getText();
        return null;
    }
    @Override public Void visitInsertValues(InsertValuesContext ctx) {
        this.currentRow = new ArrayList<>();
        visitChildren(ctx);
        values.add(new Record(currentRow));
        return null;
    }
    @Override public Void visitLiteral(LiteralContext ctx) {
        currentRow.add(PrettyPrinter.parseLiteral(ctx.getText()));
        return null;
    }
}

// 直接提取 — 逻辑集中，更清晰
class InsertStatementHandler extends StatementHandler {
    public InsertStatementHandler(InsertStmtContext ctx) {
        this.tableName = ctx.identifier().getText();
        for (InsertValuesContext v : ctx.insertValues()) {
            List<DataBox> row = new ArrayList<>();
            for (LiteralContext l : v.literal()) {
                row.add(PrettyPrinter.parseLiteral(l.getText()));
            }
            values.add(new Record(row));
        }
    }
}
```

## 总结

Visitor 模式适合**递归 + 异构 + 每层产出结果**的场景。当节点类型固定、结构扁平时，直接读取 context 更简洁。
