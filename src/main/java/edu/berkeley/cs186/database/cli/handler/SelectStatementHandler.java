package edu.berkeley.cs186.database.cli.handler;

import edu.berkeley.cs186.database.Transaction;
import edu.berkeley.cs186.database.cli.PrettyPrinter;
import edu.berkeley.cs186.database.cli.parser.RookieSQLParser.*;
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

class SelectStatementHandler extends StatementHandler {
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
    List<CommonTableExpressionHandler> withExpressions = new ArrayList<>();
    String orderColumnName;
    int limit = -1;
    int offset = 0;

    public SelectStatementHandler(SelectStmtContext ctx) {
        // WITH clause
        for (CommonTableExpressionContext cteCtx : ctx.commonTableExpression()) {
            withExpressions.add(new CommonTableExpressionHandler(cteCtx));
        }

        // SELECT clause
        for (SelectColumnContext selCol : ctx.selectClause().selectColumn()) {
            if (selCol instanceof SelectColumnStarContext) {
                selectColumns.add("*");
                selectFunctions.add(null);
                selectAliases.add(null);
            } else if (selCol instanceof SelectColumnTableStarContext tableStarCtx) {
                String tableName = tableStarCtx.IDENTIFIER().getText();
                selectColumns.add(tableName + ".*");
                selectFunctions.add(null);
                selectAliases.add(null);
            } else if (selCol instanceof SelectColumnExprContext exprCtx) {
                Expression exp = ExpressionBuilder.build(exprCtx.expression());
                selectFunctions.add(exp);
                selectColumns.add(exp.toString());
                if (exprCtx.K_AS() != null) {
                    selectAliases.add(exprCtx.identifier().getText());
                } else {
                    selectAliases.add(null);
                }
            }
        }

        // FROM clause
        AliasedTableNameContext firstTable = ctx.fromClause().aliasedTableName();
        processAliasedTable(firstTable);

        for (JoinedTableContext joinCtx : ctx.fromClause().joinedTable()) {
            joinedTableLeftCols.add(joinCtx.columnName(0).getText());
            joinedTableRightCols.add(joinCtx.columnName(1).getText());
            processAliasedTable(joinCtx.aliasedTableName());
        }

        // WHERE clause
        for (ColumnValueComparisonContext cvcCtx : ctx.columnValueComparison()) {
            ColumnValueComparisonHandler cvcHandler = new ColumnValueComparisonHandler(cvcCtx);
            predicateColumns.add(cvcHandler.columnName);
            predicateOperators.add(cvcHandler.op);
            predicateValues.add(cvcHandler.value);
        }

        // GROUP BY clause
        for (ColumnNameContext colName : ctx.columnName()) {
            groupByColumns.add(colName.getText());
        }

        // ORDER BY clause
        if (ctx.orderClause() != null) {
            orderColumnName = ctx.orderClause().columnName().getText();
        }

        // LIMIT clause
        if (ctx.limitClause() != null) {
            limit = Integer.parseInt(ctx.limitClause().integerLiteral().numericLiteral().getText());
        }
    }

    private void processAliasedTable(AliasedTableNameContext atn) {
        String name = atn.identifier(0).getText();
        tableNames.add(name);
        if (atn.identifier().size() > 1) {
            tableAliases.add(atn.identifier(1).getText());
        } else {
            tableAliases.add(name);
        }
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
        for (CommonTableExpressionHandler cteHandler: this.withExpressions) {
            String tempTableName = cteHandler.createTable(transaction, currentAliases);
            currentAliases.add(new Pair<>(tempTableName, cteHandler.name));
        }

        QueryPlan query = transaction.query(tableNames.get(0), tableAliases.get(0));
        for (Pair<String, String> tempTableName: currentAliases) {
            query.addTempTableAlias(tempTableName.getFirst(), tempTableName.getSecond());
        }
        for(int i = 1; i < tableNames.size(); i++) {
            query.join(
                tableNames.get(i),
                tableAliases.get(i),
                joinedTableLeftCols.get(i-1),
                joinedTableRightCols.get(i-1)
            );
        }
        for(int i = 0; i < predicateColumns.size(); i++) {
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
                for(String alias: tableAliases) {
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
                for(String alias: tableAliases) {
                    if (!alias.toLowerCase().trim().equals(s[0].toLowerCase()))
                        continue;
                    Schema schema = transaction.getSchema(s[0]);
                    found=true;
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
        for (CommonTableExpressionHandler cteHandler: this.withExpressions) {
            cteHandler.populateTable(transaction);
        }
        return Optional.of(query);
    }

    public void setContext(List<Pair<String, String>> context) {
        this.contextAliases = context;
    }

    @Override
    public StatementType getType() {
        return StatementType.SELECT;
    }
}
