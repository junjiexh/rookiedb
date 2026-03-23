package edu.berkeley.cs186.database.cli.visitor;

import edu.berkeley.cs186.database.Transaction;
import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.cli.parser.RookieSQLParser.ColumnNameContext;
import edu.berkeley.cs186.database.cli.parser.RookieSQLParser.CommonTableExpressionContext;
import edu.berkeley.cs186.database.cli.parser.RookieSQLParser.SelectStmtContext;
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
    SelectStmtContext selectStmtCtx = null;
    SelectStatementVisitor visitor = null;

    public CommonTableExpressionVisitor(CommonTableExpressionContext ctx) {
        this.name = ctx.identifier().getText();
        for (ColumnNameContext colName : ctx.columnName()) {
            this.columns.add(colName.getText());
        }
        this.selectStmtCtx = ctx.selectStmt();
    }

    public String createTable(Transaction transaction, List<Pair<String, String>> outerContext) {
        this.visitor = new SelectStatementVisitor(this.selectStmtCtx);
        this.visitor.setContext(new ArrayList<>(outerContext));
        QueryPlan p = this.visitor.getQueryPlan(transaction).get();
        p.execute();
        Schema schema = p.getFinalOperator().getSchema();
        if (this.columns.size() != 0) {
            if (schema.size() != this.columns.size()) {
                throw new UnsupportedOperationException("Number of columns in WITH statement doesn't match number of columns in subquery.");
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
