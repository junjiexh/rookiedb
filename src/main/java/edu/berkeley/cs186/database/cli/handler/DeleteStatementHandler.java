package edu.berkeley.cs186.database.cli.handler;

import edu.berkeley.cs186.database.Transaction;
import edu.berkeley.cs186.database.cli.parser.RookieSQLParser.DeleteStmtContext;
import edu.berkeley.cs186.database.cli.parser.RookieSQLParser.ExpressionContext;
import edu.berkeley.cs186.database.query.expr.Expression;
import edu.berkeley.cs186.database.query.expr.ExpressionBuilder;
import edu.berkeley.cs186.database.table.Schema;

import java.io.PrintStream;

class DeleteStatementHandler extends StatementHandler {
    public String tableName;
    private ExpressionContext exprCtx;

    public DeleteStatementHandler(DeleteStmtContext ctx) {
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
