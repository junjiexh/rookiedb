package edu.berkeley.cs186.database.cli.handler;

import edu.berkeley.cs186.database.Transaction;
import edu.berkeley.cs186.database.cli.parser.RookieSQLParser.ExpressionContext;
import edu.berkeley.cs186.database.cli.parser.RookieSQLParser.UpdateStmtContext;
import edu.berkeley.cs186.database.databox.BoolDataBox;
import edu.berkeley.cs186.database.query.expr.Expression;
import edu.berkeley.cs186.database.query.expr.ExpressionBuilder;
import edu.berkeley.cs186.database.table.Schema;

import java.io.PrintStream;

public class UpdateStatementHandler extends StatementHandler {
    public String tableName;
    public String updateColumnName;
    private ExpressionContext exprCtx;
    private ExpressionContext condCtx;

    public UpdateStatementHandler(UpdateStmtContext ctx) {
        this.tableName = ctx.identifier().getText();
        this.updateColumnName = ctx.columnName().getText();
        this.exprCtx = ctx.expression(0);
        if (ctx.expression().size() > 1) {
            this.condCtx = ctx.expression(1);
        }
    }

    @Override
    public void execute(Transaction transaction, PrintStream out) {
        try {
            Schema schema = transaction.getSchema(tableName);
            Expression exprFunc = ExpressionBuilder.build(exprCtx);
            Expression condFunc;
            if (this.condCtx == null) {
                condFunc = Expression.literal(new BoolDataBox(true));
            } else {
                condFunc = ExpressionBuilder.build(condCtx);
            }
            exprFunc.setSchema(schema);
            condFunc.setSchema(schema);
            transaction.update(
                    this.tableName,
                    this.updateColumnName,
                    exprFunc::evaluate,
                    condFunc::evaluate
            );
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
