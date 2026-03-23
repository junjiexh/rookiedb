package edu.berkeley.cs186.database.cli.handler;

import edu.berkeley.cs186.database.Transaction;
import edu.berkeley.cs186.database.cli.parser.RookieSQLParser.ExplainStmtContext;
import edu.berkeley.cs186.database.query.QueryPlan;

import java.io.PrintStream;

class ExplainStatementHandler extends StatementHandler {
    StatementHandler visitor;

    public ExplainStatementHandler(ExplainStmtContext ctx) {
        this.visitor = new SelectStatementHandler(ctx.selectStmt());
    }

    @Override
    public void execute(Transaction transaction, PrintStream out) {
        QueryPlan query = this.visitor.getQueryPlan(transaction).get();
        query.execute();
        out.println(query.getFinalOperator());
    }

    @Override
    public StatementType getType() {
        return StatementType.EXPLAIN;
    }
}
