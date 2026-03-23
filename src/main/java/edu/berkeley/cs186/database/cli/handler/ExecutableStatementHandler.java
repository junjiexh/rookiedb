package edu.berkeley.cs186.database.cli.handler;

import edu.berkeley.cs186.database.Transaction;
import edu.berkeley.cs186.database.cli.parser.RookieSQLParser.ExecutableStmtContext;
import edu.berkeley.cs186.database.query.QueryPlan;

import java.io.PrintStream;
import java.util.Optional;

public class ExecutableStatementHandler {
    StatementHandler visitor = null;

    public ExecutableStatementHandler(ExecutableStmtContext ctx) {
        if (ctx.selectStmt() != null) {
            this.visitor = new SelectStatementHandler(ctx.selectStmt());
        } else if (ctx.createTableStmt() != null) {
            this.visitor = new CreateTableStatementHandler(ctx.createTableStmt());
        } else if (ctx.createIndexStmt() != null) {
            this.visitor = new CreateIndexStatementHandler(ctx.createIndexStmt());
        } else if (ctx.dropTableStmt() != null) {
            this.visitor = new DropTableStatementHandler(ctx.dropTableStmt());
        } else if (ctx.dropIndexStmt() != null) {
            this.visitor = new DropIndexStatementHandler(ctx.dropIndexStmt());
        } else if (ctx.insertStmt() != null) {
            this.visitor = new InsertStatementHandler(ctx.insertStmt());
        } else if (ctx.deleteStmt() != null) {
            this.visitor = new DeleteStatementHandler(ctx.deleteStmt());
        } else if (ctx.updateStmt() != null) {
            this.visitor = new UpdateStatementHandler(ctx.updateStmt());
        } else if (ctx.commitStmt() != null) {
            this.visitor = new CommitStatementHandler();
        } else if (ctx.rollbackStmt() != null) {
            this.visitor = new RollbackStatementHandler(ctx.rollbackStmt());
        } else if (ctx.savepointStmt() != null) {
            this.visitor = new SavepointStatementHandler(ctx.savepointStmt());
        } else if (ctx.releaseStmt() != null) {
            this.visitor = new ReleaseStatementHandler(ctx.releaseStmt());
        } else if (ctx.explainStmt() != null) {
            this.visitor = new ExplainStatementHandler(ctx.explainStmt());
        }
    }

    public Optional<QueryPlan> execute(Transaction transaction, PrintStream out) {
        Optional<QueryPlan> qp = visitor.getQueryPlan(transaction);
        if (qp.isPresent()) return qp;
        visitor.execute(transaction, out);
        return Optional.empty();
    }
}
