package edu.berkeley.cs186.database.cli.visitor;

import edu.berkeley.cs186.database.Transaction;
import edu.berkeley.cs186.database.cli.parser.RookieSQLParser.ExecutableStmtContext;
import edu.berkeley.cs186.database.query.QueryPlan;

import java.io.PrintStream;
import java.util.Optional;

public class ExecutableStatementVisitor {
    StatementVisitor visitor = null;

    public ExecutableStatementVisitor(ExecutableStmtContext ctx) {
        if (ctx.selectStmt() != null) {
            this.visitor = new SelectStatementVisitor(ctx.selectStmt());
        } else if (ctx.createTableStmt() != null) {
            this.visitor = new CreateTableStatementVisitor(ctx.createTableStmt());
        } else if (ctx.createIndexStmt() != null) {
            this.visitor = new CreateIndexStatementVisitor(ctx.createIndexStmt());
        } else if (ctx.dropTableStmt() != null) {
            this.visitor = new DropTableStatementVisitor(ctx.dropTableStmt());
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
