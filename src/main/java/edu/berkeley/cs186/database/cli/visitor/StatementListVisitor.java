package edu.berkeley.cs186.database.cli.visitor;

import edu.berkeley.cs186.database.Database;
import edu.berkeley.cs186.database.Transaction;
import edu.berkeley.cs186.database.cli.parser.RookieSQLBaseVisitor;
import edu.berkeley.cs186.database.cli.parser.RookieSQLParser;

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
        for (StatementVisitor visitor: this.statementVisitors) {
            switch(visitor.getType()) {
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
                        if(savepointName.isPresent()) {
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
        if (ctx.beginStmt() != null) {
            this.statementVisitors.add(new BeginStatementVisitor());
        } else if (ctx.commitStmt() != null) {
            this.statementVisitors.add(new CommitStatementVisitor());
        } else if (ctx.rollbackStmt() != null) {
            this.statementVisitors.add(new RollbackStatementVisitor(ctx.rollbackStmt()));
        } else if (ctx.savepointStmt() != null) {
            this.statementVisitors.add(new SavepointStatementVisitor(ctx.savepointStmt()));
        } else if (ctx.releaseStmt() != null) {
            this.statementVisitors.add(new ReleaseStatementVisitor(ctx.releaseStmt()));
        } else if (ctx.selectStmt() != null) {
            try {
                this.statementVisitors.add(new SelectStatementVisitor(ctx.selectStmt()));
            } catch (UnsupportedOperationException e) {
                this.out.println("Failed to execute SELECT");
                this.out.println(e.getMessage());
            }
        } else if (ctx.createTableStmt() != null) {
            this.statementVisitors.add(new CreateTableStatementVisitor(ctx.createTableStmt()));
        } else if (ctx.dropTableStmt() != null) {
            this.statementVisitors.add(new DropTableStatementVisitor(ctx.dropTableStmt()));
        } else if (ctx.createIndexStmt() != null) {
            this.statementVisitors.add(new CreateIndexStatementVisitor(ctx.createIndexStmt()));
        } else if (ctx.dropIndexStmt() != null) {
            this.statementVisitors.add(new DropIndexStatementVisitor(ctx.dropIndexStmt()));
        } else if (ctx.insertStmt() != null) {
            this.statementVisitors.add(new InsertStatementVisitor(ctx.insertStmt()));
        } else if (ctx.deleteStmt() != null) {
            this.statementVisitors.add(new DeleteStatementVisitor(ctx.deleteStmt()));
        } else if (ctx.updateStmt() != null) {
            this.statementVisitors.add(new UpdateStatementVisitor(ctx.updateStmt()));
        } else if (ctx.explainStmt() != null) {
            this.statementVisitors.add(new ExplainStatementVisitor(ctx.explainStmt()));
        }
        return null;
    }
}
