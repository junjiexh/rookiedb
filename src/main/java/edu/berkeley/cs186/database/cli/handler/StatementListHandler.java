package edu.berkeley.cs186.database.cli.handler;

import edu.berkeley.cs186.database.Database;
import edu.berkeley.cs186.database.Transaction;
import edu.berkeley.cs186.database.cli.parser.RookieSQLBaseVisitor;
import edu.berkeley.cs186.database.cli.parser.RookieSQLParser;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class StatementListHandler extends RookieSQLBaseVisitor<Void> {
    private Database database;
    private PrintStream out;
    public List<StatementHandler> statementHandlers;

    public StatementListHandler(Database database, PrintStream out) {
        this.database = database;
        this.out = out;
        this.statementHandlers = new ArrayList<>();
    }

    public Transaction execute(Transaction currTransaction) {
        for (StatementHandler handler: this.statementHandlers) {
            switch(handler.getType()) {
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
                        Optional<String> savepointName = handler.getSavepointName();
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
                            handler.execute(tmp, out);
                            tmp.commit();
                        } catch (Exception e) {
                            e.printStackTrace(this.out);
                            this.out.println("Operation failed.");
                        }
                    } else {
                        try {
                            handler.execute(currTransaction, out);
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
            this.statementHandlers.add(new BeginStatementHandler());
        } else if (ctx.commitStmt() != null) {
            this.statementHandlers.add(new CommitStatementHandler());
        } else if (ctx.rollbackStmt() != null) {
            this.statementHandlers.add(new RollbackStatementHandler(ctx.rollbackStmt()));
        } else if (ctx.savepointStmt() != null) {
            this.statementHandlers.add(new SavepointStatementHandler(ctx.savepointStmt()));
        } else if (ctx.releaseStmt() != null) {
            this.statementHandlers.add(new ReleaseStatementHandler(ctx.releaseStmt()));
        } else if (ctx.selectStmt() != null) {
            try {
                this.statementHandlers.add(new SelectStatementHandler(ctx.selectStmt()));
            } catch (UnsupportedOperationException e) {
                this.out.println("Failed to execute SELECT");
                this.out.println(e.getMessage());
            }
        } else if (ctx.createTableStmt() != null) {
            this.statementHandlers.add(new CreateTableStatementHandler(ctx.createTableStmt()));
        } else if (ctx.dropTableStmt() != null) {
            this.statementHandlers.add(new DropTableStatementHandler(ctx.dropTableStmt()));
        } else if (ctx.createIndexStmt() != null) {
            this.statementHandlers.add(new CreateIndexStatementHandler(ctx.createIndexStmt()));
        } else if (ctx.dropIndexStmt() != null) {
            this.statementHandlers.add(new DropIndexStatementHandler(ctx.dropIndexStmt()));
        } else if (ctx.insertStmt() != null) {
            this.statementHandlers.add(new InsertStatementHandler(ctx.insertStmt()));
        } else if (ctx.deleteStmt() != null) {
            this.statementHandlers.add(new DeleteStatementHandler(ctx.deleteStmt()));
        } else if (ctx.updateStmt() != null) {
            this.statementHandlers.add(new UpdateStatementHandler(ctx.updateStmt()));
        } else if (ctx.explainStmt() != null) {
            this.statementHandlers.add(new ExplainStatementHandler(ctx.explainStmt()));
        }
        return null;
    }
}
