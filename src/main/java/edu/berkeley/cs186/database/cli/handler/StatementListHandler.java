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
    public Void visitBeginStmt(RookieSQLParser.BeginStmtContext ctx) {
        this.statementHandlers.add(new BeginStatementHandler());
        return null;
    }

    @Override
    public Void visitCommitStmt(RookieSQLParser.CommitStmtContext ctx) {
        this.statementHandlers.add(new CommitStatementHandler());
        return null;
    }

    @Override
    public Void visitRollbackStmt(RookieSQLParser.RollbackStmtContext ctx) {
        this.statementHandlers.add(new RollbackStatementHandler(ctx));
        return null;
    }

    @Override
    public Void visitSavepointStmt(RookieSQLParser.SavepointStmtContext ctx) {
        this.statementHandlers.add(new SavepointStatementHandler(ctx));
        return null;
    }

    @Override
    public Void visitReleaseStmt(RookieSQLParser.ReleaseStmtContext ctx) {
        this.statementHandlers.add(new ReleaseStatementHandler(ctx));
        return null;
    }

    @Override
    public Void visitSelectStmt(RookieSQLParser.SelectStmtContext ctx) {
        try {
            this.statementHandlers.add(new SelectStatementHandler(ctx));
        } catch (UnsupportedOperationException e) {
            this.out.println("Failed to execute SELECT");
            this.out.println(e.getMessage());
        }
        return null;
    }

    @Override
    public Void visitCreateTableStmt(RookieSQLParser.CreateTableStmtContext ctx) {
        this.statementHandlers.add(new CreateTableStatementHandler(ctx));
        return null;
    }

    @Override
    public Void visitDropTableStmt(RookieSQLParser.DropTableStmtContext ctx) {
        this.statementHandlers.add(new DropTableStatementHandler(ctx));
        return null;
    }

    @Override
    public Void visitCreateIndexStmt(RookieSQLParser.CreateIndexStmtContext ctx) {
        this.statementHandlers.add(new CreateIndexStatementHandler(ctx));
        return null;
    }

    @Override
    public Void visitDropIndexStmt(RookieSQLParser.DropIndexStmtContext ctx) {
        this.statementHandlers.add(new DropIndexStatementHandler(ctx));
        return null;
    }

    @Override
    public Void visitInsertStmt(RookieSQLParser.InsertStmtContext ctx) {
        this.statementHandlers.add(new InsertStatementHandler(ctx));
        return null;
    }

    @Override
    public Void visitDeleteStmt(RookieSQLParser.DeleteStmtContext ctx) {
        this.statementHandlers.add(new DeleteStatementHandler(ctx));
        return null;
    }

    @Override
    public Void visitUpdateStmt(RookieSQLParser.UpdateStmtContext ctx) {
        this.statementHandlers.add(new UpdateStatementHandler(ctx));
        return null;
    }

    @Override
    public Void visitExplainStmt(RookieSQLParser.ExplainStmtContext ctx) {
        this.statementHandlers.add(new ExplainStatementHandler(ctx));
        return null;
    }
}
