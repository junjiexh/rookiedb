package edu.berkeley.cs186.database.cli.visitor;

import edu.berkeley.cs186.database.Transaction;
import edu.berkeley.cs186.database.cli.parser.RookieSQLParser.SavepointStmtContext;

import java.io.PrintStream;

class SavepointStatementVisitor extends StatementVisitor {
    public String savepointName;

    public SavepointStatementVisitor(SavepointStmtContext ctx) {
        this.savepointName = ctx.identifier().getText();
    }

    @Override
    public void execute(Transaction transaction, PrintStream out) {
        transaction.savepoint(savepointName);
    }

    @Override
    public StatementType getType() {
        return StatementType.SAVEPOINT;
    }
}
