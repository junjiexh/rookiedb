package edu.berkeley.cs186.database.cli.handler;

import edu.berkeley.cs186.database.Transaction;
import edu.berkeley.cs186.database.cli.parser.RookieSQLParser.ReleaseStmtContext;

import java.io.PrintStream;

class ReleaseStatementHandler extends StatementHandler {
    public String savepointName;

    public ReleaseStatementHandler(ReleaseStmtContext ctx) {
        this.savepointName = ctx.identifier().getText();
    }

    @Override
    public void execute(Transaction transaction, PrintStream out) {
        try {
            transaction.releaseSavepoint(savepointName);
            out.println("RELEASE SAVEPOINT " + savepointName);
        } catch (Exception e) {
            out.println(e.getMessage());
            out.println("Failed to execute RELEASE SAVEPOINT.");
        }
    }

    @Override
    public StatementType getType() {
        return StatementType.RELEASE_SAVEPOINT;
    }
}
