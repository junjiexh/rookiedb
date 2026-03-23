package edu.berkeley.cs186.database.cli.visitor;

import edu.berkeley.cs186.database.Transaction;
import edu.berkeley.cs186.database.cli.parser.RookieSQLParser.DropIndexStmtContext;

import java.io.PrintStream;

class DropIndexStatementVisitor extends StatementVisitor {
    public String tableName;
    public String columnName;

    public DropIndexStatementVisitor(DropIndexStmtContext ctx) {
        this.tableName = ctx.identifier().getText();
        this.columnName = ctx.columnName().getText();
    }

    @Override
    public void execute(Transaction transaction, PrintStream out) {
        try {
            transaction.dropIndex(tableName, columnName);
            out.printf("DROP INDEX %s(%s)\n", tableName, columnName);
        } catch (Exception e) {
            out.println(e.getMessage());
            out.println("Failed to execute DROP INDEX.");
        }
    }

    @Override
    public StatementType getType() {
        return StatementType.DROP_INDEX;
    }
}
