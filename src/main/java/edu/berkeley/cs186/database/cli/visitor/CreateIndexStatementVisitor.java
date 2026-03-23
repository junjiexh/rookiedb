package edu.berkeley.cs186.database.cli.visitor;

import edu.berkeley.cs186.database.Transaction;
import edu.berkeley.cs186.database.cli.parser.RookieSQLParser.CreateIndexStmtContext;

import java.io.PrintStream;

class CreateIndexStatementVisitor extends StatementVisitor {
    public String tableName;
    public String columnName;

    public CreateIndexStatementVisitor(CreateIndexStmtContext ctx) {
        this.tableName = ctx.identifier().getText();
        this.columnName = ctx.columnName().getText();
    }

    @Override
    public void execute(Transaction transaction, PrintStream out) {
        transaction.createIndex(tableName, columnName, false);
        out.printf("CREATE INDEX ON %s (%s)\n", tableName, columnName);
    }

    @Override
    public StatementType getType() {
        return StatementType.CREATE_INDEX;
    }
}
