package edu.berkeley.cs186.database.cli.visitor;

import edu.berkeley.cs186.database.Transaction;
import edu.berkeley.cs186.database.cli.PrettyPrinter;
import edu.berkeley.cs186.database.cli.parser.RookieSQLParser.InsertStmtContext;
import edu.berkeley.cs186.database.cli.parser.RookieSQLParser.InsertValuesContext;
import edu.berkeley.cs186.database.cli.parser.RookieSQLParser.LiteralContext;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.table.Record;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;

class InsertStatementVisitor extends StatementVisitor {
    public String tableName;
    public List<Record> values = new ArrayList<>();

    public InsertStatementVisitor(InsertStmtContext ctx) {
        this.tableName = ctx.identifier().getText();
        for (InsertValuesContext ivCtx : ctx.insertValues()) {
            List<DataBox> currValues = new ArrayList<>();
            for (LiteralContext litCtx : ivCtx.literal()) {
                currValues.add(PrettyPrinter.parseLiteral(litCtx.getText()));
            }
            values.add(new Record(currValues));
        }
    }

    @Override
    public void execute(Transaction transaction, PrintStream out) {
        try {
            for (Record record : values) {
                transaction.insert(this.tableName, record);
            }
            out.println("INSERT");
        } catch (Exception e) {
            out.println(e.getMessage());
            out.println("Failed to execute INSERT.");
        }
    }

    @Override
    public StatementType getType() {
        return StatementType.INSERT;
    }
}
