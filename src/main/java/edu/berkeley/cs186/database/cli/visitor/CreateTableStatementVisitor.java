package edu.berkeley.cs186.database.cli.visitor;

import edu.berkeley.cs186.database.Transaction;
import edu.berkeley.cs186.database.cli.parser.RookieSQLParser.ColumnDefContext;
import edu.berkeley.cs186.database.cli.parser.RookieSQLParser.CreateTableStmtContext;
import edu.berkeley.cs186.database.databox.Type;
import edu.berkeley.cs186.database.query.QueryOperator;
import edu.berkeley.cs186.database.query.QueryPlan;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.Schema;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

class CreateTableStatementVisitor extends StatementVisitor {
    public String tableName;
    public List<String> errorMessages = new ArrayList<>();
    public Schema schema = new Schema();
    public SelectStatementVisitor selectStatementVisitor = null;

    public CreateTableStatementVisitor(CreateTableStmtContext ctx) {
        this.tableName = ctx.identifier().getText();
        if (ctx.selectStmt() != null) {
            this.selectStatementVisitor = new SelectStatementVisitor(ctx.selectStmt());
        } else {
            for (ColumnDefContext colDef : ctx.columnDef()) {
                String fieldName = colDef.identifier(0).getText();
                String fieldTypeStr = colDef.identifier(1).getText();
                String paramStr = colDef.NUMERIC_LITERAL() != null ? colDef.NUMERIC_LITERAL().getText() : null;
                Type fieldType = Type.intType();
                switch (fieldTypeStr.toLowerCase()) {
                    case "int":
                    case "integer":
                        fieldType = Type.intType();
                        break;
                    case "char":
                    case "varchar":
                    case "string":
                        if (paramStr == null) {
                            errorMessages.add(String.format("Missing length for %s(n).", fieldType));
                            continue;
                        }
                        if (paramStr.indexOf('.') >= 0) {
                            errorMessages.add(String.format("Length of %s(n) must be integer, not `%s`.", fieldType, paramStr));
                            continue;
                        }
                        fieldType = Type.stringType(Integer.parseInt(paramStr));
                        break;
                    case "float":
                        fieldType = Type.floatType();
                        break;
                    case "long":
                        fieldType = Type.longType();
                        break;
                    case "bool":
                    case "boolean":
                        fieldType = Type.boolType();
                        break;
                    default:
                        assert false : String.format(
                                "Invalid field type \"%s\"",
                                fieldTypeStr
                        );
                }
                schema.add(fieldName, fieldType);
            }
        }
    }

    public void execute(Transaction transaction, PrintStream out) {
        if (this.errorMessages.size() > 0) {
            for (String msg : errorMessages) {
                out.println(msg);
            }
            out.println("Failed to execute CREATE TABLE.");
        } else {
            if (selectStatementVisitor != null) {
                QueryPlan p = selectStatementVisitor.getQueryPlan(transaction).get();
                p.execute();
                QueryOperator op = p.getFinalOperator();
                Schema s = op.getSchema();
                for (int i = 0; i < s.size(); i++) {
                    if (s.getFieldName(i).contains(".")) {
                        throw new UnsupportedOperationException("Cannot have `.` in table field name.");
                    }
                }
                transaction.createTable(s, this.tableName);
                Iterator<Record> records = op.iterator();
                while (records.hasNext()) {
                    Record r = records.next();
                    transaction.insert(this.tableName, r);
                }
            } else {
                transaction.createTable(this.schema, this.tableName);
            }
            out.println("CREATE TABLE " + tableName);
        }
    }

    @Override
    public StatementType getType() {
        return StatementType.CREATE_TABLE;
    }
}
