package edu.berkeley.cs186.database.cli.handler;

import edu.berkeley.cs186.database.cli.PrettyPrinter;
import edu.berkeley.cs186.database.cli.parser.RookieSQLParser.ColumnValueComparisonContext;
import edu.berkeley.cs186.database.cli.parser.RookieSQLParser.CvcColumnFirstContext;
import edu.berkeley.cs186.database.cli.parser.RookieSQLParser.CvcLiteralFirstContext;
import edu.berkeley.cs186.database.common.PredicateOperator;
import edu.berkeley.cs186.database.databox.DataBox;

class ColumnValueComparisonHandler {
    PredicateOperator op;
    String columnName;
    DataBox value;

    public ColumnValueComparisonHandler(ColumnValueComparisonContext ctx) {
        if (ctx instanceof CvcColumnFirstContext colFirst) {
            this.columnName = colFirst.columnName().getText();
            this.op = PredicateOperator.fromSymbol(colFirst.comparisonOperator().getText());
            this.value = PrettyPrinter.parseLiteral(colFirst.literal().getText());
        } else if (ctx instanceof CvcLiteralFirstContext litFirst) {
            this.columnName = litFirst.columnName().getText();
            this.op = PredicateOperator.fromSymbol(litFirst.comparisonOperator().getText()).reverse();
            this.value = PrettyPrinter.parseLiteral(litFirst.literal().getText());
        }
    }
}
