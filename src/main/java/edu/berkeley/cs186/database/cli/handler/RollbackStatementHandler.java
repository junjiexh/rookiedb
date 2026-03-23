package edu.berkeley.cs186.database.cli.handler;

import edu.berkeley.cs186.database.cli.parser.RookieSQLParser.RollbackStmtContext;

import java.util.Optional;

class RollbackStatementHandler extends StatementHandler {
    public String savepointName;

    public RollbackStatementHandler(RollbackStmtContext ctx) {
        if (ctx.identifier() != null) {
            this.savepointName = ctx.identifier().getText();
        }
    }

    @Override
    public StatementType getType() {
        return StatementType.ROLLBACK;
    }

    @Override
    public Optional<String> getSavepointName() {
        if (savepointName != null) {
            return Optional.of(this.savepointName);
        }
        return Optional.empty();
    }
}
