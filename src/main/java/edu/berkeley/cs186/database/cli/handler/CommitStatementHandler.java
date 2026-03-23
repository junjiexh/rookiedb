package edu.berkeley.cs186.database.cli.handler;

/**
 * Purely symbolic class
 */
class CommitStatementHandler extends StatementHandler {
    public StatementType getType() {
        return StatementType.COMMIT;
    }
}
