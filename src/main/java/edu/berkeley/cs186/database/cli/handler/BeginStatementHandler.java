package edu.berkeley.cs186.database.cli.handler;

/**
 * Purely symbolic class
 */
class BeginStatementHandler extends StatementHandler {
    public StatementType getType() {
        return StatementType.BEGIN;
    }
}
