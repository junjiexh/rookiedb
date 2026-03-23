package edu.berkeley.cs186.database.cli.handler;

enum StatementType{
    CREATE_TABLE,
    CREATE_INDEX,
    DROP_TABLE,
    DROP_INDEX,
    SELECT,
    INSERT,
    DELETE,
    UPDATE,
    BEGIN,
    COMMIT,
    ROLLBACK,
    SAVEPOINT,
    RELEASE_SAVEPOINT,
    EXPLAIN
}