package edu.berkeley.cs186.database.mvcc;

/**
 * Thrown when an MVCC write encounters a row already being modified by
 * another in-progress (or recently committed) transaction.
 *
 * Under FirstUpdateWins semantics the loser must abort and retry.
 */
public class WriteConflictException extends RuntimeException {
    public WriteConflictException(String msg) { super(msg); }
}
