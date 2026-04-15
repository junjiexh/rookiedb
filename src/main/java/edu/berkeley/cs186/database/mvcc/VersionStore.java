package edu.berkeley.cs186.database.mvcc;

import edu.berkeley.cs186.database.table.Record;

import java.util.List;
import java.util.Optional;

/**
 * Logical row store with MVCC semantics.
 *
 * A "row" is identified by an opaque rowId. Each row carries a chain of
 * versions; visibility is computed against the caller's Snapshot.
 *
 * Phase 2 ships an in-memory implementation. Phase 3 will add a Table-backed
 * implementation that persists versions through the buffer manager.
 */
public interface VersionStore {

    /** Insert a new row. Returns its rowId. */
    long insert(Record record, long xid);

    /**
     * Update the row's currently-live version to a new value.
     * Throws WriteConflictException if another active txn already wrote it.
     */
    void update(long rowId, Record record, long xid);

    /**
     * Logically delete the row.
     * Throws WriteConflictException on write-write conflict.
     */
    void delete(long rowId, long xid);

    /** Read the version of row visible to snap, if any. */
    Optional<Record> read(long rowId, Snapshot snap, TransactionManager tm);

    /** Return all rows visible to snap. */
    List<Record> scan(Snapshot snap, TransactionManager tm);
}
