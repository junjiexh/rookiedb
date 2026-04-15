package edu.berkeley.cs186.database.mvcc;

import edu.berkeley.cs186.database.table.Record;

/**
 * A row version: payload + (xmin, xmax) header.
 *
 *   xmin = xid that created this version (immutable after creation)
 *   xmax = xid that superseded/deleted this version (mutable; 0 = still alive)
 */
public final class VersionedRecord {
    private final Record record;
    private final long xmin;
    private long xmax;   // mutable: set when superseded or deleted

    public VersionedRecord(Record record, long xmin) {
        this.record = record;
        this.xmin = xmin;
        this.xmax = 0L;
    }

    public Record record() { return record; }
    public long xmin() { return xmin; }
    public long xmax() { return xmax; }

    void setXmax(long xmax) { this.xmax = xmax; }
}
