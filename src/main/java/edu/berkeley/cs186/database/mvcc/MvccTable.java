package edu.berkeley.cs186.database.mvcc;

import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.RecordId;
import edu.berkeley.cs186.database.table.Schema;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

/**
 * MVCC wrapper around a real RookieDB Table.
 *
 * The underlying Table is created with a physical schema (user columns plus
 * __xmin/__xmax). This wrapper stamps version metadata on write and filters
 * by Snapshot visibility on read.
 *
 * Phase 3b: INSERT, READ, SCAN. Each logical row is a single physical record;
 * UPDATE and DELETE (which require version chains) arrive in Phase 3c.
 */
public class MvccTable {
    private final TransactionContext ctx;
    private final String tableName;
    private final Schema userSchema;
    private final TransactionManager tm;

    public MvccTable(TransactionContext ctx, String tableName,
                     Schema userSchema, TransactionManager tm) {
        this.ctx = ctx;
        this.tableName = tableName;
        this.userSchema = userSchema;
        this.tm = tm;
    }

    public Schema userSchema() { return userSchema; }

    /** Insert a new user record stamped with xmin = xid, xmax = ALIVE. */
    public RecordId mvccInsert(Record userRecord, long xid) {
        Record physical = MvccSchema.attachVersion(userRecord, xid, MvccSchema.ALIVE);
        return ctx.addRecord(tableName, physical);
    }

    /** Read a record by its physical RecordId, filtered by snapshot visibility. */
    public Optional<Record> mvccGetRecord(RecordId rid, Snapshot snap) {
        Record physical = ctx.getRecord(tableName, rid);
        if (physical == null) return Optional.empty();
        if (!Visibility.isVisible(MvccSchema.xmin(physical), MvccSchema.xmax(physical),
                                  snap, tm)) {
            return Optional.empty();
        }
        return Optional.of(MvccSchema.userRecord(physical));
    }

    /** Scan the full table, returning only user records visible to the snapshot. */
    public List<Record> mvccScan(Snapshot snap) {
        List<Record> out = new ArrayList<>();
        Iterator<Record> it = ctx.getRecordIterator(tableName);
        while (it.hasNext()) {
            Record physical = it.next();
            if (Visibility.isVisible(MvccSchema.xmin(physical), MvccSchema.xmax(physical),
                                     snap, tm)) {
                out.add(MvccSchema.userRecord(physical));
            }
        }
        return out;
    }
}
