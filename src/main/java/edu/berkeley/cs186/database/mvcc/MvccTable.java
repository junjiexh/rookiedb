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
 * Each logical row is a forward-linked chain of physical records: the head
 * rid stays stable (and is what callers hold), while UPDATE appends a new
 * record and stamps (xmax, next_rid) on the prior tip.
 *
 *   v0 [xmin=a, xmax=b, next=v1.rid] -> v1 [xmin=b, xmax=c, next=v2.rid] -> ...
 *
 * At most one version in a chain is visible to any snapshot (since v_old.xmax
 * and v_new.xmin are the same xid — visibility flips in lockstep).
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

    // ---------- INSERT ----------

    public RecordId mvccInsert(Record userRecord, long xid) {
        Record physical = MvccSchema.attachVersion(userRecord, xid);
        return ctx.addRecord(tableName, physical);
    }

    // ---------- READ ----------

    /** Follow the chain from headRid and return the first visible user record. */
    public Optional<Record> mvccGetRecord(RecordId headRid, Snapshot snap) {
        RecordId cur = headRid;
        while (cur != null) {
            Record physical = ctx.getRecord(tableName, cur);
            if (physical == null) return Optional.empty();
            if (Visibility.isVisible(MvccSchema.xmin(physical), MvccSchema.xmax(physical),
                                     snap, tm)) {
                return Optional.of(MvccSchema.userRecord(physical));
            }
            cur = MvccSchema.next(physical);
        }
        return Optional.empty();
    }

    /**
     * Scan the whole table. Each physical record is filtered independently by
     * Visibility; at most one version per chain can be visible to any snapshot.
     */
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

    // ---------- UPDATE / DELETE ----------

    public void mvccUpdate(RecordId headRid, Record newUserRecord, long xid) {
        LiveTip tip = claimLiveTip(headRid, xid);
        RecordId newRid = ctx.addRecord(tableName,
            MvccSchema.attachVersion(newUserRecord, xid));
        ctx.updateRecord(tableName, tip.rid,
            MvccSchema.withXmaxAndNext(tip.record, xid, newRid));
    }

    public void mvccDelete(RecordId headRid, long xid) {
        LiveTip tip = claimLiveTip(headRid, xid);
        ctx.updateRecord(tableName, tip.rid, MvccSchema.withXmax(tip.record, xid));
    }

    // ---------- internal: find + validate the chain's live tip ----------

    /** Physical record and its rid — the version a writer will mutate. */
    private static final class LiveTip {
        final Record record;
        final RecordId rid;
        LiveTip(Record r, RecordId id) { this.record = r; this.rid = id; }
    }

    /**
     * Walk the chain from head, collect all versions, then scan backward to
     * find the last version whose xmin is self or committed (skipping dead
     * versions with aborted xmin). Validate that version's xmax for conflict.
     */
    private LiveTip claimLiveTip(RecordId headRid, long xid) {
        List<LiveTip> chain = new ArrayList<>();
        RecordId cur = headRid;
        while (cur != null) {
            Record r = ctx.getRecord(tableName, cur);
            if (r == null) break;
            chain.add(new LiveTip(r, cur));
            cur = MvccSchema.next(r);
        }
        if (chain.isEmpty()) {
            throw new IllegalArgumentException("row " + headRid + " not found");
        }
        for (int i = chain.size() - 1; i >= 0; i--) {
            LiveTip v = chain.get(i);
            long xmin = MvccSchema.xmin(v.record);
            if (xmin != xid) {
                if (tm.isAborted(xmin)) continue;           // dead, skip
                if (!tm.isCommitted(xmin)) {
                    throw new WriteConflictException(       // concurrent writer
                        "row " + headRid + " has uncommitted version from xid " + xmin);
                }
            }
            long xmax = MvccSchema.xmax(v.record);
            if (xmax == MvccSchema.ALIVE || xmax == xid) return v;
            if (tm.isAborted(xmax)) {
                Record cleaned = MvccSchema.withXmaxAndNext(
                    v.record, MvccSchema.ALIVE, null);
                ctx.updateRecord(tableName, v.rid, cleaned);
                return new LiveTip(cleaned, v.rid);
            }
            throw new WriteConflictException(
                "row " + headRid + " was concurrently modified by xid " + xmax);
        }
        throw new IllegalStateException("no live version for row " + headRid);
    }
}
