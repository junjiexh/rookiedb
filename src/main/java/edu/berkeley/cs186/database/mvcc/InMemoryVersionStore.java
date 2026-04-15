package edu.berkeley.cs186.database.mvcc;

import edu.berkeley.cs186.database.table.Record;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

/**
 * In-memory MVCC version store.
 *
 * Storage layout: rowId -> ordered list of versions (oldest first).
 *
 * Update creates a new version and stamps the previous live version's xmax.
 * Delete only stamps xmax on the live version.
 *
 * Aborted versions stay in the chain but are skipped on reads (xmin invisible)
 * and treated as "not present" by the write path.
 *
 * Write-write conflict (FirstUpdateWins): if the latest non-aborted version's
 * xmax is already set by another active or committed transaction, this write
 * loses and must abort.
 */
public class InMemoryVersionStore implements VersionStore {

    private final TransactionManager tm;
    private final Map<Long, List<VersionedRecord>> rows = new LinkedHashMap<>();
    private final AtomicLong nextRowId = new AtomicLong(1);

    public InMemoryVersionStore(TransactionManager tm) {
        this.tm = tm;
    }

    @Override
    public synchronized long insert(Record record, long xid) {
        long rowId = nextRowId.getAndIncrement();
        List<VersionedRecord> chain = new ArrayList<>();
        chain.add(new VersionedRecord(record, xid));
        rows.put(rowId, chain);
        return rowId;
    }

    @Override
    public synchronized void update(long rowId, Record record, long xid) {
        VersionedRecord live = claimLiveVersionForWrite(rowId, xid);
        live.setXmax(xid);
        rows.get(rowId).add(new VersionedRecord(record, xid));
    }

    @Override
    public synchronized void delete(long rowId, long xid) {
        VersionedRecord live = claimLiveVersionForWrite(rowId, xid);
        live.setXmax(xid);
    }

    @Override
    public synchronized Optional<Record> read(long rowId, Snapshot snap,
                                              TransactionManager tm) {
        List<VersionedRecord> chain = rows.get(rowId);
        if (chain == null) return Optional.empty();
        for (VersionedRecord v : chain) {
            if (Visibility.isVisible(v.xmin(), v.xmax(), snap, tm)) {
                return Optional.of(v.record());
            }
        }
        return Optional.empty();
    }

    @Override
    public synchronized List<Record> scan(Snapshot snap, TransactionManager tm) {
        List<Record> out = new ArrayList<>();
        for (List<VersionedRecord> chain : rows.values()) {
            for (VersionedRecord v : chain) {
                if (Visibility.isVisible(v.xmin(), v.xmax(), snap, tm)) {
                    out.add(v.record());
                }
            }
        }
        return out;
    }

    /**
     * Find and return the live version this xid may write to.
     *
     * Walks the chain backwards skipping versions whose xmin was aborted
     * (those are dead and must not be chained off). On the first
     * "real" version found, validate its xmax:
     *   - 0 or aborted-other     → OK to claim (clear stale xmax)
     *   - set by self            → OK (no-op stamp)
     *   - set by active/committed → WriteConflictException
     */
    private VersionedRecord claimLiveVersionForWrite(long rowId, long xid) {
        List<VersionedRecord> chain = rows.get(rowId);
        if (chain == null || chain.isEmpty()) {
            throw new IllegalArgumentException("rowId " + rowId + " not found");
        }
        for (int i = chain.size() - 1; i >= 0; i--) {
            VersionedRecord v = chain.get(i);
            if (v.xmin() != xid) {
                if (tm.isAborted(v.xmin())) continue;       // dead, skip
                if (!tm.isCommitted(v.xmin())) {
                    throw new WriteConflictException(       // concurrent writer
                        "row " + rowId + " has uncommitted version from xid " + v.xmin());
                }
            }
            long xmax = v.xmax();
            if (xmax == 0L || xmax == xid) return v;
            if (tm.isAborted(xmax)) {
                v.setXmax(0L);                              // stale xmax, clean
                return v;
            }
            throw new WriteConflictException(
                "row " + rowId + " was concurrently modified by xid " + xmax);
        }
        throw new IllegalStateException("no live version for row " + rowId);
    }
}
