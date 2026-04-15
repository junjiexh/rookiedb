package edu.berkeley.cs186.database.mvcc;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Central manager for MVCC transaction state.
 *
 * Tracks:
 *   - next xid to allocate
 *   - active xids (begun but not yet committed/aborted)
 *   - committed xids
 *   - aborted xids
 *
 * Snapshots are taken from this manager's current state.
 *
 * Reserved xid 0: represents "no transaction" (used for xmax == 0 sentinel).
 */
public class TransactionManager {
    private final AtomicLong nextXid = new AtomicLong(1);
    private final Set<Long> active = ConcurrentHashMap.newKeySet();
    private final Set<Long> committed = ConcurrentHashMap.newKeySet();
    private final Set<Long> aborted = ConcurrentHashMap.newKeySet();

    public long begin() {
        long xid = nextXid.getAndIncrement();
        active.add(xid);
        return xid;
    }

    public void commit(long xid) {
        if (!active.remove(xid)) {
            throw new IllegalStateException("xid " + xid + " not active");
        }
        committed.add(xid);
    }

    public void abort(long xid) {
        if (!active.remove(xid)) {
            throw new IllegalStateException("xid " + xid + " not active");
        }
        aborted.add(xid);
    }

    public boolean isActive(long xid) { return active.contains(xid); }
    public boolean isCommitted(long xid) { return committed.contains(xid); }
    public boolean isAborted(long xid) { return aborted.contains(xid); }

    /**
     * Capture a snapshot of the system state for the given viewing transaction.
     * Must be called after begin(xid). The viewing xid is excluded from activeXids.
     */
    public synchronized Snapshot snapshot(long xid) {
        Set<Long> activeAtSnap = new HashSet<>(active);
        activeAtSnap.remove(xid);
        long xmax = nextXid.get();
        return new Snapshot(xid, xmax, activeAtSnap);
    }
}
