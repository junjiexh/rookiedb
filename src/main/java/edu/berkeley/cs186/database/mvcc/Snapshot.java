package edu.berkeley.cs186.database.mvcc;

import java.util.Collections;
import java.util.Set;

/**
 * Immutable view of the transaction state at a point in time.
 *
 * Three pieces of information:
 *   xid        = the viewing transaction
 *   xmax       = next-to-be-allocated xid; any xid >= xmax was not yet begun
 *   activeXids = xids that were begun but not yet committed/aborted at snapshot time
 *                (excluding the viewing xid itself)
 */
public final class Snapshot {
    private final long xid;
    private final long xmax;
    private final Set<Long> activeXids;

    public Snapshot(long xid, long xmax, Set<Long> activeXids) {
        this.xid = xid;
        this.xmax = xmax;
        this.activeXids = Collections.unmodifiableSet(activeXids);
    }

    public long xid() { return xid; }
    public long xmax() { return xmax; }

    /** True iff xid was active (in-progress) when this snapshot was taken. */
    public boolean isActiveAtSnapshot(long otherXid) {
        return activeXids.contains(otherXid);
    }

    /** True iff xid was not yet begun when this snapshot was taken. */
    public boolean isInFuture(long otherXid) {
        return otherXid >= xmax;
    }
}
