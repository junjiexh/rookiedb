package edu.berkeley.cs186.database.mvcc;

/**
 * Visibility rules for MVCC snapshot isolation.
 *
 * A version (xmin, xmax) is visible to snapshot S iff:
 *   1. xmin is "effectively committed" from S's viewpoint, AND
 *   2. xmax is NOT "effectively committed" from S's viewpoint.
 *
 * "Effectively committed" means:
 *   - xid == S.xid (own write), OR
 *   - xid < S.xmax AND xid NOT IN S.activeXids AND tm.isCommitted(xid)
 *
 * Own deletes (xmax == S.xid) also count as "effectively committed"
 * so that a transaction doesn't see its own deletes.
 *
 * xmax == 0 is the sentinel for "not deleted".
 */
public final class Visibility {
    private Visibility() {}

    public static boolean isVisible(long xmin, long xmax, Snapshot snap,
                                    TransactionManager tm) {
        if (!isEffectivelyCommitted(xmin, snap, tm)) return false;
        if (xmax == 0L) return true;
        // xmax is effectively committed => delete is visible => row is NOT visible
        return !isEffectivelyCommitted(xmax, snap, tm);
    }

    private static boolean isEffectivelyCommitted(long xid, Snapshot snap,
                                                   TransactionManager tm) {
        if (xid == snap.xid()) return true;
        if (snap.isInFuture(xid)) return false;
        if (snap.isActiveAtSnapshot(xid)) return false;
        return tm.isCommitted(xid);
    }
}
