package edu.berkeley.cs186.database.mvcc;

import edu.berkeley.cs186.database.categories.ProjTests;
import edu.berkeley.cs186.database.categories.PublicTests;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.*;

@Category({ProjTests.class, PublicTests.class})
public class TestTransactionManager {

    private TransactionManager tm;

    @Before
    public void setUp() {
        tm = new TransactionManager();
    }

    @Test
    public void xidsAreMonotonicAndUnique() {
        long x1 = tm.begin();
        long x2 = tm.begin();
        long x3 = tm.begin();
        assertTrue(x1 < x2);
        assertTrue(x2 < x3);
    }

    @Test
    public void newTxnIsActiveAfterBegin() {
        long xid = tm.begin();
        assertTrue(tm.isActive(xid));
        assertFalse(tm.isCommitted(xid));
        assertFalse(tm.isAborted(xid));
    }

    @Test
    public void commitTransitionsState() {
        long xid = tm.begin();
        tm.commit(xid);
        assertFalse(tm.isActive(xid));
        assertTrue(tm.isCommitted(xid));
        assertFalse(tm.isAborted(xid));
    }

    @Test
    public void abortTransitionsState() {
        long xid = tm.begin();
        tm.abort(xid);
        assertFalse(tm.isActive(xid));
        assertFalse(tm.isCommitted(xid));
        assertTrue(tm.isAborted(xid));
    }

    @Test
    public void snapshotCapturesActiveXidsAtTime() {
        long w1 = tm.begin();         // active
        long w2 = tm.begin();         // active
        long committed = tm.begin();
        tm.commit(committed);         // not active
        long reader = tm.begin();
        Snapshot snap = tm.snapshot(reader);

        assertTrue(snap.isActiveAtSnapshot(w1));
        assertTrue(snap.isActiveAtSnapshot(w2));
        assertFalse(snap.isActiveAtSnapshot(committed));
        // Reader itself is active but should not be in its own activeXids
        assertFalse(snap.isActiveAtSnapshot(reader));
    }

    @Test
    public void snapshotXmaxExcludesFutureXids() {
        long x1 = tm.begin();
        Snapshot snap = tm.snapshot(x1);
        long future = tm.begin();
        // future xid is >= snap.xmax
        assertTrue(future >= snap.xmax());
    }

    @Test
    public void snapshotXidMatchesViewingTxn() {
        long reader = tm.begin();
        Snapshot snap = tm.snapshot(reader);
        assertEquals(reader, snap.xid());
    }
}
