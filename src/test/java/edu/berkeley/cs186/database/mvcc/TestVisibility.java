package edu.berkeley.cs186.database.mvcc;

import edu.berkeley.cs186.database.categories.ProjTests;
import edu.berkeley.cs186.database.categories.PublicTests;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Tests for MVCC visibility rules.
 *
 * A version is described by (xmin, xmax):
 *   xmin = xid that created the version
 *   xmax = xid that deleted/superseded the version (0 = still alive)
 *
 * A snapshot is taken at transaction start and contains:
 *   xid       = the viewing transaction
 *   xmax      = next-to-be-allocated xid (all xids >= this are "in the future")
 *   activeXids = xids that were in progress at snapshot time
 *
 * Visibility rule (PostgreSQL-style snapshot isolation):
 *   xmin must be "visible as committed" to the snapshot AND
 *   xmax must NOT be "visible as committed" to the snapshot.
 */
@Category({ProjTests.class, PublicTests.class})
public class TestVisibility {

    private TransactionManager tm;

    @Before
    public void setUp() {
        tm = new TransactionManager();
    }

    // --- xmin visibility ---

    @Test
    public void ownInsertIsVisible() {
        long xid = tm.begin();
        Snapshot snap = tm.snapshot(xid);
        // Row inserted by this transaction, not deleted
        assertTrue(Visibility.isVisible(xid, 0L, snap, tm));
    }

    @Test
    public void committedInsertBeforeSnapshotIsVisible() {
        long writer = tm.begin();
        tm.commit(writer);

        long reader = tm.begin();
        Snapshot snap = tm.snapshot(reader);
        assertTrue(Visibility.isVisible(writer, 0L, snap, tm));
    }

    @Test
    public void uncommittedInsertFromOtherTxnIsInvisible() {
        long writer = tm.begin();   // still active
        long reader = tm.begin();
        Snapshot snap = tm.snapshot(reader);
        // writer was active when reader took snapshot
        assertFalse(Visibility.isVisible(writer, 0L, snap, tm));
    }

    @Test
    public void insertCommittedAfterSnapshotIsInvisible() {
        // reader's snapshot taken first, then writer starts+commits
        long reader = tm.begin();
        Snapshot snap = tm.snapshot(reader);

        long writer = tm.begin();
        tm.commit(writer);
        // Even though writer is now committed, reader's snapshot predates it
        assertFalse(Visibility.isVisible(writer, 0L, snap, tm));
    }

    @Test
    public void insertByAbortedTxnIsInvisible() {
        long writer = tm.begin();
        tm.abort(writer);

        long reader = tm.begin();
        Snapshot snap = tm.snapshot(reader);
        assertFalse(Visibility.isVisible(writer, 0L, snap, tm));
    }

    @Test
    public void inProgressAtSnapshotLaterCommittedIsInvisible() {
        // Concurrent writer: active when snap taken, commits later
        long writer = tm.begin();
        long reader = tm.begin();
        Snapshot snap = tm.snapshot(reader);
        // Writer now commits AFTER snapshot taken
        tm.commit(writer);
        // Must still be invisible (snapshot isolation)
        assertFalse(Visibility.isVisible(writer, 0L, snap, tm));
    }

    // --- xmax visibility (deletes) ---

    @Test
    public void rowDeletedBySelfIsInvisible() {
        long xid = tm.begin();
        Snapshot snap = tm.snapshot(xid);
        // Same txn inserted and deleted
        assertFalse(Visibility.isVisible(xid, xid, snap, tm));
    }

    @Test
    public void rowDeletedByCommittedBeforeSnapshotIsInvisible() {
        long inserter = tm.begin();
        tm.commit(inserter);
        long deleter = tm.begin();
        tm.commit(deleter);

        long reader = tm.begin();
        Snapshot snap = tm.snapshot(reader);
        assertFalse(Visibility.isVisible(inserter, deleter, snap, tm));
    }

    @Test
    public void rowDeletedByUncommittedTxnStillVisible() {
        long inserter = tm.begin();
        tm.commit(inserter);
        long deleter = tm.begin();   // still active

        long reader = tm.begin();
        Snapshot snap = tm.snapshot(reader);
        // Deleter active when reader snap taken → delete not visible → row still visible
        assertTrue(Visibility.isVisible(inserter, deleter, snap, tm));
    }

    @Test
    public void rowDeletedByAbortedTxnStillVisible() {
        long inserter = tm.begin();
        tm.commit(inserter);
        long deleter = tm.begin();
        tm.abort(deleter);

        long reader = tm.begin();
        Snapshot snap = tm.snapshot(reader);
        assertTrue(Visibility.isVisible(inserter, deleter, snap, tm));
    }

    @Test
    public void rowDeletedAfterSnapshotStillVisible() {
        long inserter = tm.begin();
        tm.commit(inserter);

        long reader = tm.begin();
        Snapshot snap = tm.snapshot(reader);

        long deleter = tm.begin();
        tm.commit(deleter);
        // Delete happened after reader's snapshot
        assertTrue(Visibility.isVisible(inserter, deleter, snap, tm));
    }

    // --- Snapshot Isolation semantics: repeatable reads ---

    @Test
    public void snapshotIsStableAcrossConcurrentCommits() {
        long reader = tm.begin();
        Snapshot snap = tm.snapshot(reader);

        // Multiple concurrent writers commit in sequence
        long w1 = tm.begin();
        tm.commit(w1);
        long w2 = tm.begin();
        tm.commit(w2);
        long w3 = tm.begin();
        tm.commit(w3);

        // None of them visible
        assertFalse(Visibility.isVisible(w1, 0L, snap, tm));
        assertFalse(Visibility.isVisible(w2, 0L, snap, tm));
        assertFalse(Visibility.isVisible(w3, 0L, snap, tm));
    }
}
