package edu.berkeley.cs186.database.mvcc;

import edu.berkeley.cs186.database.Database;
import edu.berkeley.cs186.database.Transaction;
import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.categories.ProjTests;
import edu.berkeley.cs186.database.categories.PublicTests;
import edu.berkeley.cs186.database.concurrency.LockManager;
import edu.berkeley.cs186.database.memory.ClockEvictionPolicy;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;

import java.io.File;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Integration tests for the Database/MVCC wiring introduced in Phase 3d.1:
 * Database owns a shared TransactionManager, and every user transaction
 * opened via beginTransaction() is registered/committed/aborted through it.
 */
@Category({ProjTests.class, PublicTests.class})
public class TestDatabaseMvcc {

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    private Database db;

    @Before
    public void setUp() throws Exception {
        File dir = tempFolder.newFolder("database_mvcc");
        // Enable ARIES so that t.rollback() works; the dummy recovery manager
        // throws on abort() and would make it impossible to test the abort path.
        db = new Database(dir.getAbsolutePath(), 64, new LockManager(),
                          new ClockEvictionPolicy(), true);
        db.setWorkMem(32);
        db.waitAllTransactions();
    }

    @After
    public void tearDown() {
        while (TransactionContext.getTransaction() != null) {
            TransactionContext.unsetTransaction();
        }
        if (db != null) db.close();
    }

    // ---- 3d.1: Database owns a TransactionManager ----------------------------

    @Test
    public void beginTransactionAllocatesXid() {
        TransactionManager tm = db.getTransactionManager();
        assertNotNull(tm);

        // RookieDB keeps the current TransactionContext in a ThreadLocal and
        // rejects a second setTransaction() before the first is cleared, so we
        // can only have one open transaction per thread. Check monotonicity
        // with two sequential begins instead.
        long x1;
        try (Transaction t1 = db.beginTransaction()) {
            x1 = t1.getXid();
            assertTrue("user xid must be non-zero", x1 > 0);
            assertTrue(tm.isActive(x1));
        }

        long x2;
        try (Transaction t2 = db.beginTransaction()) {
            x2 = t2.getXid();
            assertTrue("user xid must be non-zero", x2 > 0);
            assertTrue(tm.isActive(x2));
            assertTrue("xids must be monotonic across begin()s", x2 > x1);
        }
    }

    @Test
    public void commitMarksXidCommittedInManager() {
        TransactionManager tm = db.getTransactionManager();
        long xid;
        try (Transaction t = db.beginTransaction()) {
            xid = t.getXid();
            assertTrue(tm.isActive(xid));
            t.commit();
        }
        assertTrue("commit() must mark the xid committed in the TM",
                tm.isCommitted(xid));
        assertFalse(tm.isActive(xid));
        assertFalse(tm.isAborted(xid));
    }

    @Test
    public void rollbackMarksXidAborted() {
        TransactionManager tm = db.getTransactionManager();
        long xid;
        try (Transaction t = db.beginTransaction()) {
            xid = t.getXid();
            assertTrue(tm.isActive(xid));
            t.rollback();
        }
        assertTrue("rollback() must mark the xid aborted in the TM",
                tm.isAborted(xid));
        assertFalse(tm.isActive(xid));
        assertFalse(tm.isCommitted(xid));
    }
}
