package edu.berkeley.cs186.database.mvcc;

import edu.berkeley.cs186.database.Database;
import edu.berkeley.cs186.database.Transaction;
import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.categories.ProjTests;
import edu.berkeley.cs186.database.categories.PublicTests;
import edu.berkeley.cs186.database.concurrency.LockManager;
import edu.berkeley.cs186.database.databox.Type;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.RecordId;
import edu.berkeley.cs186.database.table.Schema;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.util.List;

import static org.junit.Assert.*;

/**
 * Phase 3c: UPDATE, DELETE, and write-write conflicts on MvccTable.
 *
 * Version chains use a forward __next_rid pointer so the previous version
 * stays readable by older snapshots.
 */
@Category({ProjTests.class, PublicTests.class})
public class TestMvccTableUpdate {

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    private Database db;
    private TransactionManager tm;

    private static final Schema USER_SCHEMA =
        new Schema().add("id", Type.intType()).add("val", Type.intType());

    @Before
    public void setUp() throws Exception {
        File dir = tempFolder.newFolder("mvcc_upd");
        db = new Database(dir.getAbsolutePath(), 64, new LockManager());
        db.setWorkMem(32);
        db.waitAllTransactions();
        tm = new TransactionManager();

        try (Transaction t = db.beginTransaction()) {
            t.createTable(MvccSchema.physical(USER_SCHEMA), "t");
        }
        db.waitAllTransactions();
    }

    @After
    public void tearDown() {
        // Defensively clear any transaction left on this thread by a prior test
        while (TransactionContext.getTransaction() != null) {
            TransactionContext.unsetTransaction();
        }
        if (db != null) db.close();
    }

    private Record rec(int id, int val) { return new Record(id, val); }

    private MvccTable mvccTable(Transaction t) {
        return new MvccTable(t.getTransactionContext(), "t", USER_SCHEMA, tm);
    }

    // ---- UPDATE ----

    @Test
    public void updateVisibleToLaterReader() {
        long w = tm.begin();
        RecordId rid;
        try (Transaction t = db.beginTransaction()) {
            rid = mvccTable(t).mvccInsert(rec(1, 10), w);
        }
        tm.commit(w);

        long u = tm.begin();
        try (Transaction t = db.beginTransaction()) {
            mvccTable(t).mvccUpdate(rid, rec(1, 99), u);
        }
        tm.commit(u);

        long r = tm.begin();
        Snapshot snap = tm.snapshot(r);
        try (Transaction t = db.beginTransaction()) {
            assertEquals(rec(1, 99), mvccTable(t).mvccGetRecord(rid, snap).get());
        }
    }

    @Test
    public void updateOldVersionVisibleToOlderSnapshot() {
        long setup = tm.begin();
        RecordId rid;
        try (Transaction t = db.beginTransaction()) {
            rid = mvccTable(t).mvccInsert(rec(1, 10), setup);
        }
        tm.commit(setup);

        // Reader takes snapshot BEFORE update
        long reader = tm.begin();
        Snapshot oldSnap = tm.snapshot(reader);

        long updater = tm.begin();
        try (Transaction t = db.beginTransaction()) {
            mvccTable(t).mvccUpdate(rid, rec(1, 99), updater);
        }
        tm.commit(updater);

        // Old reader still sees original
        try (Transaction t = db.beginTransaction()) {
            assertEquals(rec(1, 10), mvccTable(t).mvccGetRecord(rid, oldSnap).get());
        }

        // New reader sees updated
        long newReader = tm.begin();
        Snapshot newSnap = tm.snapshot(newReader);
        try (Transaction t = db.beginTransaction()) {
            assertEquals(rec(1, 99), mvccTable(t).mvccGetRecord(rid, newSnap).get());
        }
    }

    @Test
    public void multipleUpdatesFormChain() {
        long setup = tm.begin();
        RecordId rid;
        try (Transaction t = db.beginTransaction()) {
            rid = mvccTable(t).mvccInsert(rec(1, 1), setup);
        }
        tm.commit(setup);

        for (int v : new int[]{2, 3, 4}) {
            long x = tm.begin();
            try (Transaction t = db.beginTransaction()) {
                mvccTable(t).mvccUpdate(rid, rec(1, v), x);
            }
            tm.commit(x);
        }

        long r = tm.begin();
        Snapshot snap = tm.snapshot(r);
        try (Transaction t = db.beginTransaction()) {
            assertEquals(rec(1, 4), mvccTable(t).mvccGetRecord(rid, snap).get());
        }
    }

    @Test
    public void scanAfterUpdateShowsOnlyLatest() {
        long setup = tm.begin();
        RecordId rid;
        try (Transaction t = db.beginTransaction()) {
            rid = mvccTable(t).mvccInsert(rec(1, 1), setup);
        }
        tm.commit(setup);

        long u = tm.begin();
        try (Transaction t = db.beginTransaction()) {
            mvccTable(t).mvccUpdate(rid, rec(1, 99), u);
        }
        tm.commit(u);

        long r = tm.begin();
        Snapshot snap = tm.snapshot(r);
        try (Transaction t = db.beginTransaction()) {
            List<Record> all = mvccTable(t).mvccScan(snap);
            assertEquals(1, all.size());
            assertEquals(rec(1, 99), all.get(0));
        }
    }

    // ---- WRITE-WRITE CONFLICT ----

    @Test(expected = WriteConflictException.class)
    public void concurrentUpdatesConflict() {
        long setup = tm.begin();
        RecordId rid;
        try (Transaction t = db.beginTransaction()) {
            rid = mvccTable(t).mvccInsert(rec(1, 1), setup);
        }
        tm.commit(setup);

        long t1 = tm.begin();
        long t2 = tm.begin();

        try (Transaction tx = db.beginTransaction()) {
            mvccTable(tx).mvccUpdate(rid, rec(1, 10), t1);
        }
        try (Transaction tx = db.beginTransaction()) {
            mvccTable(tx).mvccUpdate(rid, rec(1, 20), t2);
        }
    }

    @Test
    public void updateAfterConflictingTxnAbortsSucceeds() {
        long setup = tm.begin();
        RecordId rid;
        try (Transaction t = db.beginTransaction()) {
            rid = mvccTable(t).mvccInsert(rec(1, 1), setup);
        }
        tm.commit(setup);

        long t1 = tm.begin();
        try (Transaction tx = db.beginTransaction()) {
            mvccTable(tx).mvccUpdate(rid, rec(1, 10), t1);
        }
        tm.abort(t1);

        long t2 = tm.begin();
        try (Transaction tx = db.beginTransaction()) {
            mvccTable(tx).mvccUpdate(rid, rec(1, 20), t2);
        }
        tm.commit(t2);

        long r = tm.begin();
        Snapshot snap = tm.snapshot(r);
        try (Transaction tx = db.beginTransaction()) {
            assertEquals(rec(1, 20), mvccTable(tx).mvccGetRecord(rid, snap).get());
        }
    }

    // ---- DELETE ----

    @Test
    public void deleteInvisibleToSelfAndLaterReaders() {
        long setup = tm.begin();
        RecordId rid;
        try (Transaction t = db.beginTransaction()) {
            rid = mvccTable(t).mvccInsert(rec(1, 1), setup);
        }
        tm.commit(setup);

        long d = tm.begin();
        try (Transaction tx = db.beginTransaction()) {
            mvccTable(tx).mvccDelete(rid, d);
        }
        tm.commit(d);

        long r = tm.begin();
        Snapshot snap = tm.snapshot(r);
        try (Transaction tx = db.beginTransaction()) {
            assertFalse(mvccTable(tx).mvccGetRecord(rid, snap).isPresent());
        }
    }

    @Test
    public void scanAfterDeleteReturnsEmpty() {
        long setup = tm.begin();
        RecordId rid;
        try (Transaction t = db.beginTransaction()) {
            rid = mvccTable(t).mvccInsert(rec(1, 1), setup);
        }
        tm.commit(setup);

        long d = tm.begin();
        try (Transaction tx = db.beginTransaction()) {
            mvccTable(tx).mvccDelete(rid, d);
        }
        tm.commit(d);

        long r = tm.begin();
        Snapshot snap = tm.snapshot(r);
        try (Transaction tx = db.beginTransaction()) {
            assertTrue(mvccTable(tx).mvccScan(snap).isEmpty());
        }
    }

    @Test
    public void deleteStillVisibleToOlderSnapshot() {
        long setup = tm.begin();
        RecordId rid;
        try (Transaction t = db.beginTransaction()) {
            rid = mvccTable(t).mvccInsert(rec(1, 1), setup);
        }
        tm.commit(setup);

        long reader = tm.begin();
        Snapshot oldSnap = tm.snapshot(reader);

        long d = tm.begin();
        try (Transaction tx = db.beginTransaction()) {
            mvccTable(tx).mvccDelete(rid, d);
        }
        tm.commit(d);

        try (Transaction tx = db.beginTransaction()) {
            assertEquals(rec(1, 1), mvccTable(tx).mvccGetRecord(rid, oldSnap).get());
        }
    }

    @Test(expected = WriteConflictException.class)
    public void concurrentDeleteConflicts() {
        long setup = tm.begin();
        RecordId rid;
        try (Transaction t = db.beginTransaction()) {
            rid = mvccTable(t).mvccInsert(rec(1, 1), setup);
        }
        tm.commit(setup);

        long t1 = tm.begin();
        long t2 = tm.begin();
        try (Transaction tx = db.beginTransaction()) {
            mvccTable(tx).mvccDelete(rid, t1);
        }
        try (Transaction tx = db.beginTransaction()) {
            mvccTable(tx).mvccDelete(rid, t2);
        }
    }
}
