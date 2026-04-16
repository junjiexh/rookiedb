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
import java.util.Optional;

import static org.junit.Assert.*;

/**
 * Integration tests for MvccTable: a thin wrapper around a real RookieDB Table
 * whose physical schema carries __xmin/__xmax stamps.
 *
 * Phase 3b covers only INSERT + READ + SCAN. UPDATE/DELETE and write-write
 * conflicts arrive in Phase 3c along with the version chain.
 */
@Category({ProjTests.class, PublicTests.class})
public class TestMvccTable {

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    private Database db;
    private TransactionManager tm;

    private static final Schema USER_SCHEMA =
        new Schema().add("id", Type.intType()).add("val", Type.intType());

    @Before
    public void setUp() throws Exception {
        File dir = tempFolder.newFolder("mvcc_table");
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
        while (TransactionContext.getTransaction() != null) {
            TransactionContext.unsetTransaction();
        }
        if (db != null) db.close();
    }

    private Record userRec(int id, int val) {
        return new Record(id, val);
    }

    private MvccTable mvccTable(Transaction t) {
        return new MvccTable(t.getTransactionContext(), "t", USER_SCHEMA, tm);
    }

    // --- INSERT + READ ---

    @Test
    public void insertVisibleToSelf() {
        long xid = tm.begin();
        try (Transaction t = db.beginTransaction()) {
            MvccTable mt = mvccTable(t);
            RecordId rid = mt.mvccInsert(userRec(1, 100), xid);

            Snapshot snap = tm.snapshot(xid);
            Optional<Record> got = mt.mvccGetRecord(rid, snap);
            assertTrue(got.isPresent());
            assertEquals(userRec(1, 100), got.get());
        }
    }

    @Test
    public void insertNotVisibleToConcurrentReader() {
        long writer = tm.begin();
        long reader = tm.begin();
        Snapshot readerSnap = tm.snapshot(reader);

        try (Transaction t = db.beginTransaction()) {
            MvccTable mt = mvccTable(t);
            RecordId rid = mt.mvccInsert(userRec(1, 100), writer);
            assertFalse(mt.mvccGetRecord(rid, readerSnap).isPresent());
        }
    }

    @Test
    public void committedInsertVisibleToLaterReader() {
        long writer = tm.begin();
        RecordId rid;
        try (Transaction t = db.beginTransaction()) {
            rid = mvccTable(t).mvccInsert(userRec(1, 100), writer);
        }
        tm.commit(writer);

        long reader = tm.begin();
        Snapshot snap = tm.snapshot(reader);
        try (Transaction t = db.beginTransaction()) {
            assertEquals(userRec(1, 100), mvccTable(t).mvccGetRecord(rid, snap).get());
        }
    }

    @Test
    public void getRecordOnPhysicalIdWithInvisibleXminReturnsEmpty() {
        // Insert by aborted writer — physical record exists but xmin invalid
        long writer = tm.begin();
        RecordId rid;
        try (Transaction t = db.beginTransaction()) {
            rid = mvccTable(t).mvccInsert(userRec(1, 100), writer);
        }
        tm.abort(writer);

        long reader = tm.begin();
        Snapshot snap = tm.snapshot(reader);
        try (Transaction t = db.beginTransaction()) {
            assertFalse(mvccTable(t).mvccGetRecord(rid, snap).isPresent());
        }
    }

    // --- SCAN ---

    @Test
    public void scanReturnsOnlyVisibleRows() {
        long writer = tm.begin();
        try (Transaction t = db.beginTransaction()) {
            MvccTable mt = mvccTable(t);
            mt.mvccInsert(userRec(1, 10), writer);
            mt.mvccInsert(userRec(2, 20), writer);
            mt.mvccInsert(userRec(3, 30), writer);
        }
        tm.commit(writer);

        long reader = tm.begin();
        Snapshot snap = tm.snapshot(reader);
        try (Transaction t = db.beginTransaction()) {
            List<Record> rows = mvccTable(t).mvccScan(snap);
            assertEquals(3, rows.size());
            assertTrue(rows.contains(userRec(1, 10)));
            assertTrue(rows.contains(userRec(2, 20)));
            assertTrue(rows.contains(userRec(3, 30)));
        }
    }

    @Test
    public void scanSkipsUncommittedInserts() {
        // writer1 commits, writer2 does not
        long w1 = tm.begin();
        long w2 = tm.begin();
        try (Transaction t = db.beginTransaction()) {
            MvccTable mt = mvccTable(t);
            mt.mvccInsert(userRec(1, 10), w1);
            mt.mvccInsert(userRec(2, 20), w2);   // w2 uncommitted
        }
        tm.commit(w1);

        long reader = tm.begin();
        Snapshot snap = tm.snapshot(reader);
        try (Transaction t = db.beginTransaction()) {
            List<Record> rows = mvccTable(t).mvccScan(snap);
            assertEquals(1, rows.size());
            assertEquals(userRec(1, 10), rows.get(0));
        }
    }

    @Test
    public void scanReturnsEmptyForEmptyTable() {
        long reader = tm.begin();
        Snapshot snap = tm.snapshot(reader);
        try (Transaction t = db.beginTransaction()) {
            assertTrue(mvccTable(t).mvccScan(snap).isEmpty());
        }
    }
}
