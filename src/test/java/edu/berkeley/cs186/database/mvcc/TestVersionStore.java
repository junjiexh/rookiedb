package edu.berkeley.cs186.database.mvcc;

import edu.berkeley.cs186.database.categories.ProjTests;
import edu.berkeley.cs186.database.categories.PublicTests;
import edu.berkeley.cs186.database.databox.IntDataBox;
import edu.berkeley.cs186.database.table.Record;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.List;
import java.util.Optional;

import static org.junit.Assert.*;

/**
 * Tests for the in-memory VersionStore — a logical row store with MVCC semantics.
 *
 * Key abstraction: each logical row has a chain of versions. A version becomes
 * visible/invisible based on (xmin, xmax) and the reader's Snapshot.
 *
 * VersionStore is storage-backend-agnostic. Phase 3 will wire a real Table behind it.
 */
@Category({ProjTests.class, PublicTests.class})
public class TestVersionStore {

    private TransactionManager tm;
    private VersionStore store;

    @Before
    public void setUp() {
        tm = new TransactionManager();
        store = new InMemoryVersionStore(tm);
    }

    private Record rec(int v) {
        return new Record(new IntDataBox(v));
    }

    // --- INSERT ---

    @Test
    public void insertIsVisibleToSelf() {
        long xid = tm.begin();
        long rowId = store.insert(rec(42), xid);
        Snapshot snap = tm.snapshot(xid);

        Optional<Record> r = store.read(rowId, snap, tm);
        assertTrue(r.isPresent());
        assertEquals(rec(42), r.get());
    }

    @Test
    public void insertNotVisibleToConcurrentReader() {
        long writer = tm.begin();
        long reader = tm.begin();
        Snapshot readerSnap = tm.snapshot(reader);

        long rowId = store.insert(rec(42), writer);
        // Reader's snapshot taken before writer inserted — but anyway writer not committed
        assertFalse(store.read(rowId, readerSnap, tm).isPresent());
    }

    @Test
    public void committedInsertVisibleToLaterReader() {
        long writer = tm.begin();
        long rowId = store.insert(rec(42), writer);
        tm.commit(writer);

        long reader = tm.begin();
        Snapshot snap = tm.snapshot(reader);
        assertEquals(rec(42), store.read(rowId, snap, tm).get());
    }

    // --- UPDATE ---

    @Test
    public void updateProducesNewVersionVisibleToSelf() {
        long t1 = tm.begin();
        long rowId = store.insert(rec(1), t1);
        store.update(rowId, rec(2), t1);
        tm.commit(t1);

        long t2 = tm.begin();
        Snapshot snap = tm.snapshot(t2);
        assertEquals(rec(2), store.read(rowId, snap, tm).get());
    }

    @Test
    public void updateOldVersionStillVisibleToConcurrentReader() {
        // Setup: row exists, committed
        long setup = tm.begin();
        long rowId = store.insert(rec(1), setup);
        tm.commit(setup);

        // Reader takes snapshot
        long reader = tm.begin();
        Snapshot readerSnap = tm.snapshot(reader);

        // Writer updates after reader's snapshot
        long writer = tm.begin();
        store.update(rowId, rec(99), writer);
        tm.commit(writer);

        // Reader still sees old value
        assertEquals(rec(1), store.read(rowId, readerSnap, tm).get());
    }

    // --- DELETE ---

    @Test
    public void deleteRemovesRowFromOwnView() {
        long t1 = tm.begin();
        long rowId = store.insert(rec(1), t1);
        tm.commit(t1);

        long t2 = tm.begin();
        store.delete(rowId, t2);
        Snapshot snap = tm.snapshot(t2);

        assertFalse(store.read(rowId, snap, tm).isPresent());
    }

    @Test
    public void deleteNotVisibleToConcurrentReader() {
        long setup = tm.begin();
        long rowId = store.insert(rec(1), setup);
        tm.commit(setup);

        long reader = tm.begin();
        Snapshot readerSnap = tm.snapshot(reader);

        long deleter = tm.begin();
        store.delete(rowId, deleter);
        tm.commit(deleter);

        assertEquals(rec(1), store.read(rowId, readerSnap, tm).get());
    }

    // --- WRITE-WRITE CONFLICT ---

    @Test(expected = WriteConflictException.class)
    public void concurrentUpdatesConflict() {
        long setup = tm.begin();
        long rowId = store.insert(rec(1), setup);
        tm.commit(setup);

        long t1 = tm.begin();
        long t2 = tm.begin();

        store.update(rowId, rec(10), t1);
        // t2 sees a version with xmax=t1 (uncommitted) → must abort
        store.update(rowId, rec(20), t2);
    }

    @Test(expected = WriteConflictException.class)
    public void deleteThenConcurrentDeleteConflicts() {
        long setup = tm.begin();
        long rowId = store.insert(rec(1), setup);
        tm.commit(setup);

        long t1 = tm.begin();
        long t2 = tm.begin();

        store.delete(rowId, t1);
        store.delete(rowId, t2);
    }

    @Test
    public void updateAfterConflictingTxnAbortsSucceeds() {
        long setup = tm.begin();
        long rowId = store.insert(rec(1), setup);
        tm.commit(setup);

        long t1 = tm.begin();
        store.update(rowId, rec(10), t1);
        tm.abort(t1);

        // After abort, t2 should be able to update
        long t2 = tm.begin();
        store.update(rowId, rec(20), t2);
        tm.commit(t2);

        long reader = tm.begin();
        Snapshot snap = tm.snapshot(reader);
        assertEquals(rec(20), store.read(rowId, snap, tm).get());
    }

    // --- SCAN ---

    @Test
    public void scanReturnsOnlyVisibleRows() {
        long setup = tm.begin();
        long r1 = store.insert(rec(1), setup);
        long r2 = store.insert(rec(2), setup);
        long r3 = store.insert(rec(3), setup);
        tm.commit(setup);

        long deleter = tm.begin();
        store.delete(r2, deleter);
        tm.commit(deleter);

        long reader = tm.begin();
        Snapshot snap = tm.snapshot(reader);
        List<Record> seen = store.scan(snap, tm);
        assertEquals(2, seen.size());
        assertTrue(seen.contains(rec(1)));
        assertTrue(seen.contains(rec(3)));
    }

    @Test
    public void scanShowsOldVersionToOlderSnapshot() {
        long setup = tm.begin();
        long r1 = store.insert(rec(1), setup);
        tm.commit(setup);

        long reader = tm.begin();
        Snapshot oldSnap = tm.snapshot(reader);

        long writer = tm.begin();
        store.update(r1, rec(99), writer);
        tm.commit(writer);

        // Old snapshot still sees the original value
        List<Record> seen = store.scan(oldSnap, tm);
        assertEquals(1, seen.size());
        assertEquals(rec(1), seen.get(0));
    }
}
