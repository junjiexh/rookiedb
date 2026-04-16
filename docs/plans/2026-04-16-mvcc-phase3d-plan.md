# MVCC Phase 3d — Database/Transaction Integration

> **For Claude:** Use TDD throughout. Red → Green → Refactor. Tests first, then minimum
> code to pass. All MVCC code lives under `src/{main,test}/java/edu/berkeley/cs186/database/mvcc/`.

**Goal:** Wire MVCC end-to-end through `Database.beginTransaction()` so a normal
RookieDB `Transaction` allocates an MVCC xid, captures a `Snapshot`, and routes
INSERT/UPDATE/DELETE/SELECT through the version layer instead of the legacy
in-place path.

When this phase is done, the existing MvccTable wrapper goes away (or becomes
package-private) — callers should not have to think about MVCC, they just open a
transaction and write SQL/CRUD.

---

## Where we are (recap)

Five MVCC commits already on `main`:

| Commit | Phase | What landed |
|--------|-------|-------------|
| `99d7c72` | 1   | `TransactionManager`, `Snapshot`, `Visibility` (pure visibility logic, 19 tests) |
| `6561f0a` | 2   | `VersionStore` interface + `InMemoryVersionStore` (12 tests) |
| `8f966ba` | 3a  | `MvccSchema` — append `__xmin`, `__xmax`, `__next_rid` system cols |
| `50855a4` | 3b  | `MvccTable` (insert/read/scan only) wrapping a real `Table` (7 tests) |
| `2c65a0c` | 3c  | Version chains, UPDATE, DELETE, write-write conflict (10 tests) |

Plus benchmark baseline at `34912ea` (`bench: add concurrency benchmark harness`).

**Total: 57/57 MVCC tests pass.**

### Code map (read these before writing anything)

```
src/main/java/edu/berkeley/cs186/database/mvcc/
├── TransactionManager.java       // begin/commit/abort + active set + snapshot()
├── Snapshot.java                 // immutable (xid, xmax, activeXids)
├── Visibility.java               // isVisible(xmin, xmax, snap, tm) — the truth table
├── MvccSchema.java               // user schema -> physical schema with stamps
├── VersionedRecord.java          // (Record, xmin, xmax) data carrier (Phase 2 only)
├── VersionStore.java             // Phase 2 interface
├── InMemoryVersionStore.java     // Phase 2 in-memory impl
├── MvccTable.java                // Phase 3b/c — wraps a real Table
└── WriteConflictException.java   // FUW abort signal

src/test/java/edu/berkeley/cs186/database/mvcc/
├── TestVisibility.java           // 12 cases
├── TestTransactionManager.java   // 7 cases
├── TestVersionStore.java         // 12 cases (in-memory store)
├── TestMvccSchema.java           //  9 cases
├── TestMvccTable.java            //  7 cases — insert/read/scan via real DB
└── TestMvccTableUpdate.java      // 10 cases — update/delete/conflict
```

**Read order if you've never seen this code:** Visibility.java → MvccSchema.java →
MvccTable.java → TestMvccTableUpdate.java.

### Design recap (one screen)

- **xid**: monotonic long allocated by `TransactionManager.begin()`. xid 0 is the
  "no transaction" sentinel.
- **Snapshot**: captured at begin-time. Contains `xid`, `xmax = nextXid at snap`,
  and `activeXids` (excludes self).
- **Visibility (snapshot isolation, PG-style)**: a version is visible iff its
  `xmin` is "effectively committed" (self / `xid < xmax` AND not in activeXids
  AND `tm.isCommitted(xid)`) AND its `xmax` is NOT effectively committed.
- **Version chain**: each logical row is a forward-linked chain of physical
  records via `__next_rid`. Head rid stays stable; UPDATE appends a new physical
  record and stamps `xmax + next_rid` on the previous tip.
- **FirstUpdateWins**: the second writer to touch a row throws
  `WriteConflictException`. Aborted writers are lazy-cleaned (skip the dead
  record, reset stale xmax).

---

## What 3d must do

### Goal in one sentence

`db.beginTransaction()` returns a Transaction whose `insert/update/delete/query`
calls go through MVCC automatically; reads do not block writers and writers do
not block readers.

### Concrete acceptance criteria

1. `Database.beginTransaction()` allocates an MVCC xid via a single shared
   `TransactionManager` instance owned by the Database.
2. The new `Transaction` carries its xid AND a Snapshot taken at begin time.
3. `Transaction.commit()` calls `tm.commit(xid)`. `rollback()` calls
   `tm.abort(xid)`. Crash before commit = abort (already true today via close()).
4. `Transaction.createTable(userSchema, name)` automatically wraps the schema
   with `MvccSchema.physical(userSchema)` so user code never sees `__xmin/__xmax`.
5. `Transaction.insert(name, record)` calls `MvccTable.mvccInsert` under the
   hood with the txn's xid.
6. `Transaction.update(name, ...)` and `delete(...)` route to `mvccUpdate /
   mvccDelete`. (UpdateWhere/DeleteWhere can be done as a scan + per-row
   update.)
7. `Transaction.query(name)` returns a QueryPlan whose scan filters by the
   txn's Snapshot.
8. `TransactionContext.getRecord(name, rid)` returns the user record (no
   stamps) for the txn's snapshot.
9. **`MvccTable` is no longer needed by callers** — fold its logic into
   `Database.TransactionContextImpl` or split into a clean internal helper.

### Non-goals for 3d

- DO NOT touch lock acquisition yet. Read still grabs S/IS, write still grabs
  X/IX. Phase 3e is where reads stop locking.
- DO NOT implement VACUUM / GC. Phase 5.
- DO NOT change index path (B+ tree). Indices keep referencing physical RecordIds
  for now; that means a stale index entry can briefly point at an old version
  but reads filter via Visibility so it's safe. Document as a known limitation.
- DO NOT touch ARIES recovery. Recovery transactions get xid 0 (or a special
  range) and bypass MVCC paths.

---

## TDD slices (make each commit green before the next)

### 3d.1 — Database owns a TransactionManager (~30 LoC)

**Tests (RED first):**
- `TestDatabaseMvcc#beginTransactionAllocatesXid` — two consecutive
  `db.beginTransaction()` calls return Transactions whose xids are monotonic.
- `TestDatabaseMvcc#commitMarksXidCommittedInManager` — after `t.commit()`,
  `db.getTransactionManager().isCommitted(t.getXid())` is true.
- `TestDatabaseMvcc#rollbackMarksXidAborted` — symmetric.

**Code:**
- Add `private final TransactionManager mvccTm = new TransactionManager()` field
  on `Database`.
- Expose `public TransactionManager getTransactionManager()` (package or test
  scope).
- Add `Transaction.getXid()` abstract method (or expose via getTransNum if you
  want to reuse — see Open Question 1 below).
- In `TransactionImpl`, call `mvccTm.begin()` in the constructor; store the xid.
- In `startCommit()` / `startRollback()`, call `mvccTm.commit/abort`.

**Gotcha:** `beginRecoveryTransaction` must NOT call `mvccTm.begin()` — recovery
xids are externally specified and may collide. Treat them as a separate xid
space (e.g., always xid 0 → bypasses MVCC visibility, "system always wins").

### 3d.2 — Snapshot captured at begin (~20 LoC)

**Tests:**
- `snapshotXidMatchesTransactionXid`
- `snapshotIsTakenAtBeginNotFirstQuery` — sequence: `t1 = beginTransaction(); t2
  = beginTransaction(); t2.commit();` → t1's snap should still see t2 as
  active.

**Code:**
- `TransactionImpl` constructor: `this.snapshot = mvccTm.snapshot(this.xid)`.
- Expose via `Transaction.getSnapshot()`.

### 3d.3 — `createTable` wraps schema automatically (~15 LoC)

**Tests:**
- `userSchemaDoesNotExposeStamps` — create a table with 2 columns, query its
  schema via `t.getSchema(name)`, expect 2 columns (not 5).
- `physicalSchemaPersists` — internal `getTable(name).getSchema()` has 5
  columns.

**Code:**
- `TransactionContextImpl.createTable(s, name)`: store the user schema in a
  per-table map; pass `MvccSchema.physical(s)` down to the underlying create.
- Override `getSchema(name)` to return the stored user schema.

**Open question:** does the user schema need to persist across DB restarts? If
yes, store it in `_metadata.tables` alongside the physical schema. If we punt
on this, document that "MVCC tables are not yet recoverable" as a known
limitation.

### 3d.4 — INSERT routes through MVCC (~20 LoC)

**Tests:**
- `insertVisibleToSelfAfterRouting` — `t.insert(name, rec); t.query(name)` sees it.
- `insertNotVisibleToConcurrentReader` — two concurrent transactions; reader
  doesn't see writer's pre-commit insert.

**Code:**
- `TransactionContextImpl.addRecord(name, rec)`: stamp via
  `MvccSchema.attachVersion(rec, xid)` then delegate to the underlying Table.
- `Transaction.insert(name, rec)` already calls `addRecord` so this should be
  enough.

### 3d.5 — getRecord/query filter by snapshot (~30 LoC)

**Tests:**
- `getRecordReturnsUserRecordOnly` — single-version row, get returns 2 cols
  not 5.
- `getRecordHidesUncommittedFromOthers`
- `queryFiltersByVisibility` — committed rows visible, uncommitted not.

**Code:**
- `TransactionContextImpl.getRecord(name, rid)`: walk the chain (lift logic from
  `MvccTable.mvccGetRecord`); strip stamps; return.
- `getRecordIterator(name)`: wrap the underlying iterator with a
  visibility-filtering decorator that strips stamps.
- `query(name)` builds QueryPlan from the same SequentialScan source which uses
  the wrapped iterator — should work transparently.

### 3d.6 — UPDATE / DELETE route through MVCC (~30 LoC)

**Tests:**
- `updateProducesNewVersionVisibleToSelf` (already passes for MvccTable —
  re-test against the public Transaction API).
- `concurrentUpdatesConflict` via `Transaction.updateRecord` (lift from
  TestMvccTableUpdate).
- `deleteHidesRow`.

**Code:**
- `TransactionContextImpl.updateRecord(name, rid, rec)`: lift `mvccUpdate` into
  here.
- `TransactionContextImpl.deleteRecord(name, rid)`: lift `mvccDelete`.

### 3d.7 — Cleanup

- Delete `MvccTable.java` (or move into the mvcc package as
  `MvccTableHelper` for internal reuse — pick one, don't leave both).
- Update or delete `TestMvccTable.java` and `TestMvccTableUpdate.java`. The new
  `TestDatabaseMvcc` tests cover the same behaviors via the public API; the old
  tests are now duplicating internal-class behavior.
- Ensure all 57 prior MVCC tests still pass (the pure-logic ones don't touch
  the integration so they should be unaffected).

---

## Open questions to resolve early in 3d

1. **Reuse `transNum` as `xid`, or add a parallel `xid` field?**
   `transNum` is already monotonic and Database manages it. Reusing it = one
   ID space, simpler. Risk: recovery transactions get `transNum` from the log
   and might confuse MVCC visibility. **Recommended:** reuse, but make
   recovery txns bypass MVCC by checking `recoveryTransaction` flag in the
   write path.

2. **Where does `TransactionManager` live?**
   On `Database`. One per Database. Rebuilt on `Database` construction.
   Recovery: replay log, then start TM with `nextXid = max(committed) + 1`.
   For 3d we punt on persistence — TM state is in-memory only. A crash
   aborts all in-flight txns (conservative-correct for them), BUT there is
   a real gap for already-committed pre-crash txns: their xids are
   forgotten, so any `xmin` stamp left on disk will fail
   `tm.isCommitted(xid)` and the row becomes invisible to post-recovery
   readers. **MVCC is not crash-safe in 3d** — document this as a known
   limitation and defer the fix to Phase 4.

   **Fix sketch for Phase 4 (committed-floor trick).** ARIES undo
   guarantees that every physical record remaining on disk after recovery
   belongs to a committed txn (uncommitted writes are physically rolled
   back). So after recovery, the TM can set
   `committedFloor = nextXidAtStartup` and have `isCommitted(xid)` return
   `true` for any `xid < committedFloor` without tracking them
   individually. Runtime txns get xids `>= committedFloor` and are
   tracked normally. Cheaper than persisting the full committed set in
   the WAL. (Recovery txns themselves still use xid 0 to bypass MVCC
   during replay, for the byte-level / circular-dependency reasons in
   [the earlier note](#gotcha-line-142).)

3. **What about the initialization transaction in `Database` constructor?**
   It creates `_metadata.tables` and `_metadata.indices`. Those tables should
   NOT be MVCC-wrapped (they have hand-rolled concurrency already). Special-case
   them in `createTable` by name prefix `_metadata`.

4. **Do scans return full physical records or filtered?**
   3d.5 says filter+strip. But `_metadata` table iterators must NOT filter.
   Easiest fix: only wrap the iterator if the table is a user table.

5. **What about `updateRecordWhere` and `deleteRecordWhere`?**
   Implement as scan-then-mutate using the existing visibility-filtered iterator
   plus per-row mvccUpdate/mvccDelete. Don't try to push predicates down yet.

6. **Thread safety of `claimLiveTip + updateRecord`.**
   The sequence is check-then-act: walk the chain to find the tip, then stamp
   xmax. Without serialization two writers could both read xmax==0, both
   stamp, and the second's `updateRecord` overwrites the first — chain
   pointer corruption. Today RookieDB's 2PL X page lock serializes
   `ctx.updateRecord`, so concurrent writers to the same page block rather
   than race; FUW conflict detection becomes mostly defensive. Phase 3e
   keeps X locks for writes (only S/IS reads are dropped), so this stays
   safe. **If a future phase ever goes lock-free on writes**, add either:
   - A short-duration page latch around `claimLiveTip + updateRecord`, OR
   - CAS on the xmax slot (atomic compare-and-swap that retries on change)
   PostgreSQL takes a page-level lightweight latch for the same reason
   during HOT updates. Note this in 3e's plan if/when written.

---

## Verification when done

1. `mvn test -Dtest='TestDatabaseMvcc' -P public` → all green.
2. `mvn test -Dtest='TestVisibility,TestTransactionManager,TestVersionStore,TestMvccSchema'`
   → still 40 green (pure logic untouched).
3. **Sanity check on existing tests:** `mvn test -Dtest='TestDatabase' -P public` —
   the basic Database tests should still pass. Locking tests (`TestDatabase2PL`)
   may break since semantics changed; expected failures, document them.
4. **Run the benchmark:**
   `mvn exec:java -Dexec.mainClass=edu.berkeley.cs186.database.benchmark.ConcurrencyBenchmark -Dexec.classpathScope=test 2pl-after-3d`
   The `2pl-after-3d` label appends a row to `results/benchmark.csv`. Compare
   to the `2pl` baseline. Throughput on Workload A/B should be ROUGHLY
   unchanged (MVCC overhead < 20%), Workload C may still deadlock until 3e.

---

## Known gotchas (collected from earlier phases)

- **`TransactionContext.setTransaction` throws if a TL is already set.** This
  is the cause of the Phaser leak we hit in 3c. Always pair set/unset
  symmetrically; tests should defensively call `unsetTransaction()` in
  `@After` to recover from any test that left state behind. See
  `TestMvccTableUpdate.tearDown` for the pattern.

- **`Phaser.register()` happens BEFORE `setTransaction()` in
  `Database.beginTransaction`.** If `setTransaction` throws, the Phaser is
  left +1 forever and `db.close()` will block in `waitAllTransactions()`.
  Defensive cleanup in tests is the workaround until we fix
  `Database.beginTransaction` to deregister on the throw path.

- **`Table.updateRecord` overwrites in place at the same RecordId.** The chain
  works because we ALSO call `addRecord` for new versions. Don't try to
  "tombstone-and-reinsert" in one call; that's two physical operations.

- **`Record.equals` compares values list, not bytes.** When testing equality of
  user records, both sides must have the same DataBox types. Use the same
  factory consistently.

- **`@Test(expected = WriteConflictException.class)` works** but make sure the
  exception isn't being swallowed inside the lock-release in
  `TransactionContext.close()` (no known issue but watch for it).

- **JUnit shares a thread across test methods in the same class.**
  ThreadLocals persist across `@Test` boundaries; `@After` is your only
  reliable cleanup hook. (We learned this the hard way in 3c.)

---

## After 3d

- **Phase 3e:** Read path drops S/IS lock acquisition; rerun benchmark, expect
  Workload C to stop deadlocking. This is the headline win for the resume.
- **Phase 4 (recovery):** make MVCC crash-safe. Minimum viable fix is the
  committed-floor trick described in Open Question 2 — after recovery,
  treat all `xid < nextXidAtStartup` as committed, relying on ARIES undo
  to guarantee on-disk xmin stamps belong to committed txns. Persisting
  the full TM state in the WAL is a heavier alternative if finer-grained
  tracking is needed later.
- **Phase 5 (VACUUM):** background or manual reclamation of dead versions.

The benchmark CSV (`results/benchmark.csv`) is the ground truth for "did MVCC
help" — keep appending labelled rows after each phase so the before/after
chart writes itself.
