package edu.berkeley.cs186.database.benchmark;

import edu.berkeley.cs186.database.Database;
import edu.berkeley.cs186.database.Transaction;
import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.concurrency.LockManager;
import edu.berkeley.cs186.database.databox.Type;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.RecordId;
import edu.berkeley.cs186.database.table.Schema;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Concurrency benchmark for measuring 2PL baseline performance.
 * Re-run after MVCC implementation to compare.
 *
 * Workloads:
 *   A - Read-heavy (95% read, 5% write)  — spread across many rows
 *   B - Balanced   (50% read, 50% write)  — spread across many rows
 *   C - Contended  (hot rows: N threads compete for small row set)
 *   D - Long read  (long scan + concurrent short writes)
 */
public class ConcurrencyBenchmark {
    private static final String TABLE_NAME = "bench";
    private static final int NUM_MEMORY_PAGES = 256;
    private static final int WARMUP_SECONDS = 2;
    private static final int MEASURE_SECONDS = 5;
    private static final int RUN_TIMEOUT_SECONDS = WARMUP_SECONDS + MEASURE_SECONDS + 10;

    private Database db;
    private Path tempDir;
    private List<RecordId> recordIds;
    private final Random rng = new Random(42);

    private static Schema benchSchema() {
        return new Schema()
                .add("id", Type.intType())
                .add("value", Type.intType())
                .add("padding", Type.stringType(64));
    }

    private static final String PADDING;
    static {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 64; i++) sb.append('x');
        PADDING = sb.toString();
    }

    private static Record makeRecord(int id, int value) {
        return new Record(id, value, PADDING);
    }

    // --- Setup / Teardown ---

    private void setup(int numRows) throws IOException {
        lastRunDeadlocked = false;
        tempDir = Files.createTempDirectory("rookiedb-bench-");
        LockManager lockManager = new LockManager();
        db = new Database(tempDir.toString(), NUM_MEMORY_PAGES, lockManager);
        db.setWorkMem(32);
        db.waitAllTransactions();

        recordIds = new ArrayList<>();
        try (Transaction t = db.beginTransaction()) {
            t.createTable(benchSchema(), TABLE_NAME);
            for (int i = 0; i < numRows; i++) {
                recordIds.add(
                    t.getTransactionContext().addRecord(TABLE_NAME, makeRecord(i, i))
                );
            }
        }
        db.waitAllTransactions();
    }

    private boolean lastRunDeadlocked = false;

    private void teardown() {
        if (db != null && !lastRunDeadlocked) {
            try { db.close(); } catch (Exception ignored) {}
        }
        // After deadlock, db.close() would hang — just abandon and delete files
        db = null;
        if (tempDir != null) {
            deleteDir(tempDir.toFile());
        }
    }

    private static void deleteDir(File dir) {
        File[] files = dir.listFiles();
        if (files != null) {
            for (File f : files) {
                if (f.isDirectory()) deleteDir(f);
                else f.delete();
            }
        }
        dir.delete();
    }

    // --- Result ---

    static class BenchmarkResult {
        final String workload;
        final int threads;
        final long totalTxns;
        final long abortedTxns;
        final long timedOut;
        final double throughput;
        final long p50ReadLatencyNs;
        final long p95ReadLatencyNs;
        final long p99ReadLatencyNs;
        final boolean deadlocked;

        BenchmarkResult(String workload, int threads, long totalTxns, long abortedTxns,
                        long timedOut, double throughput, long p50, long p95, long p99,
                        boolean deadlocked) {
            this.workload = workload;
            this.threads = threads;
            this.totalTxns = totalTxns;
            this.abortedTxns = abortedTxns;
            this.timedOut = timedOut;
            this.throughput = throughput;
            this.p50ReadLatencyNs = p50;
            this.p95ReadLatencyNs = p95;
            this.p99ReadLatencyNs = p99;
            this.deadlocked = deadlocked;
        }

        @Override
        public String toString() {
            if (deadlocked) {
                return String.format(
                    "%-12s threads=%-3d  ** DEADLOCKED ** (txns=%d before hang)",
                    workload, threads, totalTxns);
            }
            return String.format(
                "%-12s threads=%-3d txn/s=%-8.1f aborts=%-5d timeouts=%-5d p50=%-8s p95=%-8s p99=%-8s",
                workload, threads, throughput, abortedTxns, timedOut,
                formatNanos(p50ReadLatencyNs), formatNanos(p95ReadLatencyNs),
                formatNanos(p99ReadLatencyNs)
            );
        }

        static String formatNanos(long ns) {
            if (ns < 1_000) return ns + "ns";
            if (ns < 1_000_000) return String.format("%.1fus", ns / 1_000.0);
            return String.format("%.2fms", ns / 1_000_000.0);
        }

        static final String CSV_HEADER = "timestamp,label,workload,threads,totalTxns,abortedTxns,timedOut,throughput,p50ReadLatencyNs,p95ReadLatencyNs,p99ReadLatencyNs,deadlocked";

        String toCsvRow(String label, String timestamp) {
            return String.join(",",
                    timestamp, label, workload, String.valueOf(threads),
                    String.valueOf(totalTxns), String.valueOf(abortedTxns),
                    String.valueOf(timedOut), String.format(Locale.US, "%.2f", throughput),
                    String.valueOf(p50ReadLatencyNs), String.valueOf(p95ReadLatencyNs),
                    String.valueOf(p99ReadLatencyNs), String.valueOf(deadlocked));
        }
    }

    // --- Core runner ---

    @FunctionalInterface
    interface TxnTask {
        long execute(Database db, List<RecordId> rids, Random rng) throws Exception;
    }

    private BenchmarkResult runWorkload(String name, int numThreads, TxnTask task) {
        // Use daemon threads so deadlocked threads don't prevent JVM exit
        ThreadFactory daemonFactory = r -> {
            Thread t = new Thread(r);
            t.setDaemon(true);
            return t;
        };
        ExecutorService pool = Executors.newFixedThreadPool(numThreads, daemonFactory);
        AtomicLong committed = new AtomicLong();
        AtomicLong aborted = new AtomicLong();
        AtomicLong timedOut = new AtomicLong();
        ConcurrentLinkedQueue<Long> readLatencies = new ConcurrentLinkedQueue<>();
        AtomicLong runFlag = new AtomicLong(1);

        CountDownLatch warmupReady = new CountDownLatch(numThreads);
        CountDownLatch startSignal = new CountDownLatch(1);

        List<Future<?>> futures = new ArrayList<>();
        for (int t = 0; t < numThreads; t++) {
            final Random threadRng = new Random(rng.nextLong());
            futures.add(pool.submit(() -> {
                warmupReady.countDown();
                try { startSignal.await(); } catch (InterruptedException e) { return; }

                while (runFlag.get() == 1) {
                    try {
                        long lat = task.execute(db, recordIds, threadRng);
                        committed.incrementAndGet();
                        if (lat >= 0) readLatencies.add(lat);
                    } catch (Exception e) {
                        aborted.incrementAndGet();
                    }
                }
            }));
        }

        try {
            // Wait for threads to be ready
            if (!warmupReady.await(5, TimeUnit.SECONDS)) {
                runFlag.set(0);
                pool.shutdownNow();
                return new BenchmarkResult(name, numThreads, 0, 0, 0, 0, 0, 0, 0, true);
            }

            // Start all threads
            startSignal.countDown();

            // Warmup
            Thread.sleep(WARMUP_SECONDS * 1000L);

            // Reset counters for measurement
            committed.set(0);
            aborted.set(0);
            timedOut.set(0);
            readLatencies.clear();

            // Measure
            long startTime = System.nanoTime();
            Thread.sleep(MEASURE_SECONDS * 1000L);
            runFlag.set(0);
            long elapsed = System.nanoTime() - startTime;

            // Give threads 3 seconds to finish current txn
            pool.shutdown();
            boolean terminated = pool.awaitTermination(3, TimeUnit.SECONDS);

            if (!terminated) {
                lastRunDeadlocked = true;
                pool.shutdownNow();
            }

            double durationSec = elapsed / 1_000_000_000.0;
            long totalTxns = committed.get();
            double throughput = totalTxns / durationSec;
            long[] sorted = readLatencies.stream().mapToLong(Long::longValue).sorted().toArray();

            return new BenchmarkResult(name, numThreads, totalTxns, aborted.get(),
                    timedOut.get(), throughput,
                    percentile(sorted, 50), percentile(sorted, 95), percentile(sorted, 99),
                    !terminated);

        } catch (InterruptedException e) {
            pool.shutdownNow();
            return new BenchmarkResult(name, numThreads, 0, 0, 0, 0, 0, 0, 0, true);
        }
    }

    private static long percentile(long[] sorted, int pct) {
        if (sorted.length == 0) return 0;
        int idx = (int) Math.ceil(pct / 100.0 * sorted.length) - 1;
        return sorted[Math.max(0, Math.min(idx, sorted.length - 1))];
    }

    // --- Workload definitions ---

    /** Workload A: 95% read, 5% write — uniform random across all rows */
    private static final TxnTask WORKLOAD_A = (db, rids, rng) -> {
        boolean isRead = rng.nextInt(100) < 95;
        try (Transaction t = db.beginTransaction()) {
            if (isRead) {
                RecordId rid = rids.get(rng.nextInt(rids.size()));
                long start = System.nanoTime();
                t.getTransactionContext().getRecord(TABLE_NAME, rid);
                return System.nanoTime() - start;
            } else {
                RecordId rid = rids.get(rng.nextInt(rids.size()));
                t.getTransactionContext().updateRecord(TABLE_NAME, rid,
                        makeRecord(rng.nextInt(), rng.nextInt()));
                return -1;
            }
        }
    };

    /** Workload B: 50% read, 50% write — uniform random */
    private static final TxnTask WORKLOAD_B = (db, rids, rng) -> {
        boolean isRead = rng.nextBoolean();
        try (Transaction t = db.beginTransaction()) {
            if (isRead) {
                RecordId rid = rids.get(rng.nextInt(rids.size()));
                long start = System.nanoTime();
                t.getTransactionContext().getRecord(TABLE_NAME, rid);
                return System.nanoTime() - start;
            } else {
                RecordId rid = rids.get(rng.nextInt(rids.size()));
                t.getTransactionContext().updateRecord(TABLE_NAME, rid,
                        makeRecord(rng.nextInt(), rng.nextInt()));
                return -1;
            }
        }
    };

    /** Workload C: Hot-row contention — 10 hot rows, all threads compete */
    private static final TxnTask WORKLOAD_C = (db, rids, rng) -> {
        int hotRowIdx = rng.nextInt(Math.min(10, rids.size()));
        RecordId hotRow = rids.get(hotRowIdx);
        boolean isRead = rng.nextInt(100) < 80;
        try (Transaction t = db.beginTransaction()) {
            if (isRead) {
                long start = System.nanoTime();
                t.getTransactionContext().getRecord(TABLE_NAME, hotRow);
                return System.nanoTime() - start;
            } else {
                t.getTransactionContext().updateRecord(TABLE_NAME, hotRow,
                        makeRecord(hotRowIdx, rng.nextInt()));
                return -1;
            }
        }
    };

    /** Workload D: 30% long scans + 70% short writes */
    private static TxnTask workloadD(int scanSize) {
        return (db, rids, rng) -> {
            boolean isLongRead = rng.nextInt(100) < 30;
            try (Transaction t = db.beginTransaction()) {
                if (isLongRead) {
                    long start = System.nanoTime();
                    int startIdx = rng.nextInt(Math.max(1, rids.size() - scanSize));
                    for (int i = 0; i < scanSize && startIdx + i < rids.size(); i++) {
                        t.getTransactionContext().getRecord(TABLE_NAME, rids.get(startIdx + i));
                    }
                    return System.nanoTime() - start;
                } else {
                    RecordId rid = rids.get(rng.nextInt(rids.size()));
                    t.getTransactionContext().updateRecord(TABLE_NAME, rid,
                            makeRecord(rng.nextInt(), rng.nextInt()));
                    return -1;
                }
            }
        };
    }

    // --- Main ---

    public List<BenchmarkResult> runAll(PrintStream out) throws Exception {
        int numRows = 10_000;
        // 8 threads omitted: 2PL deadlocks or mass-aborts at this concurrency
        // (itself a strong argument for MVCC). Will re-enable post-MVCC.
        int[] threadCounts = {1, 2, 4};
        List<BenchmarkResult> results = new ArrayList<>();

        out.println("=== RookieDB Concurrency Benchmark (2PL Baseline) ===");
        out.println("Rows: " + numRows + " | Warmup: " + WARMUP_SECONDS
                + "s | Measure: " + MEASURE_SECONDS + "s");
        out.println();

        for (String workload : new String[]{"A", "B", "C", "D"}) {
            out.println("--- Workload " + workload + " ---");
            out.flush();
            for (int threads : threadCounts) {
                setup(numRows);
                try {
                    TxnTask task;
                    switch (workload) {
                        case "A": task = WORKLOAD_A; break;
                        case "B": task = WORKLOAD_B; break;
                        case "C": task = WORKLOAD_C; break;
                        case "D": task = workloadD(50); break;
                        default: throw new IllegalStateException();
                    }
                    BenchmarkResult result = runWorkload(workload, threads, task);
                    results.add(result);
                    out.println(result);
                    out.flush();
                } finally {
                    teardown();
                }
            }
            out.println();
            out.flush();
        }
        return results;
    }

    private static void writeCsv(List<BenchmarkResult> results, String label, Path csvPath) throws IOException {
        boolean writeHeader = !Files.exists(csvPath) || Files.size(csvPath) == 0;
        Files.createDirectories(csvPath.getParent());

        try (PrintWriter pw = new PrintWriter(Files.newBufferedWriter(csvPath,
                StandardOpenOption.CREATE, StandardOpenOption.APPEND))) {
            if (writeHeader) {
                pw.println(BenchmarkResult.CSV_HEADER);
            }
            String timestamp = LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME);
            for (BenchmarkResult r : results) {
                pw.println(r.toCsvRow(label, timestamp));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        String label = (args.length > 0) ? args[0] : "2pl";
        Path csvPath = Path.of("results", "benchmark.csv");

        ConcurrencyBenchmark bench = new ConcurrencyBenchmark();
        List<BenchmarkResult> results = bench.runAll(System.out);
        writeCsv(results, label, csvPath);

        System.out.println("Results appended to " + csvPath.toAbsolutePath());
    }
}
