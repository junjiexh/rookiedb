# RookieDB LLM Wiki Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Build a persistent, interlinked LLM-maintained wiki for the rookiedb project covering all core modules.

**Architecture:** Flat `wiki/` directory with ~25 markdown files using wikilinks for cross-referencing. SCHEMA.md defines conventions and workflows. index.md provides navigation. Each page follows a unified template with YAML frontmatter.

**Tech Stack:** Markdown with YAML frontmatter, wikilinks (`[[page-name]]`), Obsidian-compatible

---

### Task 1: Create wiki directory and SCHEMA.md

**Files:**
- Create: `wiki/SCHEMA.md`

**Step 1: Create the wiki directory**

```bash
mkdir -p wiki
```

**Step 2: Write SCHEMA.md**

Create `wiki/SCHEMA.md` with the complete LLM operation manual. Content must include:

1. **项目概述** — RookieDB 是什么，wiki 的目的
2. **约定规则：**
   - 语言：中文解释 + 英文术语/代码引用
   - 链接：`[[page-name]]` wikilinks，不用相对路径
   - Frontmatter：每页必须有 `title, category, related, source_files, last_updated`
   - 命名：文件名 kebab-case
   - 代码引用：`file_path:line_number` 格式
   - 不复制大段源码，解释设计意图并指向源文件
   - `related` 字段双向维护：A 引用 B 时，B 的 related 也加上 A
3. **页面模板** — 完整的 frontmatter + section 模板（概述、核心机制、关键代码路径、模块关系、设计权衡）
4. **页面类别** — component, concept, decision, roadmap
5. **三个核心操作工作流：**
   - **Ingest**：读源码 → 创建/更新 wiki 页 → 更新 index.md → 追加 log.md
   - **Query**：查 index → 读相关页 → 综合回答 → 有价值的回答可存为新页面
   - **Lint**：检查孤立页、过期引用、缺失交叉链接、source_files 是否存在
6. **质量规则：**
   - 每页应自包含
   - 代码引用用 `file_path:line_number`
   - 概念页面要与具体实现交叉引用

**Step 3: Commit**

```bash
git add wiki/SCHEMA.md
git commit -m "docs(wiki): add SCHEMA.md — LLM operation manual for wiki maintenance"
```

---

### Task 2: Create overview.md

**Files:**
- Create: `wiki/overview.md`
- Read: `Database.java`, `Transaction.java`, `TransactionContext.java`

**Step 1: Read core orchestration files**

Read these files to understand the system architecture:
- `src/main/java/edu/berkeley/cs186/database/Database.java`
- `src/main/java/edu/berkeley/cs186/database/Transaction.java`

**Step 2: Write overview.md**

Create `wiki/overview.md` with:

- Frontmatter: `title: RookieDB 项目概览`, `category: component`, `related: [所有组件页]`
- **系统分层架构图**（ASCII art）：
  ```
  SQL → Parser → QueryPlan → Operators → Table/Index → Buffer → Disk
  ```
  加上并发控制和恢复的横切关注点
- **模块依赖关系** — 哪个模块依赖哪个
- **数据流** — 一条 SQL 从输入到执行的完整路径
- **技术栈** — Java 25, Maven, ANTLR4, JUnit 4
- 链接到所有组件页面用 `[[wikilinks]]`

**Step 3: Commit**

```bash
git add wiki/overview.md
git commit -m "docs(wiki): add overview page — system architecture and module relationships"
```

---

### Task 3: Create storage layer pages (disk-space-manager, buffer-manager, eviction-policies, page-layout)

**Files:**
- Create: `wiki/disk-space-manager.md`, `wiki/buffer-manager.md`, `wiki/eviction-policies.md`, `wiki/page-layout.md`
- Read:
  - `src/main/java/edu/berkeley/cs186/database/io/DiskSpaceManager.java`
  - `src/main/java/edu/berkeley/cs186/database/io/DiskSpaceManagerImpl.java`
  - `src/main/java/edu/berkeley/cs186/database/io/PartitionHandle.java`
  - `src/main/java/edu/berkeley/cs186/database/memory/BufferManager.java`
  - `src/main/java/edu/berkeley/cs186/database/memory/BufferFrame.java`
  - `src/main/java/edu/berkeley/cs186/database/memory/Page.java`
  - `src/main/java/edu/berkeley/cs186/database/memory/ClockEvictionPolicy.java`
  - `src/main/java/edu/berkeley/cs186/database/memory/LRUEvictionPolicy.java`
  - `src/main/java/edu/berkeley/cs186/database/memory/EvictionPolicy.java`
  - `docs/design/disk-space-manager.md` (if exists)
  - `docs/design/buffer-manager.md` (if exists)

**Step 1: Read all storage layer source files**

Read the files listed above to understand the storage layer implementation.

**Step 2: Write disk-space-manager.md**

Content:
- 虚拟页编号方案：`PartitionNum * 10^10 + DataPageNum`
- 分区文件布局：Master Page → Header Pages → Data Pages
- Master page 结构（2048 个 16-bit 计数器）
- Header page（bitmap for 32K pages）
- 分配策略
- 容量限制（~64M pages/partition, ~256GB）

**Step 3: Write buffer-manager.md**

Content:
- 页面缓存架构（固定大小 buffer frame 数组）
- BufferFrame 结构：4096-byte buffer, page number, dirty flag, pin count, lock
- 有效页面大小：4060 bytes（36 bytes reserved for recovery LSN）
- Page 生命周期：fetchPage → read/write → unpin → eviction
- 脏页刷写策略（write-back）
- 与 recovery 的交互（pageFlushHook）

**Step 4: Write eviction-policies.md**

Content:
- EvictionPolicy 接口
- Clock 算法：second-chance，use bit，O(1) overhead
- LRU 算法：exact least-recently-used
- 对比分析

**Step 5: Write page-layout.md**

Content:
- 页面大小（4096 bytes，有效 4060）
- 数据页布局：bitmap + fixed-size records
- `numRecordsPerPage` 计算公式
- Recovery LSN 的 36-byte 预留

**Step 6: Commit**

```bash
git add wiki/disk-space-manager.md wiki/buffer-manager.md wiki/eviction-policies.md wiki/page-layout.md
git commit -m "docs(wiki): add storage layer pages — disk, buffer, eviction, page layout"
```

---

### Task 4: Create table and index pages (table-and-records, b-plus-tree)

**Files:**
- Create: `wiki/table-and-records.md`, `wiki/b-plus-tree.md`
- Read:
  - `src/main/java/edu/berkeley/cs186/database/table/Table.java`
  - `src/main/java/edu/berkeley/cs186/database/table/PageDirectory.java`
  - `src/main/java/edu/berkeley/cs186/database/table/Record.java`
  - `src/main/java/edu/berkeley/cs186/database/table/RecordId.java`
  - `src/main/java/edu/berkeley/cs186/database/table/Schema.java`
  - `src/main/java/edu/berkeley/cs186/database/index/BPlusTree.java`
  - `src/main/java/edu/berkeley/cs186/database/index/BPlusNode.java`
  - `src/main/java/edu/berkeley/cs186/database/index/InnerNode.java`
  - `src/main/java/edu/berkeley/cs186/database/index/LeafNode.java`
  - `src/main/java/edu/berkeley/cs186/database/index/BPlusTreeMetadata.java`
  - `docs/design/table-record-layout.md` (if exists)

**Step 1: Read source files**

**Step 2: Write table-and-records.md**

Content:
- 堆文件组织
- PageDirectory 结构（linked-list header pages tracking free space）
- Record 布局（bitmap validity + fixed-size records）
- Schema 定义与 DataBox 类型系统
- 复杂度分析：Insert O(H×E+R), Get O(1), Delete O(R+H)
- RecordId：(pageNum, entryNum)

**Step 3: Write b-plus-tree.md**

Content:
- B+ 树结构：BPlusTree, InnerNode, LeafNode
- 操作：put, get, remove, scan (scanEqual, scanGreaterEqual, scanAll)
- 持久化机制
- BPlusTreeMetadata
- 与 LockUtil 的交互

**Step 4: Commit**

```bash
git add wiki/table-and-records.md wiki/b-plus-tree.md
git commit -m "docs(wiki): add table/index pages — heap files, PageDirectory, B+ tree"
```

---

### Task 5: Create query layer pages (query-operators, join-algorithms, query-optimizer, external-sorting)

**Files:**
- Create: `wiki/query-operators.md`, `wiki/join-algorithms.md`, `wiki/query-optimizer.md`, `wiki/external-sorting.md`
- Read:
  - `src/main/java/edu/berkeley/cs186/database/query/QueryOperator.java`
  - `src/main/java/edu/berkeley/cs186/database/query/QueryPlan.java`
  - `src/main/java/edu/berkeley/cs186/database/query/SequentialScanOperator.java`
  - `src/main/java/edu/berkeley/cs186/database/query/SelectOperator.java`
  - `src/main/java/edu/berkeley/cs186/database/query/ProjectOperator.java`
  - `src/main/java/edu/berkeley/cs186/database/query/SortOperator.java`
  - `src/main/java/edu/berkeley/cs186/database/query/GroupByOperator.java`
  - `src/main/java/edu/berkeley/cs186/database/query/LimitOperator.java`
  - `src/main/java/edu/berkeley/cs186/database/query/IndexScanOperator.java`
  - `src/main/java/edu/berkeley/cs186/database/query/join/SNLJOperator.java`
  - `src/main/java/edu/berkeley/cs186/database/query/join/BNLJOperator.java`
  - `src/main/java/edu/berkeley/cs186/database/query/join/SortMergeOperator.java`
  - `src/main/java/edu/berkeley/cs186/database/query/join/GHJOperator.java`
  - `src/main/java/edu/berkeley/cs186/database/query/join/SHJOperator.java`
  - `src/main/java/edu/berkeley/cs186/database/query/join/PNLJOperator.java`

**Step 1: Read query layer source files**

**Step 2: Write query-operators.md**

Content:
- QueryOperator 基类与算子层次
- 各算子职责：SequentialScan, IndexScan, Select, Project, Sort, GroupBy, Limit, Materialize
- BacktrackingIterator 模式
- 算子的 execute() 模板方法

**Step 3: Write join-algorithms.md**

Content:
- 6 种 join 实现的原理与 I/O 代价分析：
  - SNLJ (Simple Nested Loop Join) — O(N×M)
  - PNLJ (Page Nested Loop Join)
  - BNLJ (Block Nested Loop Join) — O(N + N×M/B)
  - SHJ (Simple Hash Join)
  - Sort-Merge Join — O(N log N + M log M + N+M)
  - GHJ (Grace Hash Join) — O(N+M) with partitioning
- 适用场景对比

**Step 4: Write query-optimizer.md**

Content:
- QueryPlan 的优化流程
- 代价模型（estimated I/O cost）
- Join 顺序选择
- 算子选择策略
- 统计信息（TableStats, Histogram）

**Step 5: Write external-sorting.md**

Content:
- 外部归并排序原理
- SortOperator 的实现
- Run 和 Partition 辅助类
- buffer 数量对排序趟数的影响

**Step 6: Commit**

```bash
git add wiki/query-operators.md wiki/join-algorithms.md wiki/query-optimizer.md wiki/external-sorting.md
git commit -m "docs(wiki): add query layer pages — operators, joins, optimizer, external sort"
```

---

### Task 6: Create concurrency and recovery pages (lock-manager, concurrency-control, multi-granularity-locking, aries-recovery, write-ahead-logging, transaction-lifecycle)

**Files:**
- Create: `wiki/lock-manager.md`, `wiki/concurrency-control.md`, `wiki/multi-granularity-locking.md`, `wiki/aries-recovery.md`, `wiki/write-ahead-logging.md`, `wiki/transaction-lifecycle.md`
- Read:
  - `src/main/java/edu/berkeley/cs186/database/concurrency/LockManager.java`
  - `src/main/java/edu/berkeley/cs186/database/concurrency/LockContext.java`
  - `src/main/java/edu/berkeley/cs186/database/concurrency/LockType.java`
  - `src/main/java/edu/berkeley/cs186/database/concurrency/LockUtil.java`
  - `src/main/java/edu/berkeley/cs186/database/concurrency/Lock.java`
  - `src/main/java/edu/berkeley/cs186/database/concurrency/ResourceName.java`
  - `src/main/java/edu/berkeley/cs186/database/recovery/ARIESRecoveryManager.java`
  - `src/main/java/edu/berkeley/cs186/database/recovery/LogManager.java`
  - `src/main/java/edu/berkeley/cs186/database/recovery/LogRecord.java`
  - `src/main/java/edu/berkeley/cs186/database/recovery/TransactionTableEntry.java`
  - `src/main/java/edu/berkeley/cs186/database/Transaction.java`
  - `src/main/java/edu/berkeley/cs186/database/Database.java` (transaction lifecycle sections)

**Step 1: Read all concurrency and recovery source files**

**Step 2: Write lock-manager.md**

Content:
- LockManager 架构
- LockType: IS, IX, S, X, SIX — 兼容矩阵
- LockRequest 和等待队列
- Lock acquire/release/promote 流程

**Step 3: Write concurrency-control.md**

Content:
- LockContext 层级结构（database → table → page）
- ResourceName 编码
- LockUtil 自动获取祖先锁
- 锁升级（S→X, IS→IX 等）
- 与 Transaction 的交互

**Step 4: Write multi-granularity-locking.md**

Content:
- 多粒度锁理论
- 意向锁的必要性
- 兼容矩阵推导
- 与实际实现的对应关系

**Step 5: Write aries-recovery.md**

Content:
- ARIESRecoveryManager 架构
- 三阶段恢复：Analysis → Redo → Undo
- Transaction table 和 dirty page table
- Checkpoint 机制（fuzzy checkpoint）
- Log record 类型（16 种）

**Step 6: Write write-ahead-logging.md**

Content:
- WAL 协议原理
- LSN（Log Sequence Number）机制
- Log record 结构
- 与 BufferManager 的 pageFlushHook 协作
- 持久性保证

**Step 7: Write transaction-lifecycle.md**

Content:
- Transaction 状态机：RUNNING → COMMITTING → COMPLETE / ABORTING → COMPLETE
- begin/commit/abort 流程
- Transaction 与 LockContext、RecoveryManager 的交互
- Savepoint 机制

**Step 8: Commit**

```bash
git add wiki/lock-manager.md wiki/concurrency-control.md wiki/multi-granularity-locking.md wiki/aries-recovery.md wiki/write-ahead-logging.md wiki/transaction-lifecycle.md
git commit -m "docs(wiki): add concurrency/recovery pages — locking, ARIES, WAL, transactions"
```

---

### Task 7: Create SQL parser page (sql-parser)

**Files:**
- Create: `wiki/sql-parser.md`
- Read:
  - `src/main/antlr4/edu/berkeley/cs186/database/cli/parser/RookieSQL.g4`
  - `src/main/java/edu/berkeley/cs186/database/cli/handler/StatementListHandler.java`
  - `src/main/java/edu/berkeley/cs186/database/cli/handler/SelectStatementHandler.java`
  - `src/main/java/edu/berkeley/cs186/database/cli/handler/ExecutableStatementHandler.java`
  - `src/main/java/edu/berkeley/cs186/database/cli/handler/StatementHandler.java`
  - `src/main/java/edu/berkeley/cs186/database/query/expr/ExpressionBuilder.java`
  - `src/main/java/edu/berkeley/cs186/database/cli/CommandLineInterface.java`

**Step 1: Read parser and handler source files**

**Step 2: Write sql-parser.md**

Content:
- ANTLR4 语法概览（RookieSQL.g4 支持的 SQL 子集）
- 解析流程：SQL text → Lexer → Parser → Parse Tree → Visitor/Handler → QueryPlan
- StatementListHandler 分发机制
- 各 Handler 职责（Select, Insert, Update, Delete, Create, Drop, Begin, Commit, etc.）
- ExpressionBuilder：表达式求值（算术、比较、逻辑、聚合）
- 支持的 SQL 特性清单：DDL, DML, TCL, EXPLAIN, CTE, JOIN

**Step 3: Commit**

```bash
git add wiki/sql-parser.md
git commit -m "docs(wiki): add SQL parser page — ANTLR4 grammar, handlers, expression evaluation"
```

---

### Task 8: Create design decision pages

**Files:**
- Create: `wiki/decision-antlr4-migration.md`, `wiki/decision-visitor-pattern.md`
- Read:
  - `docs/plans/2026-03-23-antlr4-migration-design.md`
  - `docs/design/visitor-pattern-decision.md` (if exists, may be deleted per git status)

**Step 1: Read existing design documents**

**Step 2: Write decision-antlr4-migration.md**

Content:
- 迁移动机：JavaCC/JJTree → ANTLR4
- 方案对比
- 迁移过程
- 结果：1:1 feature parity，更好的可维护性

**Step 3: Write decision-visitor-pattern.md**

Content:
- 为什么选择 Visitor 模式处理 SQL 语句
- Handler 命名约定（*StatementHandler 而非 *Visitor）
- 与 ANTLR4 生成代码的交互方式

**Step 4: Commit**

```bash
git add wiki/decision-antlr4-migration.md wiki/decision-visitor-pattern.md
git commit -m "docs(wiki): add design decision pages — ANTLR4 migration, visitor pattern"
```

---

### Task 9: Create roadmap pages

**Files:**
- Create: `wiki/roadmap-mvcc.md`, `wiki/roadmap-network-layer.md`, `wiki/roadmap-sql-extensions.md`
- Read:
  - `docs/plans/2026-03-23-roadmap.md`
  - `docs/todo/phase2-network-layer.md`
  - `docs/todo/phase3-sql-extensions.md`
  - `docs/todo/phase4-observability-benchmark.md`
  - `docs/todo/phase5-documentation.md`

**Step 1: Read roadmap and TODO documents**

**Step 2: Write roadmap-mvcc.md**

Content:
- Phase 1 目标：Snapshot Isolation 替换多粒度锁
- 版本链设计（create_txn_id / delete_txn_id）
- 事务快照（active txn set）
- 写冲突检测（First-Committer-Wins）
- 垃圾回收
- 保留 2PL 作为 benchmark 选项

**Step 3: Write roadmap-network-layer.md**

Content:
- Phase 2 目标：TCP 客户端-服务器架构
- Wire protocol 设计
- Connection handler
- Python 客户端
- 优雅关闭

**Step 4: Write roadmap-sql-extensions.md**

Content:
- Phase 3 目标：SQL 功能扩展
- 子查询（WHERE IN, FROM subquery）
- HAVING 子句
- UNION / UNION ALL
- 通用 WHERE 表达式

**Step 5: Commit**

```bash
git add wiki/roadmap-mvcc.md wiki/roadmap-network-layer.md wiki/roadmap-sql-extensions.md
git commit -m "docs(wiki): add roadmap pages — MVCC, network layer, SQL extensions"
```

---

### Task 10: Create index.md and log.md

**Files:**
- Create: `wiki/index.md`, `wiki/log.md`

**Step 1: Write index.md**

List all wiki pages organized by category with one-line summaries and `[[wikilinks]]`:

```markdown
# RookieDB Wiki Index

## 概览
- [[overview]] — 项目全景架构、模块关系与数据流

## 组件
- [[disk-space-manager]] — 虚拟页编号、分区文件布局、磁盘分配策略
- [[buffer-manager]] — 内存页面缓存、pin/unpin、脏页刷写
- [[eviction-policies]] — Clock vs LRU 淘汰算法对比
- [[table-and-records]] — 堆文件、PageDirectory、Record 布局与 Schema
- [[b-plus-tree]] — B+ 树索引：put/get/remove/scan 操作与持久化
- [[query-operators]] — 查询算子体系：Scan/Select/Project/Sort/GroupBy/Limit
- [[join-algorithms]] — 六种 Join 算法的原理与 I/O 代价分析
- [[query-optimizer]] — 代价模型、Join 顺序选择、算子选择策略
- [[sql-parser]] — ANTLR4 语法、Handler 分发、表达式求值
- [[lock-manager]] — 锁类型(IS/IX/S/X/SIX)、兼容矩阵、等待队列
- [[concurrency-control]] — LockContext 层级、锁升级、自动祖先锁
- [[aries-recovery]] — ARIES 三阶段恢复：Analysis/Redo/Undo
- [[transaction-lifecycle]] — Transaction 状态机与 begin/commit/abort 流程

## 概念
- [[write-ahead-logging]] — WAL 协议、LSN 机制与持久性保证
- [[multi-granularity-locking]] — 多粒度锁理论与意向锁
- [[external-sorting]] — 外部归并排序与 buffer 管理
- [[page-layout]] — 页面大小、数据页布局与空间计算

## 设计决策
- [[decision-antlr4-migration]] — 从 JavaCC/JJTree 迁移到 ANTLR4
- [[decision-visitor-pattern]] — Visitor 模式与 Handler 命名约定

## Roadmap
- [[roadmap-mvcc]] — Phase 1: MVCC/Snapshot Isolation
- [[roadmap-network-layer]] — Phase 2: TCP 客户端-服务器架构
- [[roadmap-sql-extensions]] — Phase 3: 子查询、HAVING、UNION
```

**Step 2: Write log.md**

Initialize with the first entry:

```markdown
# RookieDB Wiki Log

## [2026-04-07] init | Wiki 初始化构建
- 创建 SCHEMA.md 操作手册
- 创建 overview.md 项目概览
- 创建 13 个组件页面
- 创建 4 个概念页面
- 创建 2 个设计决策页面
- 创建 3 个 Roadmap 页面
- 创建 index.md 和 log.md
```

**Step 3: Commit**

```bash
git add wiki/index.md wiki/log.md
git commit -m "docs(wiki): add index and log — wiki navigation and changelog"
```

---

### Task 11: Verify cross-references and lint

**Step 1: Verify all wikilinks resolve**

Check that every `[[page-name]]` in all wiki files points to an existing `wiki/page-name.md` file.

```bash
# Extract all wikilinks and check they resolve
grep -roh '\[\[[a-z-]*\]\]' wiki/ | sort -u | sed 's/\[\[//;s/\]\]//' | while read page; do
  [ ! -f "wiki/$page.md" ] && echo "BROKEN: $page"
done
```

Expected: no output (all links resolve).

**Step 2: Verify all related fields are bidirectional**

For each page, check that if page A lists B in `related`, then B also lists A.

**Step 3: Verify frontmatter completeness**

Check every wiki page (except SCHEMA.md, index.md, log.md) has all required frontmatter fields.

**Step 4: Fix any issues found**

**Step 5: Commit fixes if any**

```bash
git add wiki/
git commit -m "docs(wiki): fix cross-references and frontmatter consistency"
```

---

### Task 12: Add CLAUDE.md integration

**Files:**
- Modify: project-level CLAUDE.md (create if not exists)

**Step 1: Check for existing project CLAUDE.md**

```bash
ls -la /Users/zhongjunjiexiansheng/junjie-dev/rookiedb/CLAUDE.md
```

**Step 2: Add wiki reference to CLAUDE.md**

Add a section pointing to the wiki:

```markdown
## 项目知识库

本项目有一个 LLM 维护的结构化知识库，位于 `wiki/` 目录。

- `wiki/SCHEMA.md` — wiki 的操作约定和工作流
- `wiki/index.md` — 所有页面的分类索引
- 在需要了解项目架构或特定模块时，先查阅 wiki 相关页面
```

**Step 3: Commit**

```bash
git add CLAUDE.md
git commit -m "docs: add wiki reference to CLAUDE.md for LLM context"
```
