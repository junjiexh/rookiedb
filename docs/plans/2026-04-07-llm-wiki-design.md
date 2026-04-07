# RookieDB LLM Wiki 设计文档

**日期：** 2026-04-07
**目的：** 为 rookiedb 构建 LLM 维护的结构化知识库，兼顾学习参考与开发辅助

## 背景

基于 Karpathy 的 llm-wiki 模式：LLM 增量构建和维护一个持久化的、互链的 markdown wiki，
而非每次会话从零理解代码。知识编译一次、持续更新，交叉引用和矛盾标记已预先完成。

## 架构

### 三层结构

1. **Raw sources（源码）** — rookiedb 源代码和现有文档，不可修改
2. **Wiki** — `wiki/` 目录下的 LLM 生成的 markdown 文件集合
3. **Schema** — `wiki/SCHEMA.md`，定义 wiki 约定和 LLM 操作工作流

### 存放位置

- 项目内 `wiki/` 目录，纳入 git 版本控制
- 所有文件平铺在 `wiki/` 下，不使用子目录（符合 Obsidian 无文件夹习惯）

### 语言

- 中文解释 + 英文术语/代码引用（中英混合）

## 文件结构

```
wiki/
├── SCHEMA.md              # LLM 操作手册
├── index.md               # 页面索引（按类别分组）
├── log.md                 # 操作时间线日志
├── overview.md            # 项目全景概览
│
│  # 组件页面
├── disk-space-manager.md
├── buffer-manager.md
├── eviction-policies.md
├── table-and-records.md
├── b-plus-tree.md
├── query-operators.md
├── join-algorithms.md
├── query-optimizer.md
├── sql-parser.md
├── lock-manager.md
├── concurrency-control.md
├── aries-recovery.md
├── transaction-lifecycle.md
│
│  # 概念页面
├── write-ahead-logging.md
├── multi-granularity-locking.md
├── external-sorting.md
├── page-layout.md
│
│  # 设计决策页面
├── decision-antlr4-migration.md
├── decision-visitor-pattern.md
│
│  # Roadmap 页面
├── roadmap-mvcc.md
├── roadmap-network-layer.md
├── roadmap-sql-extensions.md
```

总计约 25 个文件。

## 页面模板

每个 wiki 页面遵循统一格式：

```markdown
---
title: 页面标题
category: component | concept | decision | roadmap
related: [相关页面列表]
source_files: [对应源文件列表]
last_updated: YYYY-MM-DD
---

# 页面标题

## 概述
一段话说清楚是什么、解决什么问题。

## 核心机制
详细技术内容。

## 关键代码路径
引用具体文件和行号。

## 与其他模块的关系
用 [[wikilinks]] 链接。

## 设计权衡与备注
非显而易见的设计决策。
```

## SCHEMA.md 设计

### 约定规则

- 语言：中文解释 + 英文术语/代码引用
- 链接：使用 `[[page-name]]` wikilinks
- Frontmatter：必须包含 title, category, related, source_files, last_updated
- 命名：文件名用 kebab-case
- 代码引用：`file_path:line_number` 格式
- 不复制大段源码，解释设计意图并指向源文件
- `related` 字段双向维护

### 核心操作工作流

| 操作 | 触发场景 | 流程 |
|------|---------|------|
| **Ingest** | 新增源码/文档/功能 | 读源码 → 创建/更新 wiki 页 → 更新 index.md → 追加 log.md |
| **Query** | 用户提问 | 查 index → 读相关页 → 综合回答 → 有价值的回答可存为新页面 |
| **Lint** | 定期维护 | 检查孤立页、过期引用、缺失交叉链接、source_files 是否存在 |

### 与 CLAUDE.md 集成

在项目 CLAUDE.md 中添加引用，让 Claude Code 知道 wiki 的存在并可按需查阅。

## 初始页面规划

### 概览（1 页）
- `overview.md` — 系统分层架构、模块依赖、数据流

### 组件页面（13 页）
- `disk-space-manager` — 虚拟页编号、分区文件布局
- `buffer-manager` — 页面缓存、pin/unpin、脏页刷写
- `eviction-policies` — Clock vs LRU 算法对比
- `table-and-records` — 堆文件、PageDirectory、Record 布局
- `b-plus-tree` — B+ 树操作、持久化
- `query-operators` — 算子体系
- `join-algorithms` — SNLJ/BNLJ/Sort-Merge/GHJ
- `query-optimizer` — 代价模型、join 顺序选择
- `sql-parser` — ANTLR4 语法、Handler 流程
- `lock-manager` — 锁类型、兼容矩阵
- `concurrency-control` — 多粒度锁协议
- `aries-recovery` — WAL、三阶段恢复
- `transaction-lifecycle` — Transaction 状态机

### 概念页面（4 页）
- `write-ahead-logging` — WAL 协议原理
- `multi-granularity-locking` — 多粒度锁理论
- `external-sorting` — 外部排序算法
- `page-layout` — 页面布局与空间管理

### 设计决策页面（2 页）
- `decision-antlr4-migration` — JavaCC → ANTLR4 迁移
- `decision-visitor-pattern` — Visitor 模式选择

### Roadmap 页面（3 页）
- `roadmap-mvcc` — MVCC/Snapshot Isolation
- `roadmap-network-layer` — 客户端-服务器网络层
- `roadmap-sql-extensions` — SQL 扩展
