# Phase 3: SQL 语法扩展

**状态**: 待开始
**预估**: 约 3 周
**前置**: Phase 1 (MVCC) ✅, Phase 2 (网络层) 可并行

## 目标

利用 ANTLR4 灵活性，扩展 SQL 语法能力。

## 任务清单

- [ ] **3.1 通用 WHERE 表达式** — 现在只支持 `col op value`，扩展为任意 expression（`WHERE sid > 10 AND gpa < 3.0 OR major = 'CS'`）
- [ ] **3.2 HAVING** — `GROUP BY col HAVING COUNT(*) > 5`
- [ ] **3.3 标量子查询** — `WHERE col > (SELECT MAX(col) FROM ...)`
- [ ] **3.4 IN 子查询** — `WHERE col IN (SELECT col FROM ...)`
- [ ] **3.5 FROM 子查询** — `FROM (SELECT ...) AS subq`
- [ ] **3.6 UNION / UNION ALL** — 合并两个 SELECT 结果
- [ ] **3.7 测试** — 每个新语法的正确性测试 + 边界情况

## 不做

- Window Functions — 实现复杂但面试很少深入问
- 关联子查询 (correlated subquery) — 优化器改动太大

## 改动范围

- 修改 `RookieSQL.g4`（新增语法规则）
- 新增查询算子（SubqueryOperator、UnionOperator）
- 修改 `SelectStatementHandler` + `QueryPlan`
- 新增测试

## 面试话题

- 子查询的执行策略（materialization vs streaming）
- 去关联化 (decorrelation) 的思路
- UNION 实现（排序去重 vs hash 去重）
