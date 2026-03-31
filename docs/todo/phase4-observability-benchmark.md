# Phase 4: 可观测性 + 基准测试

**状态**: 待开始
**预估**: 约 2 周
**前置**: Phase 1 (MVCC) ✅

## 目标

用数据说话，从"学生项目"升级为"工程项目"。

## 任务清单

- [ ] **4.1 查询执行统计** — 每个查询记录 I/O 次数、buffer pool hit rate、扫描行数
- [ ] **4.2 EXPLAIN ANALYZE** — 在现有 EXPLAIN 基础上附加实际执行统计
- [ ] **4.3 慢查询日志** — 超阈值查询记录 SQL 文本、执行时间、扫描行数
- [ ] **4.4 BufferManager 指标暴露** — hit rate、eviction count 等
- [ ] **4.5 Benchmark 框架** — 标准化测试脚本 + 数据生成器
- [ ] **4.6 MVCC vs 2PL 吞吐量对比** — 不同读写比例下的 benchmark
- [ ] **4.7 Join 算法性能对比** — 不同数据量下 SNLJ/BNLJ/Sort Merge/Grace Hash
- [ ] **4.8 Buffer Pool 分析** — hit rate 随数据量变化曲线
- [ ] **4.9 结果输出** — CSV 输出 + Python 画图脚本

## 改动范围

- 新增 `stats/` 模块（QueryStats、SlowQueryLog）
- 修改 `QueryOperator`（注入统计收集点）
- 修改 `BufferManager`（暴露 hit rate 指标）
- 新增 `benchmark/` 目录（测试脚本 + 数据生成器 + 画图脚本）

## 面试话题

- 性能分析方法论
- 如何定位数据库瓶颈
- benchmark 设计（如何避免偏差、如何保证可复现）
