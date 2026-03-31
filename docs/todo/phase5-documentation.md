# Phase 5: README + 架构文档

**状态**: 待开始
**预估**: 约 1 周
**前置**: Phase 1-4 完成后最佳

## 目标

项目门面。招聘者/面试官第一眼看的东西。

## 任务清单

- [ ] **5.1 README 重写**
  - [ ] 一句话定位（"A relational database built from scratch in Java 25"）
  - [ ] 架构图（Mermaid）
  - [ ] 核心特性列表
  - [ ] Quick Start（3 步跑起来）
  - [ ] Benchmark 图表
- [ ] **5.2 架构文档** `docs/architecture.md`
  - [ ] 系统分层图（Storage → Buffer Pool → Table → Query Engine → Parser → Server）
  - [ ] 各模块职责和交互方式
  - [ ] 关键设计决策索引（链接到已有的 design docs）
- [ ] **5.3 MVCC 设计文档** `docs/mvcc-design.md`
  - [ ] 版本链结构图
  - [ ] 可见性规则详解
  - [ ] GC 策略
  - [ ] vs 2PL 对比 + benchmark 数据

## 面试话题

- 文档质量直接反映工程素养
- 好的文档 = 更高效的面试
