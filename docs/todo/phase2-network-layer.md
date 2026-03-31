# Phase 2: 网络层 + Client-Server 架构

**状态**: 待开始
**预估**: 约 3 周
**前置**: Phase 1 (MVCC) ✅ 已完成

## 目标

将 RookieDB 从嵌入式升级为 Client-Server 架构，支持多客户端通过 TCP 连接。

## 任务清单

- [ ] **2.1 TCP Server 骨架** — `RookieServer` 监听端口，线程池接受连接
- [ ] **2.2 Wire Protocol 定义** — 自定义文本协议（请求 `Q<len><sql>`，响应 `R<len><data>` 或 `E<len><error>`）
- [ ] **2.3 ConnectionHandler** — 每个连接一个 handler，绑定事务状态，处理 SQL 请求
- [ ] **2.4 事务生命周期** — 支持跨消息 BEGIN/COMMIT/ROLLBACK，连接断开时自动回滚
- [ ] **2.5 优雅关闭** — SIGTERM 时停止接受新连接，等待活跃事务完成
- [ ] **2.6 Python 客户端** — 适配现有 `client.py`，支持交互式 REPL
- [ ] **2.7 集成测试** — 多客户端并发连接、事务隔离、连接断开处理
- [ ] **2.8 本地/网络双模式** — CommandLineInterface 支持 `--server` 参数启动 Server 模式

## 改动范围

- 新增 `server/` 模块（RookieServer、ConnectionHandler、WireProtocol）
- 修改 `CommandLineInterface`（支持本地和网络两种模式）
- 更新 `client.py`
- 新增集成测试

## 面试话题

- thread-per-connection vs event-driven (NIO/Netty) 的 tradeoff
- 连接管理和线程池调优
- 协议设计（为什么不兼容 PG wire protocol）
- 背压处理
