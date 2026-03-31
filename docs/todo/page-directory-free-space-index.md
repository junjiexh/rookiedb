# PageDirectory 空闲空间内存索引优化

## 问题

当前 `PageDirectory` 查找空闲页采用链表线性扫描，复杂度 O(K)（K 为 header page 数量），存在三个生产级问题：

1. **查找效率**：线性扫描随数据量增长不可接受
2. **并发瓶颈**：`loadPageWithSpace` pin 住 header page 期间阻塞其他事务
3. **精确计数维护成本**：每次记录增删都要更新 header page，产生写放大

## 方案：启动时构建内存索引（方案 B）

在 `PageDirectory` 中维护一个按 freeSpace 排序的内存索引：

```java
class PageDirectory {
    // 按空闲空间排序，快速定位有足够空间的 data page
    private TreeMap<Short, Queue<Long>> freeSpaceIndex;
}
```

### 实现要点

- **启动零额外成本**：当前 `HeaderPage` 构造函数已经遍历所有 entry 统计 `numDataPages`，只需在同一循环中将 `(freeSpace, pageNum)` 插入 TreeMap
- **查找 O(log N)**：使用 `TreeMap.ceilingKey(requiredSpace)` 定位满足空间要求的最小桶
- **更新同步**：`updateFreeSpace` 时同步维护内存索引 + 写 header page
- **不改磁盘格式**：完全兼容现有 header page / data page 布局

### 优点

- 改动小，风险低
- 查找从 O(K) 降到 O(log N) 内存操作，无额外磁盘 I/O
- 兼容现有磁盘格式，无需数据迁移

### 缺点

- 内存开销：需要为每个 data page 维护一个索引条目
- 崩溃恢复：内存索引丢失后需要重建（但启动时已经在做全量扫描）
- 并发问题仍需额外处理（TreeMap 本身非线程安全，需要加锁或用 ConcurrentSkipListMap）

## 备选方案

- **方案 A（树形 FSM）**：借鉴 PostgreSQL，改动大但长期最优
- **方案 C（分级空闲链表）**：按空闲程度分桶，O(1) 查找，适合写入模式均匀的场景

## 相关文件

- `src/main/java/edu/berkeley/cs186/database/table/PageDirectory.java`
- `src/main/java/edu/berkeley/cs186/database/memory/BufferManager.java`
