# serverless-concurrency-governor

JavaScript Serverless 并发租约核心（不接真实云平台），处理预留并发、共享突发容量、
排队等待、取消与公平调度。所有容量决策都收敛在单个 `ConcurrencyGovernor` 实例的
原子“检查 + 授予”里，从 API 层面消除了“先读活跃数、再创建 invocation”的两步竞态。

## 运行测试

```sh
node --test
```

## 为什么不会超卖

问题背景：runtime 在创建 invocation 前只读一次活跃数，多个调度节点同时读到同一份
空闲容量时会同时放行，最终由平台限流兜底。本核心的约定是：

- **不存在“读容量”的公开 API 用于决策**。`acquire` 是唯一入口，检查空闲容量与
  写入租约账本在同一个同步调用内完成，中间不可插入第二个 acquire。
- 多调度节点部署时，必须把 acquire 路由到同一个治理器实例（或等价的单写者
  序列化层，如 leader / 日志复制）。治理器本身是纯内存、纯同步的，序列化点之外
  不持有任何时钟或外部状态。
- 时间完全由调用方以 `now` 注入，便于测试与确定性重放；调用方应单调推进。

## 租约状态机

```
                 acquire(有空闲容量)
        ┌──────┐ ──────────────────▶ ┌──────┐  beginExecution  ┌─────────┐
        │ none │                     │ HELD │ ───────────────▶ │ RUNNING │
        └──────┘                     └──┬───┘                  └────┬────┘
           │                            │                           │
           │ acquire(无容量,有deadline)  │ release / 到期            │ release / 到期
           ▼                            ▼                           ▼
        ┌────────┐  promote      ┌───────────┐               ┌───────────┐
        │ QUEUED │ ─────────────▶│ (新租约)   │               │ RELEASED  │
        └───┬────┘               │ HELD      │               │  /EXPIRED │
            │ cancel / 超时       └───────────┘               └───────────┘
            ▼
        CANCELLED / TIMED_OUT
```

- 租约携带唯一 ID（`crypto.randomUUID`）与 `expiresAt = acquiredAt + leaseTtlMs`。
- 只有 `HELD` 且未过期的租约能 `beginExecution`；`RUNNING` 是执行中的状态，
  重复 begin、过期后 begin、伪造 ID 都会被拒绝。
- `renew` 可在过期前延长租约；长任务必须续租，否则到期按崩溃处理。
- 到期即过期（`expiresAt <= now`），过期是惰性结算 + `sweep` 主动扫描双通道：
  任何携带 `now` 的操作都会先结算已到期租约，再处理自身逻辑。
- 崩溃恢复：持有者宕机后无人 release，租约到期被回收，槽位发给新请求时
  **生成全新租约 ID**，旧租约永久留在终态，不会被复活或重复分配。

## 容量模型

每个 function 的硬上限 = `reservedConcurrency` + 从共享突发池拿到的槽位；
共享池全局总量 = `sharedBurstCapacity`。授予顺序：先占预留槽，预留满了再占突发槽。

- `acquire(fnId, { now })`：无容量直接 `reject(at-capacity)`。
- `acquire(fnId, { now, waitUntilMs })`：无容量则入队等待；`waitUntilMs <= now`
  直接 `reject(deadline-passed)`；队列满 `reject(queue-full)`。
- 队列是**全局 FIFO（按到达时间）**：槽位释放时从队首扫描，第一个其 function
  当前有容量的票据被晋升。某个 function 队满不会阻塞其他 function，但同一个
  function 不会长期独占释放出的槽位——顺序只由到达时间决定。

## 扩缩容期间的边界

- **突发扩容**（`setSharedBurstCapacity` 调大）：立即生效，同一调用内尝试晋升
  等待队列。
- **突发缩容**（调小）：**不撤销**活跃租约。`burstInUse` 可能暂时高于新上限，
  此时暂停一切新突发授予，直到释放/过期把用量降到上限以下。已持有的突发租约
  仍可 `beginExecution`。
- **预留缩容**（`configureFunction` 调小 `reservedConcurrency`）：同样不撤销
  活跃租约，新授予按新上限判定，用量自然回落后恢复。
- **释放与取消竞争**：所有终态转换先到先得。`release` 一个已过期/已释放的租约、
  `cancel` 一个已晋升/已取消的票据，都返回 `ignored` 且无副作用——槽位永远只
  回收一次，票据已晋升后只能走 `release` 租约的路径退出。

## API 一览

| 方法 | 说明 |
| --- | --- |
| `configureFunction(fnId, { reservedConcurrency })` | 注册或调整 function 预留并发 |
| `acquire(fnId, { now, waitUntilMs?, leaseTtlMs? })` | 原子获取租约 / 入队 / 拒绝 |
| `beginExecution(leaseId, now)` | 持有有效租约才能开始执行 |
| `renew(leaseId, now, leaseTtlMs?)` | 续租延长过期时间 |
| `release(leaseId, now)` | 释放租约并晋升等待者 |
| `cancel(ticketId, now)` | 取消排队中的票据 |
| `setSharedBurstCapacity(cap, now)` | 调整共享突发池总量 |
| `sweep(now)` | 主动扫描过期租约与超时票据 |
| `stats(fnId?)` / `inspectLease` / `inspectTicket` | 观测 |
| `validate()` | 结构不变量自检（测试用） |
