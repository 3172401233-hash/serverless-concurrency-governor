import test from 'node:test';
import assert from 'node:assert/strict';
import { ConcurrencyGovernor, LeaseState, TicketState } from '../src/governor.js';

function makeGovernor(overrides = {}) {
  return new ConcurrencyGovernor({ sharedBurstCapacity: 0, leaseTtlMs: 1000, ...overrides });
}

function mulberry32(seed) {
  let state = seed;
  return () => {
    state |= 0;
    state = (state + 0x6d2b79f5) | 0;
    let t = Math.imul(state ^ (state >>> 15), 1 | state);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

test('并发抢槽：原子授予，不超过预留+突发硬上限，租约 ID 唯一', () => {
  const gov = makeGovernor({ sharedBurstCapacity: 2 });
  gov.configureFunction('fn-a', { reservedConcurrency: 2 });

  const results = Array.from({ length: 10 }, () => gov.acquire('fn-a', { now: 0 }));
  const acquired = results.filter((r) => r.status === 'acquired');
  const rejected = results.filter((r) => r.status === 'rejected');

  assert.equal(acquired.length, 4);
  assert.equal(rejected.length, 6);
  assert.ok(rejected.every((r) => r.reason === 'at-capacity'));

  const ids = new Set(acquired.map((r) => r.lease.id));
  assert.equal(ids.size, 4, '租约 ID 必须唯一');
  assert.ok(acquired.every((r) => r.lease.expiresAt === 1000), '租约必须带过期时间');

  const stats = gov.stats('fn-a');
  assert.equal(stats.activeReserved, 2);
  assert.equal(stats.activeBurst, 2);
  assert.ok(stats.activeTotal <= 2 + 2, '同一时刻不超过 function 硬上限');
  assert.ok(gov.stats().burstInUse <= 2, '共享突发不超全局上限');
  gov.validate();
});

test('执行门禁：只有持有有效租约的请求能开始执行', () => {
  const gov = makeGovernor();
  gov.configureFunction('fn-a', { reservedConcurrency: 1 });

  const { lease } = gov.acquire('fn-a', { now: 0 });
  assert.equal(gov.beginExecution(lease.id, 10).status, 'started');
  assert.equal(gov.beginExecution(lease.id, 20).reason, 'already-started');
  assert.equal(gov.beginExecution('no-such-lease', 20).reason, 'unknown-lease');

  const rejected = gov.acquire('fn-a', { now: 30 });
  assert.equal(rejected.status, 'rejected');
  gov.validate();
});

test('释放与过期竞争：先到者生效，槽位只回收一次', () => {
  const gov = makeGovernor({ sharedBurstCapacity: 1 });
  gov.configureFunction('fn-a', { reservedConcurrency: 0 });

  const { lease } = gov.acquire('fn-a', { now: 0 });
  assert.equal(gov.stats().burstInUse, 1);

  gov.sweep(1000);
  assert.equal(gov.inspectLease(lease.id).state, LeaseState.EXPIRED);
  assert.equal(gov.stats().burstInUse, 0, '过期只回收一次');

  const late = gov.release(lease.id, 1001);
  assert.equal(late.status, 'ignored');
  assert.equal(late.reason, 'already-expired');
  assert.equal(gov.stats().burstInUse, 0, '迟到的 release 不得重复回收');

  const again = gov.release(lease.id, 1002);
  assert.equal(again.reason, 'already-expired');
  gov.validate();
});

test('重复释放与取消已晋升票据都是安全的空操作', () => {
  const gov = makeGovernor();
  gov.configureFunction('fn-a', { reservedConcurrency: 1 });

  const first = gov.acquire('fn-a', { now: 0 });
  const queued = gov.acquire('fn-a', { now: 0, waitUntilMs: 500 });
  assert.equal(queued.status, 'queued');

  const released = gov.release(first.lease.id, 10);
  assert.equal(released.status, 'released');
  assert.equal(released.promoted.length, 1);
  assert.equal(released.promoted[0].id, queued.ticketId);

  assert.equal(gov.release(first.lease.id, 20).reason, 'already-released');

  const cancelAfterPromote = gov.cancel(queued.ticketId, 30);
  assert.equal(cancelAfterPromote.status, 'ignored');
  assert.equal(cancelAfterPromote.reason, 'already-promoted');

  const promotedLeaseId = gov.inspectTicket(queued.ticketId).leaseId;
  assert.equal(gov.beginExecution(promotedLeaseId, 40).status, 'started');
  gov.validate();
});

test('取消与释放竞争：已取消的票据不占槽，槽位给下一个等待者', () => {
  const gov = makeGovernor();
  gov.configureFunction('fn-a', { reservedConcurrency: 1 });

  const active = gov.acquire('fn-a', { now: 0 });
  const t1 = gov.acquire('fn-a', { now: 1, waitUntilMs: 900 });
  const t2 = gov.acquire('fn-a', { now: 2, waitUntilMs: 900 });

  assert.equal(gov.cancel(t1.ticketId, 5).status, 'cancelled');
  assert.equal(gov.cancel(t1.ticketId, 6).reason, 'already-cancelled');

  const released = gov.release(active.lease.id, 10);
  assert.equal(released.promoted.length, 1);
  assert.equal(released.promoted[0].id, t2.ticketId, '取消的票据不得占用释放出的槽位');
  assert.equal(gov.stats('fn-a').activeTotal, 1);
  gov.validate();
});

test('租约过期：崩溃后安全回收，旧租约不能执行也不重复分配', () => {
  const gov = makeGovernor({ leaseTtlMs: 100 });
  gov.configureFunction('fn-a', { reservedConcurrency: 1 });

  const crashed = gov.acquire('fn-a', { now: 0 });
  gov.beginExecution(crashed.lease.id, 1);

  const blocked = gov.acquire('fn-a', { now: 50 });
  assert.equal(blocked.status, 'rejected', '崩溃未释放时槽位仍被占用');

  const sweepResult = gov.sweep(100);
  assert.deepEqual(sweepResult.expiredLeaseIds, [crashed.lease.id]);

  const denied = gov.beginExecution(crashed.lease.id, 101);
  assert.equal(denied.status, 'denied');
  assert.equal(denied.reason, 'lease-expired');

  const fresh = gov.acquire('fn-a', { now: 101 });
  assert.equal(fresh.status, 'acquired');
  assert.notEqual(fresh.lease.id, crashed.lease.id, '回收后必须发新租约而非复活旧租约');
  assert.equal(gov.stats('fn-a').activeTotal, 1, '同一槽位不得重复分配');
  gov.validate();
});

test('续租可延长过期时间，过期后续租无效', () => {
  const gov = makeGovernor({ leaseTtlMs: 100 });
  gov.configureFunction('fn-a', { reservedConcurrency: 1 });

  const { lease } = gov.acquire('fn-a', { now: 0 });
  assert.equal(gov.renew(lease.id, 90, 100).status, 'renewed');
  assert.equal(gov.sweep(150).expiredLeaseIds.length, 0, '续租后未到新过期时间');
  assert.deepEqual(gov.sweep(190).expiredLeaseIds, [lease.id]);
  assert.equal(gov.renew(lease.id, 191).reason, 'already-expired');
  gov.validate();
});

test('等待队列：截止前晋升，超时拒绝与出队', () => {
  const gov = makeGovernor();
  gov.configureFunction('fn-a', { reservedConcurrency: 1 });

  const active = gov.acquire('fn-a', { now: 0 });
  const waiting = gov.acquire('fn-a', { now: 0, waitUntilMs: 500 });
  assert.equal(waiting.status, 'queued');

  // 任意操作推进时间时都会惰性结算：这次 acquire 会把超时票据清出队列
  const expiredDeadline = gov.acquire('fn-a', { now: 600, waitUntilMs: 500 });
  assert.equal(expiredDeadline.reason, 'deadline-passed');
  assert.equal(gov.inspectTicket(waiting.ticketId).state, TicketState.TIMED_OUT);

  const sweepResult = gov.sweep(600);
  assert.deepEqual(sweepResult.timedOutTicketIds, [], '超时票据已被惰性结算，sweep 不重复处理');

  gov.release(active.lease.id, 700);
  assert.equal(gov.stats('fn-a').activeTotal, 0, '超时票据不得被晋升');
  gov.validate();
});

test('共享突发缩容：不回收活跃租约，但暂停新突发授予直到用量回落', () => {
  const gov = makeGovernor({ sharedBurstCapacity: 3 });
  gov.configureFunction('fn-a', { reservedConcurrency: 1 });

  const leases = Array.from({ length: 4 }, () => gov.acquire('fn-a', { now: 0 }).lease);
  assert.equal(gov.stats().burstInUse, 3);

  gov.setSharedBurstCapacity(1, 10);
  assert.equal(gov.stats().burstInUse, 3, '缩容不撤销活跃租约');
  assert.equal(gov.beginExecution(leases[3].id, 20).status, 'started', '已持有的突发租约仍可执行');

  const overCap = gov.acquire('fn-a', { now: 30 });
  assert.equal(overCap.status, 'rejected', '用量高于新上限时暂停新突发授予');

  gov.release(leases[1].id, 40);
  gov.release(leases[2].id, 40);
  assert.equal(gov.stats().burstInUse, 1);
  assert.equal(gov.acquire('fn-a', { now: 50 }).status, 'rejected', '用量等于上限仍不授予');

  gov.release(leases[3].id, 60);
  const afterDrain = gov.acquire('fn-a', { now: 70 });
  assert.equal(afterDrain.status, 'acquired', '用量回落到上限以下后恢复授予');
  gov.validate();
});

test('共享突发扩容：立即晋升等待队列中的请求', () => {
  const gov = makeGovernor({ sharedBurstCapacity: 0 });
  gov.configureFunction('fn-a', { reservedConcurrency: 1 });

  gov.acquire('fn-a', { now: 0 });
  const queued = gov.acquire('fn-a', { now: 0, waitUntilMs: 1000 });
  assert.equal(queued.status, 'queued');

  const grown = gov.setSharedBurstCapacity(1, 10);
  assert.equal(grown.promoted.length, 1);
  assert.equal(grown.promoted[0].id, queued.ticketId);
  assert.equal(gov.stats().burstInUse, 1);
  gov.validate();
});

test('预留并发缩容：活跃租约超额保留，新授予按新上限执行', () => {
  const gov = makeGovernor();
  gov.configureFunction('fn-a', { reservedConcurrency: 3 });
  const leases = Array.from({ length: 3 }, () => gov.acquire('fn-a', { now: 0 }).lease);

  gov.configureFunction('fn-a', { reservedConcurrency: 1 });
  assert.equal(gov.acquire('fn-a', { now: 10 }).status, 'rejected');

  gov.release(leases[0].id, 20);
  gov.release(leases[1].id, 20);
  assert.equal(gov.acquire('fn-a', { now: 30 }).status, 'rejected', '仍高于新预留上限');

  gov.release(leases[2].id, 40);
  assert.equal(gov.acquire('fn-a', { now: 50 }).status, 'acquired');
  gov.validate();
});

test('公平性：释放的槽位按全局到达顺序分配，不被同一 function 长期独占', () => {
  const gov = makeGovernor({ sharedBurstCapacity: 1 });
  gov.configureFunction('fn-a', { reservedConcurrency: 0 });
  gov.configureFunction('fn-b', { reservedConcurrency: 0 });

  const holder = gov.acquire('fn-a', { now: 0 });
  const bFirst = gov.acquire('fn-b', { now: 1, waitUntilMs: 1000 });
  const aSecond = gov.acquire('fn-a', { now: 2, waitUntilMs: 1000 });
  assert.equal(bFirst.status, 'queued');
  assert.equal(aSecond.status, 'queued');

  const released = gov.release(holder.lease.id, 10);
  assert.equal(released.promoted[0].id, bFirst.ticketId, '队首等待者优先，即使槽位来自另一个 function');

  const bLeaseId = gov.inspectTicket(bFirst.ticketId).leaseId;
  const releasedAgain = gov.release(bLeaseId, 20);
  assert.equal(releasedAgain.promoted[0].id, aSecond.ticketId, '严格按到达顺序轮转');
  gov.validate();
});

test('随机操作序列：任意交错下不越上限、不重复分配', () => {
  const rand = mulberry32(42);
  const gov = makeGovernor({ sharedBurstCapacity: 3, leaseTtlMs: 200 });
  gov.configureFunction('fn-a', { reservedConcurrency: 2 });
  gov.configureFunction('fn-b', { reservedConcurrency: 1 });

  const liveLeases = [];
  const liveTickets = [];
  const seenLeaseIds = new Set();
  let now = 0;

  for (let step = 0; step < 2000; step += 1) {
    now += Math.floor(rand() * 50);
    const fnId = rand() < 0.5 ? 'fn-a' : 'fn-b';
    const op = rand();

    if (op < 0.45) {
      const withWait = rand() < 0.5;
      const result = gov.acquire(fnId, {
        now,
        waitUntilMs: withWait ? now + 100 + Math.floor(rand() * 300) : null,
      });
      if (result.status === 'acquired') {
        assert.ok(!seenLeaseIds.has(result.lease.id), '租约 ID 不得重复');
        seenLeaseIds.add(result.lease.id);
        liveLeases.push(result.lease.id);
      } else if (result.status === 'queued') {
        liveTickets.push(result.ticketId);
      }
    } else if (op < 0.7 && liveLeases.length > 0) {
      const leaseId = liveLeases.splice(Math.floor(rand() * liveLeases.length), 1)[0];
      gov.release(leaseId, now);
    } else if (op < 0.8 && liveLeases.length > 0) {
      const leaseId = liveLeases[Math.floor(rand() * liveLeases.length)];
      gov.beginExecution(leaseId, now);
    } else if (op < 0.9 && liveTickets.length > 0) {
      const ticketId = liveTickets.splice(Math.floor(rand() * liveTickets.length), 1)[0];
      gov.cancel(ticketId, now);
    } else {
      gov.sweep(now);
    }

    gov.validate();
    const limits = { 'fn-a': 2, 'fn-b': 1 };
    for (const [id, reserved] of Object.entries(limits)) {
      const stats = gov.stats(id);
      assert.ok(stats.activeReserved <= reserved, `${id} 预留并发越界`);
      assert.ok(stats.activeTotal <= reserved + 3, `${id} 总并发越硬上限`);
    }
    assert.ok(gov.stats().burstInUse <= 3, '全局突发越上限');
  }
});
