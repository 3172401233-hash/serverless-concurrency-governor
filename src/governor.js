import { randomUUID } from 'node:crypto';

export const LeaseState = Object.freeze({
  HELD: 'held',
  RUNNING: 'running',
  RELEASED: 'released',
  EXPIRED: 'expired',
});

export const TicketState = Object.freeze({
  QUEUED: 'queued',
  PROMOTED: 'promoted',
  CANCELLED: 'cancelled',
  TIMED_OUT: 'timed-out',
});

const TERMINAL_LEASE_STATES = new Set([LeaseState.RELEASED, LeaseState.EXPIRED]);

function assertNonNegativeInt(value, name) {
  if (!Number.isInteger(value) || value < 0) {
    throw new TypeError(`${name} must be a non-negative integer, got ${value}`);
  }
}

function assertNow(now) {
  if (typeof now !== 'number' || !Number.isFinite(now) || now < 0) {
    throw new TypeError(`now must be a finite non-negative number, got ${now}`);
  }
}

function snapshotLease(lease) {
  return {
    id: lease.id,
    functionId: lease.functionId,
    kind: lease.kind,
    state: lease.state,
    acquiredAt: lease.acquiredAt,
    expiresAt: lease.expiresAt,
    startedAt: lease.startedAt,
  };
}

function snapshotTicket(ticket) {
  return {
    id: ticket.id,
    functionId: ticket.functionId,
    state: ticket.state,
    enqueuedAt: ticket.enqueuedAt,
    deadlineMs: ticket.deadlineMs,
    leaseId: ticket.leaseId,
  };
}

/**
 * 并发租约治理器。
 *
 * 所有容量决策都收敛在 acquire 的原子“检查+授予”里：库不暴露
 * “先读容量、再决定”的两步 API，因此多个调度节点只要把请求路由到
 * 同一个治理器实例（或等价的单写者序列化层），就不会因为同时读到
 * 同一份空闲容量而超卖。
 *
 * 时间完全由调用方以 `now` 参数注入并推进，库内部不读墙钟。
 */
export class ConcurrencyGovernor {
  constructor({ sharedBurstCapacity = 0, leaseTtlMs = 30_000, maxQueueSize = Number.POSITIVE_INFINITY } = {}) {
    assertNonNegativeInt(sharedBurstCapacity, 'sharedBurstCapacity');
    if (!(leaseTtlMs > 0)) throw new TypeError(`leaseTtlMs must be > 0, got ${leaseTtlMs}`);
    if (!(maxQueueSize >= 0)) throw new TypeError(`maxQueueSize must be >= 0, got ${maxQueueSize}`);
    this.sharedBurstCapacity = sharedBurstCapacity;
    this.leaseTtlMs = leaseTtlMs;
    this.maxQueueSize = maxQueueSize;
    this.functions = new Map();
    this.leases = new Map();
    this.tickets = new Map();
    this.queue = [];
    this.burstInUse = 0;
    this._lastTimedOut = [];
  }

  configureFunction(functionId, { reservedConcurrency }) {
    assertNonNegativeInt(reservedConcurrency, 'reservedConcurrency');
    const existing = this.functions.get(functionId);
    if (existing) {
      existing.reservedConcurrency = reservedConcurrency;
      return { functionId, reservedConcurrency };
    }
    this.functions.set(functionId, {
      id: functionId,
      reservedConcurrency,
      activeLeases: new Map(),
    });
    return { functionId, reservedConcurrency };
  }

  acquire(functionId, { now, waitUntilMs = null, leaseTtlMs = this.leaseTtlMs } = {}) {
    assertNow(now);
    const fn = this._fn(functionId);
    this._advance(now);
    if (waitUntilMs !== null && waitUntilMs <= now) {
      return { status: 'rejected', reason: 'deadline-passed' };
    }
    const lease = this._tryGrant(fn, now, leaseTtlMs);
    if (lease) {
      return { status: 'acquired', lease: snapshotLease(lease) };
    }
    if (waitUntilMs === null) {
      return { status: 'rejected', reason: 'at-capacity' };
    }
    if (this.queue.length >= this.maxQueueSize) {
      return { status: 'rejected', reason: 'queue-full' };
    }
    const ticket = {
      id: randomUUID(),
      functionId,
      state: TicketState.QUEUED,
      enqueuedAt: now,
      deadlineMs: waitUntilMs,
      leaseTtlMs,
      leaseId: null,
    };
    this.tickets.set(ticket.id, ticket);
    this.queue.push(ticket);
    return { status: 'queued', ticketId: ticket.id, deadlineMs: ticket.deadlineMs };
  }

  beginExecution(leaseId, now) {
    assertNow(now);
    this._advance(now);
    const lease = this.leases.get(leaseId);
    if (!lease) {
      return { status: 'denied', reason: 'unknown-lease' };
    }
    if (lease.state === LeaseState.RUNNING) {
      return { status: 'denied', reason: 'already-started' };
    }
    if (lease.state !== LeaseState.HELD) {
      return { status: 'denied', reason: `lease-${lease.state}` };
    }
    lease.state = LeaseState.RUNNING;
    lease.startedAt = now;
    return { status: 'started', lease: snapshotLease(lease) };
  }

  renew(leaseId, now, leaseTtlMs = this.leaseTtlMs) {
    assertNow(now);
    this._advance(now);
    const lease = this.leases.get(leaseId);
    if (!lease || TERMINAL_LEASE_STATES.has(lease.state)) {
      return { status: 'ignored', reason: lease ? `already-${lease.state}` : 'unknown-lease' };
    }
    lease.expiresAt = now + leaseTtlMs;
    return { status: 'renewed', lease: snapshotLease(lease) };
  }

  release(leaseId, now) {
    assertNow(now);
    this._advance(now);
    const lease = this.leases.get(leaseId);
    if (!lease) {
      return { status: 'ignored', reason: 'unknown-lease' };
    }
    if (TERMINAL_LEASE_STATES.has(lease.state)) {
      return { status: 'ignored', reason: `already-${lease.state}` };
    }
    this._terminateLease(lease, LeaseState.RELEASED);
    const promoted = this._promote(now);
    return { status: 'released', promoted: promoted.map(snapshotTicket) };
  }

  cancel(ticketId, now) {
    assertNow(now);
    this._advance(now);
    const ticket = this.tickets.get(ticketId);
    if (!ticket) {
      return { status: 'ignored', reason: 'unknown-ticket' };
    }
    if (ticket.state !== TicketState.QUEUED) {
      return { status: 'ignored', reason: `already-${ticket.state}` };
    }
    ticket.state = TicketState.CANCELLED;
    this.queue.splice(this.queue.indexOf(ticket), 1);
    return { status: 'cancelled' };
  }

  setSharedBurstCapacity(capacity, now) {
    assertNonNegativeInt(capacity, 'capacity');
    assertNow(now);
    this._advance(now);
    this.sharedBurstCapacity = capacity;
    const promoted = this._promote(now);
    return { status: 'updated', sharedBurstCapacity: capacity, promoted: promoted.map(snapshotTicket) };
  }

  sweep(now) {
    assertNow(now);
    const expired = this._expire(now);
    const promoted = this._promote(now);
    return {
      expiredLeaseIds: expired.map((lease) => lease.id),
      timedOutTicketIds: this._lastTimedOut.map((ticket) => ticket.id),
      promoted: promoted.map(snapshotTicket),
    };
  }

  inspectLease(leaseId) {
    const lease = this.leases.get(leaseId);
    return lease ? snapshotLease(lease) : null;
  }

  inspectTicket(ticketId) {
    const ticket = this.tickets.get(ticketId);
    return ticket ? snapshotTicket(ticket) : null;
  }

  stats(functionId) {
    if (functionId !== undefined) {
      const fn = this._fn(functionId);
      let activeReserved = 0;
      let activeBurst = 0;
      for (const lease of fn.activeLeases.values()) {
        if (lease.kind === 'reserved') activeReserved += 1;
        else activeBurst += 1;
      }
      return {
        functionId,
        reservedConcurrency: fn.reservedConcurrency,
        activeReserved,
        activeBurst,
        activeTotal: activeReserved + activeBurst,
        queued: this.queue.filter((ticket) => ticket.functionId === functionId).length,
      };
    }
    let totalActive = 0;
    for (const fn of this.functions.values()) totalActive += fn.activeLeases.size;
    return {
      sharedBurstCapacity: this.sharedBurstCapacity,
      burstInUse: this.burstInUse,
      totalActive,
      totalQueued: this.queue.length,
    };
  }

  /**
   * 结构不变量自检：租约不重复激活、计数与账本一致、队列状态合法。
   * 测试在每次操作后调用；抛错即表示内部状态被破坏。
   */
  validate() {
    const activeIds = new Set();
    let burstCount = 0;
    for (const fn of this.functions.values()) {
      for (const lease of fn.activeLeases.values()) {
        if (activeIds.has(lease.id)) {
          throw new Error(`lease ${lease.id} active more than once`);
        }
        activeIds.add(lease.id);
        if (TERMINAL_LEASE_STATES.has(lease.state)) {
          throw new Error(`lease ${lease.id} active but in terminal state ${lease.state}`);
        }
        if (this.leases.get(lease.id) !== lease) {
          throw new Error(`lease ${lease.id} missing from ledger`);
        }
        if (lease.kind === 'burst') burstCount += 1;
      }
    }
    if (burstCount !== this.burstInUse) {
      throw new Error(`burst ledger mismatch: counted ${burstCount}, recorded ${this.burstInUse}`);
    }
    for (const ticket of this.queue) {
      if (ticket.state !== TicketState.QUEUED) {
        throw new Error(`ticket ${ticket.id} in queue with state ${ticket.state}`);
      }
      if (this.tickets.get(ticket.id) !== ticket) {
        throw new Error(`ticket ${ticket.id} missing from ledger`);
      }
    }
    return true;
  }

  _fn(functionId) {
    const fn = this.functions.get(functionId);
    if (!fn) throw new Error(`unknown function: ${functionId}`);
    return fn;
  }

  _tryGrant(fn, now, leaseTtlMs) {
    let reservedActive = 0;
    for (const lease of fn.activeLeases.values()) {
      if (lease.kind === 'reserved') reservedActive += 1;
    }
    let kind = null;
    if (reservedActive < fn.reservedConcurrency) {
      kind = 'reserved';
    } else if (this.burstInUse < this.sharedBurstCapacity) {
      kind = 'burst';
    } else {
      return null;
    }
    const lease = {
      id: randomUUID(),
      functionId: fn.id,
      kind,
      state: LeaseState.HELD,
      acquiredAt: now,
      expiresAt: now + leaseTtlMs,
      startedAt: null,
    };
    fn.activeLeases.set(lease.id, lease);
    this.leases.set(lease.id, lease);
    if (kind === 'burst') this.burstInUse += 1;
    return lease;
  }

  _terminateLease(lease, state) {
    lease.state = state;
    const fn = this.functions.get(lease.functionId);
    fn.activeLeases.delete(lease.id);
    if (lease.kind === 'burst') this.burstInUse -= 1;
  }

  _expire(now) {
    const expired = [];
    for (const lease of this.leases.values()) {
      if (!TERMINAL_LEASE_STATES.has(lease.state) && lease.expiresAt <= now) {
        this._terminateLease(lease, LeaseState.EXPIRED);
        expired.push(lease);
      }
    }
    return expired;
  }

  _promote(now) {
    const promoted = [];
    const timedOut = [];
    let index = 0;
    while (index < this.queue.length) {
      const ticket = this.queue[index];
      if (ticket.deadlineMs <= now) {
        ticket.state = TicketState.TIMED_OUT;
        timedOut.push(ticket);
        this.queue.splice(index, 1);
        continue;
      }
      const fn = this.functions.get(ticket.functionId);
      const lease = this._tryGrant(fn, now, ticket.leaseTtlMs);
      if (lease) {
        ticket.state = TicketState.PROMOTED;
        ticket.leaseId = lease.id;
        promoted.push(ticket);
        this.queue.splice(index, 1);
        continue;
      }
      index += 1;
    }
    this._lastTimedOut = timedOut;
    return promoted;
  }

  _advance(now) {
    this._expire(now);
    this._promote(now);
  }
}
