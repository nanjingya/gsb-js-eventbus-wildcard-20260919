export const OVERFLOW_POLICIES = Object.freeze({
  DROP_OLDEST: "drop-oldest",
  DROP_NEWEST: "drop-newest",
  THROW: "throw",
});

export class EventBusOverflowError extends Error {
  constructor(event, listener) {
    super(`listener queue overflow for event "${event}"`);
    this.name = "EventBusOverflowError";
    this.event = event;
    this.listener = listener;
  }
}

export class EventBusDropError extends Error {
  constructor(event, listener, reason) {
    super(`queued invocation dropped for event "${event}" (${reason})`);
    this.name = "EventBusDropError";
    this.event = event;
    this.listener = listener;
    this.reason = reason;
  }
}

function assertEventName(event) {
  if (typeof event !== "string" || event.length === 0) {
    throw new TypeError("event name must be a non-empty string");
  }
}

function assertListener(fn) {
  if (typeof fn !== "function") {
    throw new TypeError("listener must be a function");
  }
}

function assertQueueOptions(maxQueue, overflow) {
  if (!Number.isInteger(maxQueue) || maxQueue < 0) {
    throw new TypeError("maxQueue must be a non-negative integer");
  }
  if (!Object.values(OVERFLOW_POLICIES).includes(overflow)) {
    throw new TypeError(
      `overflow must be one of: ${Object.values(OVERFLOW_POLICIES).join(", ")}`,
    );
  }
}

// Wildcard matching on dot-separated segments:
//   "*"  matches exactly one segment
//   "**" matches zero or more segments (only as a whole segment)
export function matchesPattern(pattern, event) {
  const p = pattern.split(".");
  const e = event.split(".");
  const walk = (pi, ei) => {
    if (pi === p.length) return ei === e.length;
    if (p[pi] === "**") {
      for (let k = ei; k <= e.length; k += 1) {
        if (walk(pi + 1, k)) return true;
      }
      return false;
    }
    if (ei >= e.length) return false;
    if (p[pi] === "*" || p[pi] === e[ei]) return walk(pi + 1, ei + 1);
    return false;
  };
  return walk(0, 0);
}

export class EventBus {
  #listeners = new Map(); // registered pattern string -> Set<entry>
  #middlewares = [];
  #maxQueue;
  #overflow;

  constructor(options = {}) {
    const { maxQueue = 1024, overflow = OVERFLOW_POLICIES.DROP_OLDEST } = options;
    assertQueueOptions(maxQueue, overflow);
    this.#maxQueue = maxQueue;
    this.#overflow = overflow;
  }

  on(event, fn, options = {}) {
    return this.#add(event, fn, false, options);
  }

  once(event, fn, options = {}) {
    return this.#add(event, fn, true, options);
  }

  #add(event, fn, once, options) {
    assertEventName(event);
    assertListener(fn);
    const maxQueue = options.maxQueue ?? this.#maxQueue;
    const overflow = options.overflow ?? this.#overflow;
    assertQueueOptions(maxQueue, overflow);
    const entry = {
      pattern: event,
      fn,
      once,
      consumed: false,
      removed: false,
      inflight: 0,
      queue: [],
      maxQueue,
      overflow,
    };
    let set = this.#listeners.get(event);
    if (!set) {
      set = new Set();
      this.#listeners.set(event, set);
    }
    set.add(entry);
    return () => this.#removeEntry(event, entry);
  }

  // Removes only registrations that exactly match (event, fn); other
  // listeners on the same event are untouched.
  off(event, fn) {
    assertEventName(event);
    assertListener(fn);
    const set = this.#listeners.get(event);
    if (!set) return;
    for (const entry of [...set]) {
      if (entry.fn === fn) this.#removeEntry(event, entry);
    }
  }

  #removeEntry(pattern, entry) {
    const set = this.#listeners.get(pattern);
    if (set) {
      set.delete(entry);
      if (set.size === 0) this.#listeners.delete(pattern);
    }
    entry.removed = true;
    if (entry.queue.length > 0) {
      const pending = entry.queue.splice(0);
      for (const item of pending) {
        item.job?.reject(
          new EventBusDropError(item.event, entry.fn, "listener removed"),
        );
      }
    }
  }

  // Registers a global middleware. Middleware run in registration order (FIFO)
  // for every emit/emitAsync. Contract: fn({ event, args }) may
  //   - return undefined         -> pass the context through unchanged
  //   - return { event?, args? } -> rewrite the event name and/or payload
  //   - return false             -> swallow the event (no listener runs)
  use(fn) {
    assertListener(fn);
    this.#middlewares.push(fn);
    return () => {
      const index = this.#middlewares.indexOf(fn);
      if (index >= 0) this.#middlewares.splice(index, 1);
    };
  }

  #applyMiddleware(event, args) {
    let ctx = { event, args };
    for (const mw of this.#middlewares) {
      const out = mw(ctx);
      if (out === false) return null;
      if (out != null && typeof out === "object") {
        ctx = {
          event: out.event ?? ctx.event,
          args: out.args ?? ctx.args,
        };
      }
    }
    return ctx;
  }

  // Snapshot of entries whose registered pattern matches the emitted event.
  // The snapshot isolates a single emit from concurrent on/off mutations:
  // listeners registered during this emit fire on the next emit, and entries
  // removed during this emit still fire from the snapshot.
  #match(event) {
    const matched = [];
    for (const [pattern, set] of this.#listeners) {
      if (!matchesPattern(pattern, event)) continue;
      for (const entry of set) matched.push(entry);
    }
    return matched;
  }

  emit(event, ...args) {
    assertEventName(event);
    const ctx = this.#applyMiddleware(event, args);
    if (!ctx) return false;
    const entries = this.#match(ctx.event);
    for (const entry of entries) {
      this.#dispatch(entry, ctx.event, ctx.args, null);
    }
    return entries.length > 0;
  }

  // Runs listeners concurrently in registration order and resolves with an
  // array of their return values (in registration order). A failing listener
  // never blocks the others; all failures are rethrown together as an
  // AggregateError whose `errors` hold the original exceptions (cause chains
  // preserved) and whose `failures` locate each failing listener.
  async emitAsync(event, ...args) {
    assertEventName(event);
    const ctx = this.#applyMiddleware(event, args);
    if (!ctx) return [];
    const entries = this.#match(ctx.event);
    const settled = await Promise.allSettled(
      entries.map(
        (entry) =>
          new Promise((resolve, reject) => {
            this.#dispatch(entry, ctx.event, ctx.args, { resolve, reject });
          }),
      ),
    );
    const failures = [];
    const results = new Array(settled.length);
    settled.forEach((outcome, index) => {
      if (outcome.status === "fulfilled") {
        results[index] = outcome.value;
      } else {
        failures.push({
          index,
          event: ctx.event,
          listener: entries[index].fn,
          error: outcome.reason,
        });
      }
    });
    if (failures.length > 0) {
      const aggregate = new AggregateError(
        failures.map((f) => f.error),
        `${failures.length} listener(s) failed for event "${ctx.event}"`,
      );
      aggregate.event = ctx.event;
      aggregate.failures = failures;
      throw aggregate;
    }
    return results;
  }

  #dispatch(entry, event, args, job) {
    if (entry.once && entry.consumed) {
      job?.reject(new EventBusDropError(event, entry.fn, "once listener already fired"));
      return;
    }
    if (entry.inflight > 0) {
      this.#enqueue(entry, event, args, job);
      return;
    }
    if (entry.once) {
      entry.consumed = true;
      this.#removeEntry(entry.pattern, entry);
    }
    this.#execute(entry, event, args, job);
  }

  // A listener is "busy" while a previously returned promise is unsettled;
  // further invocations go to its bounded per-listener queue. This applies
  // identically to sync emit and emitAsync.
  #enqueue(entry, event, args, job) {
    if (entry.queue.length < entry.maxQueue) {
      entry.queue.push({ event, args, job });
      return;
    }
    switch (entry.overflow) {
      case OVERFLOW_POLICIES.DROP_OLDEST: {
        const dropped = entry.queue.shift();
        dropped.job?.reject(new EventBusDropError(event, entry.fn, "drop-oldest"));
        entry.queue.push({ event, args, job });
        return;
      }
      case OVERFLOW_POLICIES.DROP_NEWEST: {
        job?.reject(new EventBusDropError(event, entry.fn, "drop-newest"));
        return; // sync emit: newest invocation is silently dropped
      }
      case OVERFLOW_POLICIES.THROW: {
        const error = new EventBusOverflowError(event, entry.fn);
        if (job) job.reject(error);
        else throw error;
        return;
      }
      default:
        throw new TypeError(`unknown overflow policy: ${entry.overflow}`);
    }
  }

  #execute(entry, event, args, job) {
    entry.inflight += 1;
    let result;
    try {
      result = entry.fn(...args);
    } catch (err) {
      entry.inflight -= 1;
      this.#drain(entry);
      if (job) job.reject(err);
      else throw err;
      return;
    }
    if (result != null && typeof result.then === "function") {
      const tracked = Promise.resolve(result).then(
        (value) => {
          entry.inflight -= 1;
          this.#drain(entry);
          return value;
        },
        (err) => {
          entry.inflight -= 1;
          this.#drain(entry);
          throw err;
        },
      );
      if (job) job.resolve(tracked);
      else tracked.catch(() => {}); // sync emit: async errors are not observable
    } else {
      entry.inflight -= 1;
      this.#drain(entry);
      job?.resolve(result);
    }
  }

  #drain(entry) {
    while (entry.inflight === 0 && entry.queue.length > 0) {
      const item = entry.queue.shift();
      if (entry.removed || (entry.once && entry.consumed)) {
        item.job?.reject(
          new EventBusDropError(item.event, entry.fn, "listener removed"),
        );
        continue;
      }
      this.#dispatch(entry, item.event, item.args, item.job);
    }
  }

  // Number of listeners that would run for a given concrete event name,
  // including wildcard registrations that match it.
  listenerCount(event) {
    assertEventName(event);
    let count = 0;
    for (const [pattern, set] of this.#listeners) {
      if (matchesPattern(pattern, event)) count += set.size;
    }
    return count;
  }

  // Registered event patterns (exact names and wildcard patterns), unique,
  // in registration order.
  eventNames() {
    return [...this.#listeners.keys()];
  }

  // Without arguments removes every listener. With a pattern:
  //   - contains "*" -> removes registrations whose name matches the wildcard
  //   - otherwise    -> prefix match: "user" removes "user" and "user.*"
  removeAllListeners(pattern) {
    if (pattern === undefined) {
      for (const [key, set] of [...this.#listeners]) {
        for (const entry of [...set]) this.#removeEntry(key, entry);
      }
      return;
    }
    assertEventName(pattern);
    const wildcard = pattern.includes("*");
    for (const [key, set] of [...this.#listeners]) {
      const hit = wildcard
        ? matchesPattern(pattern, key)
        : key === pattern || key.startsWith(`${pattern}.`);
      if (hit) {
        for (const entry of [...set]) this.#removeEntry(key, entry);
      }
    }
  }
}
