// Dependency-free event bus (Node standard library only).
//
// Fixes vs. the original implementation:
//  - off() removes only the one exact registration instead of the whole event.
//  - emit() iterates over a stable snapshot, so listeners added/removed during
//    dispatch (including once listeners) cannot be skipped or fired twice.

export const DROPPED = Symbol("eventbus.dropped");

export class BackpressureOverflowError extends Error {
  constructor(message, { event, capacity, strategy } = {}) {
    super(message);
    this.name = "BackpressureOverflowError";
    this.event = event;
    this.capacity = capacity;
    this.strategy = strategy;
  }
}

export class ListenerError extends Error {
  constructor(message, { cause, event, pattern, fn } = {}) {
    super(message, { cause });
    this.name = "ListenerError";
    this.event = event;
    this.pattern = pattern;
    this.fn = fn;
  }
}

const STRATEGIES = new Set(["drop-oldest", "drop-newest", "throw"]);

function isThenable(value) {
  return value != null && typeof value.then === "function";
}

// Validate either a concrete event name or a subscription pattern.
// Segments are dot-separated; "*" and "**" are only legal as whole segments.
function checkEvent(event) {
  if (typeof event !== "string" || event.length === 0) {
    throw new TypeError("event name must be a non-empty string");
  }
  const segments = event.split(".");
  for (const segment of segments) {
    if (segment.length === 0) {
      throw new TypeError(`invalid event name "${event}": empty segment`);
    }
    if (segment.includes("*") && segment !== "*" && segment !== "**") {
      throw new TypeError(
        `invalid event name "${event}": wildcard must be a whole segment`
      );
    }
  }
  return segments;
}

// Glob-style matching on dot-separated segments:
//  - "*"  matches exactly one segment
//  - "**" matches zero or more segments
function segmentsMatch(pattern, name) {
  if (pattern.length === 0) return name.length === 0;
  const [head, ...restPattern] = pattern;
  if (head === "**") {
    for (let skip = 0; skip <= name.length; skip++) {
      if (segmentsMatch(restPattern, name.slice(skip))) return true;
    }
    return false;
  }
  if (name.length === 0) return false;
  if (head === "*" || head === name[0]) {
    return segmentsMatch(restPattern, name.slice(1));
  }
  return false;
}

// Prefix matching used by removeAllListeners(filter):
// every filter segment must match the corresponding pattern segment;
// remaining pattern segments are allowed (filter is a prefix).
function prefixMatches(filterSegments, patternSegments) {
  let pi = 0;
  for (let fi = 0; fi < filterSegments.length; fi++) {
    const expected = filterSegments[fi];
    if (expected === "**") return true; // everything below is covered
    if (pi >= patternSegments.length) return false;
    if (expected === "*") {
      pi += 1;
      continue;
    }
    if (expected !== patternSegments[pi]) return false;
    pi += 1;
  }
  return true;
}

export class EventBus {
  #byPattern = new Map(); // pattern -> Set<registration>
  #order = []; // registrations in global registration order
  #middlewares = [];
  #capacity;
  #strategy;
  #inflight = new Map(); // registration -> number of in-flight + queued jobs
  #queues = new Map(); // registration -> { jobs: [], scheduled: boolean }

  constructor({ capacity = 16, overflow = "throw" } = {}) {
    if (!Number.isInteger(capacity) || capacity < 1) {
      throw new TypeError("capacity must be a positive integer");
    }
    if (!STRATEGIES.has(overflow)) {
      throw new TypeError(
        `overflow must be one of: ${[...STRATEGIES].join(", ")}`
      );
    }
    this.#capacity = capacity;
    this.#strategy = overflow;
  }

  #add(event, fn, once) {
    if (typeof fn !== "function") {
      throw new TypeError("listener must be a function");
    }
    checkEvent(event);
    const registration = { event, fn, once };
    let set = this.#byPattern.get(event);
    if (!set) {
      set = new Set();
      this.#byPattern.set(event, set);
    }
    set.add(registration);
    this.#order.push(registration);
    let removed = false;
    return () => {
      if (removed) return;
      removed = true;
      this.#remove(registration);
    };
  }

  on(event, fn) {
    return this.#add(event, fn, false);
  }

  once(event, fn) {
    return this.#add(event, fn, true);
  }

  // Removes the first registration (in registration order) matching
  // (event, fn). Wildcards are treated literally here: only a registration
  // made with the exact same pattern string is eligible.
  off(event, fn) {
    const set = this.#byPattern.get(event);
    if (!set) return;
    let target;
    for (const registration of this.#order) {
      if (registration.event === event && registration.fn === fn) {
        target = registration;
        break;
      }
    }
    if (target) this.#remove(target);
  }

  #remove(registration) {
    const set = this.#byPattern.get(registration.event);
    if (set) {
      set.delete(registration);
      if (set.size === 0) this.#byPattern.delete(registration.event);
    }
    const index = this.#order.indexOf(registration);
    if (index !== -1) this.#order.splice(index, 1);
  }

  use(fn) {
    if (typeof fn !== "function") {
      throw new TypeError("middleware must be a function");
    }
    this.#middlewares.push(fn);
    return this;
  }

  listenerCount(event) {
    checkEvent(event);
    const segments = event.split(".");
    let count = 0;
    for (const registration of this.#order) {
      if (segmentsMatch(registration.event.split("."), segments)) count += 1;
    }
    return count;
  }

  eventNames() {
    return [...this.#byPattern.keys()];
  }

  // With no argument every registration is removed. With a filter the filter
  // is interpreted as a segment prefix ("user." style); "*" matches one
  // pattern segment and "**" the rest, e.g. removeAllListeners("user.*").
  removeAllListeners(filter) {
    if (filter === undefined) {
      this.#byPattern.clear();
      this.#order.length = 0;
      return;
    }
    checkEvent(filter);
    const filterSegments = filter.split(".");
    const victims = this.#order.filter((registration) =>
      prefixMatches(filterSegments, registration.event.split("."))
    );
    for (const registration of victims) this.#remove(registration);
  }

  #collect(event) {
    const segments = event.split(".");
    return this.#order.filter((registration) =>
      segmentsMatch(registration.event.split("."), segments)
    );
  }

  #depth(registration) {
    return this.#inflight.get(registration) ?? 0;
  }

  #overflowError(event) {
    return new BackpressureOverflowError(
      `eventbus: listener backlog is full (capacity ${this.#capacity}) for "${event}"`,
      { event, capacity: this.#capacity, strategy: this.#strategy }
    );
  }

  // Decide what a synchronous (inline) dispatch should do when the listener
  // is already at capacity.
  #syncOverflow(registration, event) {
    const queue = this.#queues.get(registration);
    if (this.#strategy === "drop-oldest") {
      if (queue && queue.jobs.length > 0) {
        const oldest = queue.jobs.shift();
        oldest.resolve(DROPPED);
        // Release the evicted job's slot; the inline call claims it again
        // via the caller's normal depth increment.
        this.#releaseSlot(registration);
        return "run";
      }
      // Capacity is fully occupied by already-running jobs that cannot be
      // cancelled; the incoming invocation becomes the dropped one.
      return "skip";
    }
    if (this.#strategy === "drop-newest") return "skip";
    throw this.#overflowError(event);
  }

  #releaseSlot(registration) {
    const remaining = this.#depth(registration) - 1;
    if (remaining <= 0) {
      this.#inflight.delete(registration);
      this.#queues.delete(registration);
    } else {
      this.#inflight.set(registration, remaining);
    }
  }

  #runMiddlewaresSync(event, args) {
    let lastIndex = -1;
    const dispatch = (index, currentEvent, currentArgs) => {
      if (index <= lastIndex) {
        throw new Error("eventbus: next() must not be called multiple times");
      }
      lastIndex = index;
      const middleware = this.#middlewares[index];
      if (!middleware) return { event: currentEvent, args: currentArgs };
      let called = false;
      let pending;
      const next = (newEvent, ...newArgs) => {
        called = true;
        pending = dispatch(
          index + 1,
          newEvent === undefined ? currentEvent : newEvent,
          newArgs.length > 0 ? newArgs : currentArgs
        );
        return pending;
      };
      const result = middleware(currentEvent, currentArgs, next);
      if (isThenable(result)) {
        throw new Error(
          "eventbus: async middleware detected; use emitAsync() instead of emit()"
        );
      }
      if (!called) return null; // middleware short-circuited the dispatch
      return pending;
    };
    return dispatch(0, event, args);
  }

  async #runMiddlewaresAsync(event, args) {
    let lastIndex = -1;
    const dispatch = async (index, currentEvent, currentArgs) => {
      if (index <= lastIndex) {
        throw new Error("eventbus: next() must not be called multiple times");
      }
      lastIndex = index;
      const middleware = this.#middlewares[index];
      if (!middleware) return { event: currentEvent, args: currentArgs };
      let called = false;
      let pending;
      const next = (newEvent, ...newArgs) => {
        called = true;
        pending = dispatch(
          index + 1,
          newEvent === undefined ? currentEvent : newEvent,
          newArgs.length > 0 ? newArgs : currentArgs
        );
        return pending;
      };
      const result = await middleware(currentEvent, currentArgs, next);
      if (isThenable(result) && !called) return null;
      if (!called) return null; // middleware short-circuited the dispatch
      return pending;
    };
    return dispatch(0, event, args);
  }

  emit(event, ...args) {
    checkEvent(event);
    const target = this.#runMiddlewaresSync(event, args);
    if (target === null) return; // short-circuited by middleware
    const registrations = this.#collect(target.event);
    if (registrations.length === 0) return;

    for (const registration of registrations) {
      if (this.#depth(registration) >= this.#capacity) {
        const decision = this.#syncOverflow(registration, target.event);
        if (decision === "skip") continue;
      }
      this.#inflight.set(registration, this.#depth(registration) + 1);
      if (registration.once) this.#remove(registration);
      try {
        registration.fn(...target.args);
      } finally {
        this.#releaseSlot(registration);
      }
    }
  }

  async #runListener(registration, event, args) {
    try {
      const value = await registration.fn(...args);
      return { ok: true, value };
    } catch (error) {
      return {
        ok: false,
        error: new ListenerError(
          `eventbus: listener for "${event}" (registered as "${registration.event}") threw`,
          { cause: error, event, pattern: registration.event, fn: registration.fn }
        ),
      };
    }
  }

  // Queue one async invocation for one registration. Per registration jobs
  // run serially (FIFO); different registrations run concurrently. The queue
  // holds at most `capacity` jobs including the one currently running.
  #enqueue(registration, event, args) {
    const depth = this.#depth(registration);
    if (depth >= this.#capacity) {
      if (this.#strategy === "drop-newest") {
        return Promise.resolve(DROPPED);
      }
      if (this.#strategy === "drop-oldest") {
        const queue = this.#queues.get(registration);
        if (queue && queue.jobs.length > 0) {
          const oldest = queue.jobs.shift();
          oldest.resolve(DROPPED);
        } else {
          // Whole capacity is occupied by in-flight jobs that cannot be
          // cancelled, so the new job is discarded.
          return Promise.resolve(DROPPED);
        }
      } else {
        return Promise.reject(this.#overflowError(event));
      }
    }
    let evicted = false;
    if (depth >= this.#capacity && this.#strategy === "drop-oldest") {
      const queue = this.#queues.get(registration);
      if (queue && queue.jobs.length > 0) {
        const oldest = queue.jobs.shift();
        oldest.resolve(DROPPED);
        evicted = true;
        // Oldest's slot transfers to the new job: do not grow the depth.
      }
    }
    if (!evicted) this.#inflight.set(registration, depth + 1);
    let queue = this.#queues.get(registration);
    if (!queue) {
      queue = { jobs: [], running: false };
      this.#queues.set(registration, queue);
    }
    const job = new Promise((resolve) => {
      queue.jobs.push({
        resolve,
        task: () => this.#runListener(registration, event, args),
      });
    });
    this.#pump(registration, queue);
    return job;
  }

  #pump(registration, queue) {
    // Serial per registration: at most one job is in flight from the queue.
    if (queue.running || queue.jobs.length === 0) return;
    queue.running = true;
    const job = queue.jobs.shift();
    queueMicrotask(() => {
      Promise.resolve()
        .then(job.task)
        .then((outcome) => {
          const remaining = this.#depth(registration) - 1;
          if (remaining <= 0) {
            this.#inflight.delete(registration);
            this.#queues.delete(registration);
          } else {
            this.#inflight.set(registration, remaining);
            queue.running = false;
          }
          job.resolve(outcome);
          if (remaining > 0) this.#pump(registration, queue);
        });
    });
  }

  async emitAsync(event, ...args) {
    checkEvent(event);
    const target = await this.#runMiddlewaresAsync(event, args);
    if (target === null) return [];
    const registrations = this.#collect(target.event);
    if (registrations.length === 0) return [];

    // Under "throw" reject before anything is enqueued, so an overflowing
    // emit never partially runs.
    if (this.#strategy === "throw") {
      const overflowed = registrations.find(
        (registration) => this.#depth(registration) >= this.#capacity
      );
      if (overflowed) throw this.#overflowError(target.event);
    }

    const jobs = registrations.map((registration) => {
      if (registration.once) this.#remove(registration);
      return this.#enqueue(registration, target.event, target.args);
    });
    const outcomes = await Promise.all(jobs);

    const failures = outcomes.filter((outcome) => {
      if (outcome === DROPPED) return false;
      return !outcome.ok;
    });
    if (failures.length > 0) {
      throw new AggregateError(
        failures.map((outcome) => outcome.error),
        `eventbus: ${failures.length} listener(s) failed for "${target.event}"`
      );
    }
    return outcomes.map((outcome) =>
      outcome === DROPPED ? DROPPED : outcome.value
    );
  }
}
