import test from "node:test";
import assert from "node:assert/strict";
import {
  EventBus,
  DROPPED,
  BackpressureOverflowError,
  ListenerError,
} from "../eventbus.js";

// Deterministic microtask barrier: resolve `times` consecutive microtasks
// without relying on setTimeout or wall-clock timing.
function flush(times = 1) {
  let chain = Promise.resolve();
  for (let i = 0; i < times; i++) {
    chain = chain.then(() => Promise.resolve());
  }
  return chain;
}

// A gate resolves when open() is called; listeners waiting on it keep jobs
// in flight, giving deterministic control over backpressure.
function gate() {
  let open;
  const promise = new Promise((resolve) => {
    open = resolve;
  });
  return { promise: () => promise, open };
}

test("on and emit deliver arguments", () => {
  const bus = new EventBus();
  const seen = [];
  bus.on("ping", (a, b) => seen.push([a, b]));
  bus.emit("ping", 1, 2);
  assert.deepEqual(seen, [[1, 2]]);
});

test("emit without listeners is a no-op", () => {
  const bus = new EventBus();
  bus.emit("nothing");
});

test("REGRESSION 1: off removes only the targeted listener", () => {
  const bus = new EventBus();
  const calls = [];
  const first = () => calls.push("first");
  const second = () => calls.push("second");
  const third = () => calls.push("third");
  bus.on("evt", first);
  bus.on("evt", second);
  bus.on("evt", third);

  bus.off("evt", second);
  bus.emit("evt");
  assert.deepEqual(calls, ["first", "third"]);

  bus.off("evt", first);
  bus.emit("evt");
  assert.deepEqual(calls, ["first", "third", "third"]);

  bus.off("evt", third);
  assert.deepEqual(bus.eventNames(), []);
});

test("REGRESSION 1: the unsubscribe handle removes exactly its own registration", () => {
  const bus = new EventBus();
  const calls = [];
  const fn = () => calls.push("a");
  const offA = bus.on("evt", fn);
  bus.on("evt", fn); // same function, second registration
  offA();
  bus.emit("evt");
  assert.deepEqual(calls, ["a"]);
});

test("REGRESSION 2: a listener added during emit is not fired in the current dispatch", () => {
  const bus = new EventBus();
  const calls = [];
  bus.on("evt", () => {
    calls.push("first");
    bus.on("evt", () => calls.push("added"));
  });
  bus.on("evt", () => calls.push("second"));
  bus.emit("evt");
  assert.deepEqual(calls, ["first", "second"]);
  bus.emit("evt");
  assert.deepEqual(calls, ["first", "second", "first", "second", "added"]);
});

test("REGRESSION 2: a once listener does not skip later listeners and does not repeat", () => {
  const bus = new EventBus();
  const calls = [];
  bus.once("evt", () => calls.push("once"));
  bus.on("evt", () => calls.push("persistent"));
  bus.on("evt", () => calls.push("after"));
  bus.emit("evt");
  assert.deepEqual(calls, ["once", "persistent", "after"]);
  bus.emit("evt");
  assert.deepEqual(calls, ["once", "persistent", "after", "persistent", "after"]);
  assert.equal(bus.listenerCount("evt"), 2);
});

test("REGRESSION 2: off during emit does not skip other listeners", () => {
  const bus = new EventBus();
  const calls = [];
  const second = () => calls.push("second");
  bus.on("evt", () => {
    calls.push("first");
    bus.off("evt", second);
  });
  bus.on("evt", second);
  bus.on("evt", () => calls.push("third"));
  bus.emit("evt");
  assert.deepEqual(calls, ["first", "second", "third"]);
  assert.equal(bus.listenerCount("evt"), 2);
});

const WILDCARD_CASES = [
  // [pattern, emitted event, matches]
  ["user.login", "user.login", true],
  ["user.login", "user.logout", false],
  ["user.*", "user.login", true],
  ["user.*", "user.login.success", false],
  ["user.*", "user", false],
  ["*.login", "user.login", true],
  ["*.login", "admin.login", true],
  ["*.login", "admin.logout", false],
  ["**", "user", true],
  ["**", "user.login", true],
  ["**", "a.b.c.d.e", true],
  ["user.**", "user", true],
  ["user.**", "user.login", true],
  ["user.**", "user.login.success", true],
  ["user.**", "admin.login", false],
  ["**.success", "success", true],
  ["**.success", "user.login.success", true],
  ["**.success", "user.login.failed", false],
  ["a.**.c", "a.c", true],
  ["a.**.c", "a.b.c", true],
  ["a.**.c", "a.b.b.c", true],
  ["a.**.c", "a.b.d", false],
  ["a.*.c", "a.b.c", true],
  ["a.*.c", "a.b.b.c", false],
  ["a.*.c", "a.c", false],
  ["*", "anything", true],
  ["*", "a.b", false],
];

for (const [pattern, event, expected] of WILDCARD_CASES) {
  test(`wildcard: ${pattern} ${expected ? "matches" : "ignores"} ${event}`, () => {
    const bus = new EventBus();
    let hits = 0;
    bus.on(pattern, () => (hits += 1));
    bus.emit(event);
    assert.equal(hits, expected ? 1 : 0);
  });
}

test("wildcard listeners receive every matching emit, ordered by registration", () => {
  const bus = new EventBus();
  const seen = [];
  bus.on("**", () => seen.push("doubleStar"));
  bus.on("user.*", () => seen.push("singleStar"));
  bus.on("user.login", () => seen.push("exact"));
  bus.emit("user.login");
  assert.deepEqual(seen, ["doubleStar", "singleStar", "exact"]);
});

test("off on a wildcard registration removes only that registration", () => {
  const bus = new EventBus();
  const calls = [];
  const starOne = () => calls.push("star1");
  const starTwo = () => calls.push("star2");
  bus.on("user.*", starOne);
  bus.on("user.*", starTwo);
  bus.on("user.login", () => calls.push("exact"));

  bus.off("user.*", starOne);
  bus.emit("user.login");
  assert.deepEqual(calls, ["star2", "exact"]);
});

test("off with a literal event does not touch wildcard registrations", () => {
  const bus = new EventBus();
  const calls = [];
  bus.on("user.*", () => calls.push("star"));
  bus.off("user.login", () => {});
  bus.emit("user.login");
  assert.deepEqual(calls, ["star"]);
});

test("invalid event names are rejected", () => {
  const bus = new EventBus();
  assert.throws(() => bus.on("", () => {}), TypeError);
  assert.throws(() => bus.on("user..login", () => {}), TypeError);
  assert.throws(() => bus.on("user*", () => {}), TypeError);
  assert.throws(() => bus.on("a.b**", () => {}), TypeError);
});

test("emitAsync runs listeners concurrently and aggregates values in registration order", async () => {
  const bus = new EventBus();
  const order = [];
  const slowGate = gate();
  bus.on("evt", async () => {
    order.push("slow:start");
    await slowGate.promise();
    order.push("slow:end");
    return "slow";
  });
  bus.on("evt", async () => {
    order.push("fast");
    return "fast";
  });

  const resultPromise = bus.emitAsync("evt", 42);
  await flush(2);
  // Slow listener is blocked but the fast one has already run -> concurrent.
  assert.deepEqual(order, ["slow:start", "fast"]);
  slowGate.open();
  const results = await resultPromise;
  assert.deepEqual(results, ["slow", "fast"]);
  assert.deepEqual(order, ["slow:start", "fast", "slow:end"]);
});

test("emitAsync isolates failures and throws AggregateError with causes", async () => {
  const bus = new EventBus();
  const boom = new Error("boom");
  bus.on("evt", () => "ok");
  bus.on("evt", () => {
    throw boom;
  });
  bus.on("evt", async () => {
    await Promise.resolve();
    throw new TypeError("async boom");
  });

  await assert.rejects(
    bus.emitAsync("evt"),
    (error) => {
      assert.ok(error instanceof AggregateError);
      assert.equal(error.errors.length, 2);
      for (const wrapped of error.errors) {
        assert.ok(wrapped instanceof ListenerError);
        assert.equal(wrapped.event, "evt");
        assert.equal(wrapped.pattern, "evt");
        assert.equal(typeof wrapped.fn, "function");
      }
      assert.equal(error.errors[0].cause, boom);
      assert.ok(error.errors[1].cause instanceof TypeError);
      assert.match(error.errors[1].cause.message, /async boom/);
      return true;
    }
  );
});

test("emitAsync: one failing listener does not stop the others", async () => {
  const bus = new EventBus();
  let ran = false;
  bus.on("evt", () => {
    throw new Error("fail");
  });
  bus.on("evt", () => {
    ran = true;
    return "done";
  });
  await assert.rejects(bus.emitAsync("evt"), AggregateError);
  assert.equal(ran, true);
});

test("emitAsync waits for the full per-listener FIFO queue", async () => {
  const bus = new EventBus({ capacity: 4 });
  const g = gate();
  const seen = [];
  bus.on("evt", async (x) => {
    await g.promise();
    seen.push(x);
    return x;
  });
  const p1 = bus.emitAsync("evt", 1);
  const p2 = bus.emitAsync("evt", 2);
  const p3 = bus.emitAsync("evt", 3);
  g.open();
  const r = await Promise.all([p1, p2, p3]);
  assert.deepEqual(seen, [1, 2, 3]);
  assert.deepEqual(r, [[1], [2], [3]]);
});

test("middleware chain executes FIFO and can rewrite event name and payload", async () => {
  const bus = new EventBus();
  const trace = [];
  bus.use((event, args, next) => {
    trace.push("m1-before");
    const result = next("renamed", ...args.map((x) => x + 1));
    trace.push("m1-after");
    return result;
  });
  bus.use((event, args, next) => {
    trace.push(`m2:${event}:${args.join(",")}`);
    return next(event, ...args);
  });
  bus.on("renamed", (value) => trace.push(`listener:${value}`));

  bus.emit("original", 1);
  assert.deepEqual(trace, [
    "m1-before",
    "m2:renamed:2",
    "listener:2",
    "m1-after",
  ]);

  trace.length = 0;
  await bus.emitAsync("original", 10);
  assert.deepEqual(trace, [
    "m1-before",
    "m2:renamed:11",
    "listener:11",
    "m1-after",
  ]);
});

test("middleware short-circuit stops dispatch", () => {
  const bus = new EventBus();
  let reached = false;
  bus.use((event, args, next) => {
    // intentionally does not call next()
  });
  bus.on("evt", () => (reached = true));
  bus.emit("evt");
  assert.equal(reached, false);
});

test("async middleware is rejected under sync emit()", () => {
  const bus = new EventBus();
  bus.use(async (event, args, next) => next(event, ...args));
  assert.throws(() => bus.emit("evt"), /async middleware/);
});

test("middleware registration order is FIFO, not reversed", () => {
  const bus = new EventBus();
  const order = [];
  bus.use((e, a, next) => {
    order.push(1);
    return next(e, ...a);
  });
  bus.use((e, a, next) => {
    order.push(2);
    return next(e, ...a);
  });
  bus.use((e, a, next) => {
    order.push(3);
    return next(e, ...a);
  });
  bus.on("evt", () => order.push("listener"));
  bus.emit("evt");
  assert.deepEqual(order, [1, 2, 3, "listener"]);
});

test("backpressure throw: async emit rejects before partial dispatch", async () => {
  const bus = new EventBus({ capacity: 1, overflow: "throw" });
  const g = gate();
  let runs = 0;
  bus.on("evt", async () => {
    runs += 1;
    await g.promise();
  });

  const first = bus.emitAsync("evt");
  await flush(3);
  assert.equal(runs, 1);
  await assert.rejects(bus.emitAsync("evt"), (error) => {
    assert.ok(error instanceof BackpressureOverflowError);
    assert.equal(error.event, "evt");
    assert.equal(error.capacity, 1);
    assert.equal(error.strategy, "throw");
    return true;
  });
  g.open();
  await first;
  // After draining, capacity is available again.
  await bus.emitAsync("evt");
});

test("backpressure drop-newest: async overflow returns DROPPED, earlier jobs survive", async () => {
  const bus = new EventBus({ capacity: 2, overflow: "drop-newest" });
  const g = gate();
  const seen = [];
  bus.on("evt", async (x) => {
    await g.promise();
    seen.push(x);
  });

  const p1 = bus.emitAsync("evt", 1);
  const p2 = bus.emitAsync("evt", 2);
  const p3 = bus.emitAsync("evt", 3);
  g.open();
  const results = await Promise.all([p1, p2, p3]);
  assert.deepEqual(results, [[1], [2], [DROPPED]]);
  assert.deepEqual(seen, [1, 2]);
});

test("backpressure drop-oldest: async overflow evicts the oldest queued job", async () => {
  const bus = new EventBus({ capacity: 2, overflow: "drop-oldest" });
  const g = gate();
  const seen = [];
  bus.on("evt", async (x) => {
    await g.promise();
    seen.push(x);
  });

  const p1 = bus.emitAsync("evt", 1);
  const p2 = bus.emitAsync("evt", 2);
  const p3 = bus.emitAsync("evt", 3);
  g.open();
  const results = await Promise.all([p1, p2, p3]);
  assert.deepEqual(results, [[1], [DROPPED], [3]]);
  assert.deepEqual(seen, [1, 3]);
});

test("backpressure is per listener: a saturated listener does not drain other listeners' budgets", async () => {
  const bus = new EventBus({ capacity: 1, overflow: "drop-newest" });
  const g = gate();
  bus.on("evt", async () => g.promise());
  bus.on("evt", async () => "other");

  const stuck = bus.emitAsync("evt");
  await flush(3);
  // First listener is saturated; the second still has its own free slot.
  const results = await bus.emitAsync("evt");
  assert.deepEqual(results, [DROPPED, "other"]);
  g.open();
  await stuck;
});

test("backpressure sync: throw strategy fires when a reentrant listener exceeds capacity", () => {
  const bus = new EventBus({ capacity: 2, overflow: "throw" });
  let depth = 0;
  const fn = (n) => {
    depth += 1;
    if (n > 0) bus.emit("evt", n - 1);
    depth -= 1;
  };
  bus.on("evt", fn);
  assert.throws(() => bus.emit("evt", 5), BackpressureOverflowError);
});

test("backpressure sync: drop-newest silently skips the overflowing inline call", () => {
  const bus = new EventBus({ capacity: 1, overflow: "drop-newest" });
  const seen = [];
  let depth = 0;
  bus.on("evt", (n) => {
    depth += 1;
    seen.push(n);
    if (n > 0 && depth <= 1) bus.emit("evt", n - 1); // nested call is dropped
    depth -= 1;
  });
  bus.emit("evt", 0);
  assert.deepEqual(seen, [0]);
});

test("backpressure sync shares the boundary with queued async jobs", async () => {
  const bus = new EventBus({ capacity: 1, overflow: "throw" });
  const g = gate();
  bus.on("evt", async () => g.promise());
  bus.emitAsync("evt");
  await flush(3); // job now occupies the single slot
  assert.throws(() => bus.emit("evt"), BackpressureOverflowError);
  g.open();
  await flush(2);
});

test("once listeners are removed before the listener runs, even when it throws", () => {
  const bus = new EventBus();
  bus.once("evt", () => {
    throw new Error("boom");
  });
  assert.throws(() => bus.emit("evt"), /boom/);
  assert.equal(bus.listenerCount("evt"), 0);
  bus.emit("evt"); // stays removed
});

test("once works with emitAsync and aggregates after one-shot removal", async () => {
  const bus = new EventBus();
  let hits = 0;
  bus.once("evt", async () => {
    hits += 1;
    return "once";
  });
  assert.deepEqual(await bus.emitAsync("evt"), ["once"]);
  assert.deepEqual(await bus.emitAsync("evt"), []);
  assert.equal(hits, 1);
});

test("listenerCount and eventNames include wildcard registrations", () => {
  const bus = new EventBus();
  bus.on("user.login", () => {});
  bus.on("user.*", () => {});
  bus.on("**", () => {});
  bus.on("other", () => {});
  assert.equal(bus.listenerCount("user.login"), 3);
  assert.equal(bus.listenerCount("other"), 1);
  assert.equal(bus.listenerCount("nope"), 0);
  assert.deepEqual(
    [...bus.eventNames()].sort(),
    ["**", "other", "user.*", "user.login"]
  );
});

test("removeAllListeners() with no arguments clears everything", () => {
  const bus = new EventBus();
  bus.on("a", () => {});
  bus.on("b", () => {});
  bus.removeAllListeners();
  assert.deepEqual(bus.eventNames(), []);
  assert.equal(bus.listenerCount("a"), 0);
});

test("removeAllListeners(prefix) clears by event prefix, including wildcard subscribers", () => {
  const bus = new EventBus();
  const calls = [];
  bus.on("user.login", () => calls.push("login"));
  bus.on("user.logout", () => calls.push("logout"));
  bus.on("user.*", () => calls.push("star"));
  bus.on("user.**", () => calls.push("doubleStar"));
  bus.on("admin.kick", () => calls.push("kick"));

  bus.removeAllListeners("user");
  bus.emit("user.login");
  bus.emit("admin.kick");
  assert.deepEqual(calls, ["kick"]);
});

test("removeAllListeners with a wildcard filter removes matched patterns only", () => {
  const bus = new EventBus();
  bus.on("user.login", () => {});
  bus.on("user.logout", () => {});
  bus.on("admin.login", () => {});
  bus.removeAllListeners("*.login");
  assert.deepEqual([...bus.eventNames()].sort(), ["admin.login", "user.logout"]);
});
