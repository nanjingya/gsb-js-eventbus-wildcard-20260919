import test from "node:test";
import assert from "node:assert/strict";
import { EventBus } from "../eventbus.js";

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
