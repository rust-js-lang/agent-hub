import assert from "node:assert/strict";
import { test } from "node:test";
import { listen, Subscription } from "../dashboard/src/live.js";
import { EVENTS } from "../protocol/src/endpoints.js";

// Exercise the generated production module. The fixture replaces browser I/O
// and time, while the Rust source still owns every recovery decision.
async function fixture(run) {
  const original = { window: globalThis.window, EventSource: globalThis.EventSource,
    setTimeout: globalThis.setTimeout, clearTimeout: globalThis.clearTimeout, now: Date.now };
  let now = 1000;
  let sequence = 0;
  let reloads = 0;
  const timers = new Map();
  const sources = [];
  const subscriptions = [];
  const hubs = [];
  const problems = [];
  const states = [];
  const probes = [];
  let refreshes = 0;
  const info = { version: "0.1.3", build: "initial", started: 1, protocol: 1, features: [] };
  const response = (value = info) => new Response(JSON.stringify(value), {
    headers: { "content-type": "application/json", "agent-hub-protocol": "1" },
  });
  let fetcher = async () => response();
  const flush = async () => { for (let i = 0; i < 3; i++) await new Promise(setImmediate); };
  const tick = async milliseconds => {
    const end = now + milliseconds;
    for (;;) {
      await flush();
      const due = [...timers].filter(([, t]) => t.at <= end).sort((a, b) => a[1].at - b[1].at)[0];
      if (!due) break;
      const [id, timer] = due;
      timers.delete(id);
      now = timer.at;
      timer.callback();
    }
    now = end;
    await flush();
  };
  class Source extends EventTarget {
    readyState = 0;
    closed = false;
    constructor(url) {
      super();
      this.url = url;
      this.created = now;
      sources.push(this);
      queueMicrotask(() => { if (!this.closed) this.emit("open", 1); });
    }
    emit(type, state = this.readyState) {
      this.readyState = state;
      this.dispatchEvent(new Event(type));
    }
    frame(data) { this.dispatchEvent(new MessageEvent("message", { data })); }
    close() { this.closed = true; this.readyState = 2; }
  }
  globalThis.EventSource = Source;
  globalThis.window = {
    fetch: (...args) => { probes.push(now); return fetcher(...args); },
    location: { reload: () => { reloads++; } },
  };
  globalThis.setTimeout = (callback, delay = 0) => {
    const id = ++sequence;
    timers.set(id, { callback, at: now + delay });
    return id;
  };
  globalThis.clearTimeout = id => timers.delete(id);
  Date.now = () => now;
  const start = () => {
    const subscription = listen(hub => hubs.push(hub), live => states.push(live),
      () => { refreshes++; }, problem => problems.push(problem));
    subscriptions.push(subscription);
    return subscription;
  };
  try {
    await run({ start, stop: subscription => Subscription.stop(subscription), tick, flush, response, info,
      sources, hubs, problems, states, probes, timers,
      fetch: callback => { fetcher = callback; },
      get refreshes() { return refreshes; }, get reloads() { return reloads; } });
  } finally {
    for (const subscription of subscriptions) Subscription.stop(subscription);
    Object.assign(globalThis, { window: original.window, EventSource: original.EventSource,
      setTimeout: original.setTimeout, clearTimeout: original.clearTimeout });
    Date.now = original.now;
  }
}

test("CONNECTING leaves recovery to EventSource", () => fixture(async f => {
  f.start();
  assert.equal(f.sources.length, 0, "mount must negotiate before constructing a source");
  await f.flush();
  assert.equal(f.probes.length, 2); // Mount, then open.
  f.sources[0].emit("error", 0);
  await f.tick(60_000);
  assert.equal(f.probes.length, 2);
  assert.equal(f.sources.length, 1);
  assert.equal(f.timers.size, 0);
}));

test("repeated CLOSED errors keep one timer; open resets the backoff", () => fixture(async f => {
  f.start();
  await f.flush();
  f.sources[0].emit("error", 2);
  f.sources[0].emit("error", 2);
  await f.flush();
  assert.equal(f.timers.size, 1);
  await f.tick(999);
  assert.equal(f.sources.length, 1);
  await f.tick(1);
  assert.equal(f.sources.length, 2);
  assert.equal(f.sources[0].closed, true);
  assert.equal(f.sources[1].closed, false);
  f.sources[1].emit("error", 2);
  await f.flush();
  await f.tick(999);
  assert.equal(f.sources.length, 2);
  await f.tick(1);
  assert.equal(f.sources.length, 3);
  assert.equal(f.sources.filter(s => !s.closed).length, 1);
}));

test("an offline mount retries negotiation before creating its first source", () => fixture(async f => {
  let failures = 2;
  f.fetch(async () => { if (failures-- > 0) throw new TypeError("offline"); return f.response(); });
  f.start();
  await f.flush();
  assert.equal(f.sources.length, 0);
  await f.tick(1000);
  assert.equal(f.sources.length, 0);
  await f.tick(2000);
  assert.equal(f.sources.length, 1);
  assert.equal(f.sources[0].created, 4000);
  assert.equal(f.states.at(-1), true);
}));

test("missing advertised stream requirements refuse before source construction", () => fixture(async f => {
  // Model a newly gated stream using its real definition and transport.
  const previous = EVENTS.route.needs;
  EVENTS.route.needs = "WriteNonce";
  try {
    f.start();
    await f.flush();
    assert.equal(f.sources.length, 0);
    assert.match(f.problems[0], /write-nonce/);
    f.info.features = ["write-nonce"];
    await f.tick(1000);
    assert.equal(f.sources.length, 1);
    assert.equal(f.states.at(-1), true);
  } finally {
    EVENTS.route.needs = previous;
  }
}));

test("unreachable probes continue with 1s, 2s, 5s backoff until reachable", () => fixture(async f => {
  f.start();
  await f.flush();
  let failures = 3;
  f.fetch(async () => { if (failures-- > 0) throw new TypeError("offline"); return f.response(); });
  f.sources[0].emit("error", 2);
  await f.flush();
  await f.tick(1000);
  await f.tick(2000);
  await f.tick(5000);
  assert.deepEqual(f.probes.slice(2), [1000, 2000, 4000, 9000]);
  assert.equal(f.sources.length, 1);
  await f.tick(10_000);
  assert.equal(f.sources.length, 2);
  assert.equal(f.sources[1].closed, false);
}));

test("a changed build on the same protocol reconnects and reports its metadata", () => fixture(async f => {
  f.start();
  await f.flush();
  f.info.build = "changed";
  f.sources[0].emit("error", 2);
  await f.flush();
  assert.equal(f.hubs.at(-1).build, "changed");
  await f.tick(1000);
  assert.equal(f.sources.length, 2);
  assert.equal(f.reloads, 0);
}));

test("a long outage caps probe backoff at thirty seconds", () => fixture(async f => {
  f.start();
  await f.flush();
  f.fetch(async () => { throw new TypeError("offline"); });
  f.sources[0].emit("error", 2);
  await f.flush();
  for (const delay of [1, 2, 5, 10, 20, 30, 30, 30]) await f.tick(delay * 1000);
  assert.deepEqual(f.probes.slice(2), [1000, 2000, 4000, 9000, 19000, 39000, 69000, 99000, 129000]);
  assert.equal(f.sources.length, 1);
  assert.equal(f.timers.size, 1);
}));

test("a late incompatible probe cannot override a newer connection", () => fixture(async f => {
  f.start();
  await f.flush();
  let resolve;
  f.fetch(() => new Promise(answer => { resolve = answer; }));
  f.sources[0].emit("open", 1);
  await f.flush();
  f.fetch(async () => f.response());
  f.sources[0].emit("error", 2);
  await f.flush();
  await f.tick(1000);
  assert.equal(f.sources.length, 2);
  resolve(f.response({ ...f.info, protocol: 2 }));
  await f.flush();
  assert.equal(f.reloads, 0);
  assert.equal(f.sources[1].closed, false);
}));

test("stop cancels both a pending timer and an in-flight probe", () => fixture(async f => {
  const first = f.start();
  await f.flush();
  f.sources[0].emit("error", 2);
  await f.flush();
  assert.equal(f.timers.size, 1);
  f.stop(first);
  await f.tick(30_000);
  assert.equal(f.sources.length, 1);
  assert.equal(f.timers.size, 0);
  let resolve;
  f.fetch(() => new Promise(answer => { resolve = answer; }));
  const second = f.start();
  await f.flush();
  f.stop(second);
  resolve(f.response({ ...f.info, protocol: 2 }));
  await f.tick(30_000);
  assert.equal(f.sources.length, 1, "a stopped mount probe must never create a source");
  assert.equal(f.sources[0].closed, true);
  assert.equal(f.reloads, 0);
}));

test("unknown frames are skipped and malformed known frames retain the stream", () => fixture(async f => {
  f.start();
  await f.flush();
  const before = f.refreshes;
  f.sources[0].frame('{"type":"future-event","extra":true}');
  assert.equal(f.refreshes, before);
  f.sources[0].frame('{"type":"sent"}');
  assert.equal(f.problems.length, 1);
  assert.equal(f.sources.length, 1);
  assert.equal(f.sources[0].closed, false);
  f.sources[0].frame(JSON.stringify({ type: "agent", agent: {
    name: "a", machine: "test", lastSeen: 1, watching: false, unread: 0, person: false,
  } }));
  assert.equal(f.refreshes, before + 1);
  const query = new URL(f.sources[0].url, "http://test").searchParams;
  assert.equal(query.get("protocol"), "1");
  assert.equal(query.get("types"), "sent,read,agent,retired,restarting");
}));

test("incompatible protocol closes recovery and requests exactly one reload", () => fixture(async f => {
  f.start();
  await f.flush();
  f.info.protocol = 2;
  f.sources[0].emit("error", 2);
  await f.flush();
  f.sources[0].emit("error", 2);
  await f.tick(60_000);
  assert.equal(f.reloads, 1);
  assert.equal(f.sources.length, 1);
  assert.equal(f.sources[0].closed, true);
  assert.equal(f.timers.size, 0);
}));
