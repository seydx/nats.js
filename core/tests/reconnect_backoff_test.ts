/*
 * Copyright 2026 The NATS Authors
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 */
import { assert, assertEquals } from "@std/assert";
import { parseOptions } from "../src/internal_mod.ts";

Deno.test("reconnect-backoff - legacy linear when reconnectionDelayMax not set", () => {
  const opts = parseOptions({
    reconnectTimeWait: 2000,
    reconnectJitter: 0,
  });
  const h = opts.reconnectDelayHandler!;
  // legacy: constant base + additive jitter (here 0) — attempt count ignored
  assertEquals(h(0), 2000);
  assertEquals(h(5), 2000);
  assertEquals(h(100), 2000);
});

Deno.test("reconnect-backoff - exponential when reconnectionDelayMax set", () => {
  const opts = parseOptions({
    reconnectTimeWait: 1000,
    reconnectionDelayMax: 30000,
  });
  const h = opts.reconnectDelayHandler!;
  // base * 2^attempt, no multiplicative jitter
  assertEquals(h(0), 1000);
  assertEquals(h(1), 2000);
  assertEquals(h(2), 4000);
  assertEquals(h(3), 8000);
  assertEquals(h(4), 16000);
  // capped
  assertEquals(h(5), 30000);
  assertEquals(h(10), 30000);
});

Deno.test("reconnect-backoff - randomization factor produces values within ±factor", () => {
  const opts = parseOptions({
    reconnectTimeWait: 1000,
    reconnectionDelayMax: 60000,
    reconnectionRandomizationFactor: 0.5,
  });
  const h = opts.reconnectDelayHandler!;
  // attempt=3 → base delay = 8000, with ±50% jitter → range [4000, 12000]
  for (let i = 0; i < 200; i++) {
    const v = h(3);
    assert(v >= 4000, `${v} >= 4000`);
    assert(v <= 12000, `${v} <= 12000`);
  }
});

Deno.test("reconnect-backoff - randomization factor on capped delay", () => {
  const opts = parseOptions({
    reconnectTimeWait: 1000,
    reconnectionDelayMax: 5000,
    reconnectionRandomizationFactor: 0.5,
  });
  const h = opts.reconnectDelayHandler!;
  // attempt=10 would yield 1024000, capped to 5000 → jitter then [2500, 7500]
  for (let i = 0; i < 200; i++) {
    const v = h(10);
    assert(v >= 2500, `${v} >= 2500`);
    assert(v <= 7500, `${v} <= 7500`);
  }
});

Deno.test("reconnect-backoff - custom handler overrides defaults", () => {
  let seen = -1;
  const opts = parseOptions({
    reconnectionDelayMax: 30000, // would normally activate exp backoff
    reconnectDelayHandler: (attempt: number) => {
      seen = attempt;
      return 42;
    },
  });
  assertEquals(opts.reconnectDelayHandler!(7), 42);
  assertEquals(seen, 7);
});

Deno.test("reconnect-backoff - legacy 0-arity handler still callable", () => {
  // A handler declared without the `attempt` param must still work — JS
  // function arity is loose, TypeScript widens 0-arg fns to N-arg fns.
  const legacy = (): number => 123;
  const opts = parseOptions({
    reconnectDelayHandler: legacy,
  });
  assertEquals(opts.reconnectDelayHandler!(99), 123);
});
