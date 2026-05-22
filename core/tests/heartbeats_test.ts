/*
 * Copyright 2020-2023 The NATS Authors
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import { assert, assertEquals, fail } from "@std/assert";

import { deferred, delay, Heartbeat } from "../src/internal_mod.ts";
import type { PH, Status } from "../src/internal_mod.ts";

function pm(
  lag: number,
  disconnect: () => void,
  statusHandler: (s: Status) => void,
  skip?: number[],
): PH {
  let counter = 0;
  return {
    flush(): Promise<void> {
      counter++;
      const d = deferred<void>();
      if (skip && skip.indexOf(counter) !== -1) {
        return d;
      }
      delay(lag)
        .then(() => d.resolve());
      return d;
    },
    disconnect(): void {
      disconnect();
    },
    dispatchStatus(status: Status): void {
      statusHandler(status);
    },
  };
}
Deno.test("heartbeat - timers fire", async () => {
  const status: Status[] = [];
  const ph = pm(25, () => {
    fail("shouldn't have disconnected");
  }, (s: Status): void => {
    status.push(s);
  });

  const hb = new Heartbeat(ph, 100, 3);
  hb._schedule();
  await delay(400);
  assert(hb.timer);
  hb.cancel();
  // we can have a timer still running here - we need to wait for lag
  await delay(50);
  assertEquals(hb.timer, undefined);
  assert(status.length >= 2, `status ${status.length} >= 2`);
  assertEquals(status[0].type, "ping");
});

Deno.test("heartbeat - errors fire on missed maxOut", async () => {
  const disconnect = deferred<void>();
  const status: Status[] = [];
  const ph = pm(25, () => {
    disconnect.resolve();
  }, (s: Status): void => {
    status.push(s);
  }, [4, 5, 6]);

  const hb = new Heartbeat(ph, 100, 3);
  hb._schedule();
  await disconnect;
  assertEquals(hb.timer, undefined);
  assert(status.length >= 7, `${status.length} >= 7`);
  assertEquals(status[0].type, "ping");
});

Deno.test("heartbeat - pingTimeout disconnects before maxOut", async () => {
  // flush never resolves for any ping — without pingTimeout this would take
  // interval × maxOut = 1000ms; with pingTimeout=200 it must disconnect sooner.
  const disconnect = deferred<void>();
  const ph = pm(0, () => {
    disconnect.resolve();
  }, () => {}, [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);

  const interval = 200;
  const maxOut = 3;
  const timeout = 150;
  const hb = new Heartbeat(ph, interval, maxOut, timeout);
  const started = Date.now();
  hb._schedule();
  await disconnect;
  const elapsed = Date.now() - started;
  // first tick at `interval`, then `timeout` until the per-ping timer fires
  assert(
    elapsed < interval * maxOut,
    `elapsed ${elapsed}ms must be < ${interval * maxOut}ms (interval × maxOut)`,
  );
  assert(
    elapsed >= interval + timeout - 50,
    `elapsed ${elapsed}ms must be >= ${interval + timeout - 50}ms`,
  );
  assertEquals(hb.timer, undefined);
});

Deno.test("heartbeat - pingTimeout cleared on pong", async () => {
  // flush resolves quickly (lag=10) — pingTimeout=50 should be cleared every
  // time before it can fire, so we never disconnect.
  const ph = pm(10, () => {
    fail("shouldn't have disconnected");
  }, () => {});
  const hb = new Heartbeat(ph, 100, 3, 50);
  hb._schedule();
  await delay(400);
  hb.cancel();
  await delay(50);
  assertEquals(hb.timer, undefined);
});

Deno.test("heartbeat - recovers from missed", async () => {
  let maxPending = 0;
  const d = deferred<void>();
  const ph = pm(25, () => {
    fail("shouldn't have disconnected");
  }, (s: Status): void => {
    if (s.type === "ping") {
      // increase it
      if (s.pendingPings >= maxPending) {
        maxPending = s.pendingPings;
      } else {
        // if lower it recovered
        d.resolve();
      }
    }
  }, [4, 5]);

  const hb = new Heartbeat(ph, 100, 3);
  hb._schedule();
  await d;
  hb.cancel();
  // some resources in the test runner are not always cleaned unless we wait a bit
  await delay(500);
});
