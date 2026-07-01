/*
 * Copyright 2020-2024 The NATS Authors
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

import type { Deferred } from "./util.ts";
import { deferred } from "./util.ts";
import type { Status } from "./core.ts";

export interface PH {
  flush(p?: Deferred<void>): Promise<void>;
  disconnect(): void;
  dispatchStatus(status: Status): void;
}

export class Heartbeat {
  ph: PH;
  interval: number;
  maxOut: number;
  timeout: number;
  timer?: number;
  pendings: Promise<void>[];
  // camera.ui fork patch — absolute wall-clock deadline by which a live
  // connection must have completed a ping/pong cycle. Reset on every answered
  // ping. The `interval`/`timeout` setTimeouts are throttled (or frozen for
  // minutes) when a mobile app is backgrounded, so on resume they'd grant the
  // dead socket a whole fresh `timeout` window before noticing. Comparing
  // `Date.now()` against this deadline (see `hasExpired`) detects the stale
  // connection immediately — the same trick engine.io uses for its ping
  // timeout — instead of trusting a timer that stopped ticking.
  deadline: number;

  constructor(ph: PH, interval: number, maxOut: number, timeout = 0) {
    this.ph = ph;
    this.interval = interval;
    this.maxOut = maxOut;
    this.timeout = timeout;
    this.pendings = [];
    this.deadline = 0;
  }

  // api to start the heartbeats, since this can be
  // spuriously called from dial, ensure we don't
  // leak timers
  start() {
    this.cancel();
    this._arm();
    this._schedule();
  }

  // api for canceling the heartbeats, if stale is
  // true it will initiate a client disconnect
  cancel(stale?: boolean) {
    if (this.timer) {
      clearTimeout(this.timer);
      this.timer = undefined;
    }
    this.deadline = 0;
    this._reset();
    if (stale) {
      this.ph.disconnect();
    }
  }

  // camera.ui fork patch — push the wall-clock deadline forward by one full
  // ping/pong window. Called on start and whenever a ping is answered.
  _arm() {
    const window = this.interval + (this.timeout > 0 ? this.timeout : this.interval);
    this.deadline = Date.now() + window;
  }

  // camera.ui fork patch — wall-clock staleness check, immune to timer freezing
  // while backgrounded. `false` when disarmed (deadline === 0).
  hasExpired(): boolean {
    return this.deadline > 0 && Date.now() > this.deadline;
  }

  _schedule() {
    // @ts-ignore: node is not a number - we treat this opaquely
    this.timer = setTimeout(() => {
      // camera.ui fork patch — if the timer fires after having been frozen
      // (mobile resume) past the point a ping should have been answered, the
      // connection is stale NOW. Disconnect immediately instead of sending a
      // doomed ping and waiting out another full `timeout`.
      if (this.hasExpired()) {
        this.cancel(true);
        return;
      }
      this.ph.dispatchStatus(
        { type: "ping", pendingPings: this.pendings.length + 1 },
      );
      if (this.pendings.length === this.maxOut) {
        this.cancel(true);
        return;
      }
      const ping = deferred<void>();

      // if no PONG within `timeout` ms, kill
      // the connection without waiting for the next interval tick.
      let pingTimer: ReturnType<typeof setTimeout> | undefined;
      if (this.timeout > 0) {
        pingTimer = setTimeout(() => {
          pingTimer = undefined;
          this.cancel(true);
        }, this.timeout);
      }
      const clearPingTimer = () => {
        if (pingTimer !== undefined) {
          clearTimeout(pingTimer);
          pingTimer = undefined;
        }
      };

      this.ph.flush(ping)
        .then(() => {
          clearPingTimer();
          this._arm();
          this._reset();
        })
        .catch(() => {
          // we disconnected - pongs were rejected
          clearPingTimer();
          this.cancel();
        });
      this.pendings.push(ping);
      this._schedule();
    }, this.interval);
  }

  _reset() {
    // clear pendings after resolving them
    this.pendings = this.pendings.filter((p) => {
      const d = p as Deferred<void>;
      d.resolve();
      return false;
    });
  }
}
