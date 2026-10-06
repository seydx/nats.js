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

// camera.ui fork patch: how long the ping of a tick past the deadline waits
// for its pong
const LATE_PING_TIMEOUT = 4000;

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
  // camera.ui fork patch — wall-clock time of the last bytes that count as a
  // sign of life (see `inbound`). A PONG queues behind everything the server
  // sent before it, so during a long push burst the pings go unanswered for
  // a while although bytes keep arriving: those bytes keep the connection
  // from being declared stale.
  lastInbound: number;
  // camera.ui fork patch — the timers waiting for the PONG of a ping, a
  // cancel stops them all
  pingTimers: Set<ReturnType<typeof setTimeout>>;

  constructor(ph: PH, interval: number, maxOut: number, timeout = 0) {
    this.ph = ph;
    this.interval = interval;
    this.maxOut = maxOut;
    this.timeout = timeout;
    this.pendings = [];
    this.deadline = 0;
    this.lastInbound = 0;
    this.pingTimers = new Set();
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
    this.lastInbound = 0;
    for (const pingTimer of this.pingTimers) {
      clearTimeout(pingTimer);
    }
    this.pingTimers.clear();
    this._reset();
    if (stale) {
      this.ph.disconnect();
    }
  }

  // camera.ui fork patch — called with every read from the transport, before
  // it is parsed. Bytes count while the deadline holds or while counted bytes
  // keep coming within the fresh window. Bytes after a longer silence past the
  // deadline do not count: an app resumed after being frozen gets its socket
  // backlog parsed at once, which says nothing about the link now.
  inbound() {
    const now = Date.now();
    if (
      this.deadline > 0 &&
      (now <= this.deadline || now - this.lastInbound <= this._freshMs())
    ) {
      this.lastInbound = now;
    }
  }

  // camera.ui fork patch — ms until the last counted bytes are no longer
  // fresh, 0 when they are not.
  _freshFor(at: number = Date.now()): number {
    if (this.lastInbound === 0) {
      return 0;
    }
    return Math.max(0, this.lastInbound + this._freshMs() - at);
  }

  _freshMs(): number {
    return this.timeout > 0 ? this.timeout : this.interval;
  }

  // camera.ui fork patch — push the wall-clock deadline forward by one full
  // ping/pong window. Called on start and by `pong`.
  _arm() {
    const window = this.interval +
      (this.timeout > 0 ? this.timeout : this.interval);
    this.deadline = Date.now() + window;
  }

  // camera.ui fork patch: called for every pong, the heartbeat's own and a
  // user flush. It re-arms unless the heartbeat expired while its ping was
  // out: then the pong can come from the socket backlog of a frozen app and
  // says nothing about the link now. A disarmed heartbeat stays disarmed.
  pong(sentAt: number) {
    if (
      this.deadline > 0 && (!this.hasExpired() || this.hasExpired(sentAt))
    ) {
      this._arm();
    }
  }

  // camera.ui fork patch — wall-clock staleness check, immune to timer freezing
  // while backgrounded. `false` when disarmed (deadline === 0) and while
  // counted bytes are fresh.
  hasExpired(at: number = Date.now()): boolean {
    return this.deadline > 0 && at > this.deadline &&
      this._freshFor(at) === 0;
  }

  _schedule() {
    // @ts-ignore: node is not a number - we treat this opaquely
    this.timer = setTimeout(() => {
      // camera.ui fork patch: a tick past the deadline is no proof on its
      // own: hidden pages run timers once a minute while the socket still
      // answers. Its ping gets a short wait instead of the full `timeout`.
      const late = this.hasExpired();
      // camera.ui fork patch — the pongs are queued behind arriving bytes:
      // neither disconnect nor add a ping, look again next interval
      if (this.pendings.length === this.maxOut && this._freshFor() > 0) {
        this._schedule();
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
      // the connection without waiting for the next interval tick, unless
      // bytes still arrive (camera.ui fork patch): then wait for the rest of
      // their fresh window and look again.
      let pingTimer: ReturnType<typeof setTimeout> | undefined;
      const armPingTimer = (ms: number) => {
        pingTimer = setTimeout(expirePing, ms);
        this.pingTimers.add(pingTimer);
      };
      const clearPingTimer = () => {
        if (pingTimer !== undefined) {
          clearTimeout(pingTimer);
          this.pingTimers.delete(pingTimer);
          pingTimer = undefined;
        }
      };
      const expirePing = () => {
        clearPingTimer();
        const fresh = this._freshFor();
        if (fresh > 0) {
          armPingTimer(fresh);
          return;
        }
        this.cancel(true);
      };
      const wait = late
        ? Math.min(LATE_PING_TIMEOUT, this.timeout)
        : this.timeout;
      if (wait > 0) {
        armPingTimer(wait);
      }

      this.ph.flush(ping)
        .then(() => {
          // re-armed in pong(): _reset resolves unanswered pings too
          clearPingTimer();
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
