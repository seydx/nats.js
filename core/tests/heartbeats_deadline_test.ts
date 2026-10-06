import { assert, assertEquals } from "@std/assert";

import { deferred, Heartbeat } from "../src/internal_mod.ts";

import type { Deferred, PH, Status } from "../src/internal_mod.ts";

const realSetTimeout = globalThis.setTimeout;
const realClearTimeout = globalThis.clearTimeout;
const realNow = Date.now;

// a hand-rolled clock: wall time and timers only move when the test says so
class FakeClock {
  now = 1_700_000_000_000;
  // > 0 fires every timer at the next multiple of `align`, like the
  // once-a-minute wake-ups of a hidden browser page
  align = 0;
  private timers = new Map<number, { at: number; fn: () => void }>();
  private nextId = 1;

  install(): void {
    // @ts-ignore: numeric timer ids are all Heartbeat needs
    globalThis.setTimeout = (fn: () => void, ms = 0) => {
      const id = this.nextId++;
      this.timers.set(id, { at: this.now + ms, fn });
      return id;
    };
    // @ts-ignore: see above
    globalThis.clearTimeout = (id?: number) => {
      if (id !== undefined) this.timers.delete(id);
    };
    Date.now = () => this.now;
  }

  uninstall(): void {
    globalThis.setTimeout = realSetTimeout;
    globalThis.clearTimeout = realClearTimeout;
    Date.now = realNow;
  }

  get pending(): number {
    return this.timers.size;
  }

  // wall time passes while no timer runs: a frozen or sleeping page
  freeze(ms: number): void {
    this.now += ms;
  }

  async advance(ms: number): Promise<void> {
    const end = this.now + ms;
    while (true) {
      await settle();
      let next: [number, { at: number; fn: () => void }] | undefined;
      for (const entry of this.timers) {
        const at = this.fireAt(entry[1].at);
        if (at <= end && (!next || at < this.fireAt(next[1].at))) {
          next = entry;
        }
      }
      if (!next) break;
      this.now = Math.max(this.now, this.fireAt(next[1].at));
      this.timers.delete(next[0]);
      next[1].fn();
    }
    this.now = end;
    await settle();
  }

  private fireAt(at: number): number {
    return this.align > 0 ? Math.ceil(at / this.align) * this.align : at;
  }
}

function settle(): Promise<void> {
  return new Promise((resolve) => realSetTimeout(resolve, 0));
}

// answers like processPong: the heartbeat hears of the pong before the ping
// resolves
class FakePH implements PH {
  pings = 0;
  disconnects = 0;
  statuses: Status[] = [];
  answered: Deferred<void>[] = [];
  unanswered: { ping: Deferred<void>; sentAt: number }[] = [];
  hb?: Heartbeat;

  constructor(public answer: "now" | "never") {}

  flush(p?: Deferred<void>): Promise<void> {
    const ping = p ?? deferred<void>();
    this.pings++;
    if (this.answer === "now") {
      this.hb?.pong(Date.now());
      ping.resolve();
      this.answered.push(ping);
    } else {
      this.unanswered.push({ ping, sentAt: Date.now() });
    }
    return ping;
  }

  disconnect(): void {
    this.disconnects++;
  }

  dispatchStatus(status: Status): void {
    this.statuses.push(status);
  }

  pong(): void {
    const out = this.unanswered.shift();
    if (out) {
      this.hb?.pong(out.sentAt);
      out.ping.resolve();
    }
  }
}

async function withClock(fn: (clock: FakeClock) => Promise<void>) {
  const clock = new FakeClock();
  clock.install();
  try {
    await fn(clock);
  } finally {
    clock.uninstall();
  }
}

Deno.test("heartbeat deadline - armed on start and pushed forward by every pong", async () => {
  await withClock(async (clock) => {
    const ph = new FakePH("now");
    const hb = new Heartbeat(ph, 25_000, 1, 20_000);
    ph.hb = hb;
    const t0 = clock.now;
    hb.start();
    assertEquals(hb.deadline, t0 + 45_000);

    await clock.advance(25_000);
    assertEquals(ph.pings, 1);
    assertEquals(hb.deadline, t0 + 25_000 + 45_000);

    await clock.advance(25_000);
    assertEquals(hb.deadline, t0 + 50_000 + 45_000);
    hb.cancel();
    assertEquals(hb.deadline, 0);
  });
});

Deno.test("heartbeat deadline - hasExpired once the wall clock jumps past it, not once disarmed", async () => {
  await withClock(async (clock) => {
    const hb = new Heartbeat(new FakePH("now"), 25_000, 1, 20_000);
    hb.start();
    clock.freeze(45_000);
    assertEquals(hb.hasExpired(), false);
    clock.freeze(1);
    assertEquals(hb.hasExpired(), true);
    hb.cancel();
    assertEquals(hb.hasExpired(), false);
    await clock.advance(0);
  });
});

Deno.test("heartbeat deadline - ticks a minute apart in a hidden page keep a connection that answers", async () => {
  await withClock(async (clock) => {
    const ph = new FakePH("now");
    const hb = new Heartbeat(ph, 25_000, 1, 20_000);
    ph.hb = hb;
    hb.start();
    clock.align = 60_000;
    await clock.advance(30 * 60_000);
    assertEquals(ph.disconnects, 0);
    assert(ph.pings >= 29, `${ph.pings} pings`);
    hb.cancel();
    assertEquals(clock.pending, 0);
  });
});

Deno.test("heartbeat deadline - a late tick waits a few seconds for its pong", async () => {
  await withClock(async (clock) => {
    const ph = new FakePH("never");
    const hb = new Heartbeat(ph, 25_000, 1, 20_000);
    ph.hb = hb;
    hb.start();
    clock.freeze(60_000);
    await clock.advance(0);
    assertEquals(ph.pings, 1);
    assertEquals(ph.disconnects, 0);

    await clock.advance(2_000);
    ph.pong();
    await clock.advance(10_000);
    assertEquals(ph.disconnects, 0);
    assertEquals(hb.hasExpired(), false);
    hb.cancel();
    assertEquals(clock.pending, 0);
  });
});

Deno.test("heartbeat deadline - a tick 10 minutes late disconnects when its ping goes unanswered", async () => {
  await withClock(async (clock) => {
    const ph = new FakePH("never");
    const hb = new Heartbeat(ph, 25_000, 1, 20_000);
    hb.start();
    clock.freeze(10 * 60_000);
    await clock.advance(0);
    assertEquals(ph.pings, 1);

    await clock.advance(2_900);
    assertEquals(ph.disconnects, 0);
    await clock.advance(2_100);
    assertEquals(ph.disconnects, 1);
    assertEquals(clock.pending, 0);
  });
});

Deno.test("heartbeat deadline - a late tick with every ping outstanding disconnects at once", async () => {
  await withClock(async (clock) => {
    const ph = new FakePH("never");
    const hb = new Heartbeat(ph, 25_000, 1);
    hb.start();
    await clock.advance(25_000);
    assertEquals(ph.pings, 1);
    clock.freeze(10 * 60_000);
    await clock.advance(0);
    assertEquals(ph.pings, 1);
    assertEquals(ph.disconnects, 1);
    assertEquals(clock.pending, 0);
  });
});

Deno.test("heartbeat deadline - cancel during a late ping leaves no timer behind", async () => {
  await withClock(async (clock) => {
    const ph = new FakePH("never");
    const hb = new Heartbeat(ph, 25_000, 1, 20_000);
    hb.start();
    clock.freeze(60_000);
    await clock.advance(0);
    assert(clock.pending > 0);
    hb.cancel();
    assertEquals(clock.pending, 0);
    await clock.advance(60_000);
    assertEquals(ph.disconnects, 0);
  });
});

Deno.test("heartbeat deadline - the pong to a ping sent before a freeze does not re-arm", async () => {
  await withClock(async (clock) => {
    const ph = new FakePH("never");
    const hb = new Heartbeat(ph, 25_000, 2);
    ph.hb = hb;
    hb.start();
    await clock.advance(25_000);
    assertEquals(ph.pings, 1);

    // on resume the overdue tick runs first, then the backlog with the old pong
    clock.freeze(10 * 60_000);
    await clock.advance(0);
    assertEquals(ph.pings, 2);
    ph.pong();
    await clock.advance(0);
    assertEquals(hb.hasExpired(), true);

    ph.pong();
    await clock.advance(0);
    assertEquals(hb.hasExpired(), false);
    hb.cancel();
    assertEquals(clock.pending, 0);
  });
});
