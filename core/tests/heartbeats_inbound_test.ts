import { assert, assertEquals } from "@std/assert";

import {
  deferred,
  delay,
  Heartbeat,
  NatsConnectionImpl,
  setTransportFactory,
} from "../src/internal_mod.ts";
import type { PH, Status, Transport } from "../src/internal_mod.ts";

function pm(
  disconnect: () => void,
  statusHandler: (s: Status) => void,
): PH {
  return {
    // no ping is ever answered
    flush(): Promise<void> {
      return deferred<void>();
    },
    disconnect(): void {
      disconnect();
    },
    dispatchStatus(status: Status): void {
      statusHandler(status);
    },
  };
}

function feed(hb: Heartbeat, every: number): () => void {
  const timer = setInterval(() => hb.inbound(), every);
  return () => clearInterval(timer);
}

// a server that answers the pings of the handshake and none after it
class SilentPongTransport implements Transport {
  readonly lang = "test";
  readonly version = "0.0.0";
  isClosed = false;
  closeError?: Error;
  answerPings = true;
  private frames: Uint8Array[] = [];
  private wake?: () => void;
  private done = deferred<void | Error>();

  connect(): Promise<void> {
    this.push(
      'INFO {"server_id":"fake","version":"2.10.0","max_payload":1048576,"headers":true}\r\n',
    );
    return Promise.resolve();
  }

  push(text: string): void {
    this.frames.push(new TextEncoder().encode(text));
    this.wake?.();
  }

  async *[Symbol.asyncIterator](): AsyncIterableIterator<Uint8Array> {
    while (!this.isClosed) {
      const frame = this.frames.shift();
      if (frame) {
        yield frame;
        continue;
      }
      await new Promise<void>((resolve) => this.wake = resolve);
    }
  }

  isEncrypted(): boolean {
    return false;
  }

  send(frame: Uint8Array): void {
    if (!this.answerPings) {
      return;
    }
    const pings = new TextDecoder().decode(frame).split("PING\r\n").length - 1;
    for (let i = 0; i < pings; i++) {
      this.push("PONG\r\n");
    }
  }

  close(err?: Error): Promise<void> {
    this.end(err);
    return Promise.resolve();
  }

  disconnect(): void {
    this.end();
  }

  closed(): Promise<void | Error> {
    return this.done;
  }

  discard(): void {}

  private end(err?: Error): void {
    if (this.isClosed) {
      return;
    }
    this.isClosed = true;
    this.closeError = err;
    this.wake?.();
    this.done.resolve(err);
  }
}

Deno.test("heartbeat - arriving bytes hold off maxOut until they stop", async () => {
  const disconnect = deferred<void>();
  let pings = 0;
  const ph = pm(() => {
    disconnect.resolve();
  }, (s: Status) => {
    if (s.type === "ping") pings++;
  });

  const hb = new Heartbeat(ph, 50, 2);
  hb.start();
  const stop = feed(hb, 10);
  let disconnected = false;
  disconnect.then(() => disconnected = true);
  await delay(400);
  assertEquals(disconnected, false);
  assertEquals(pings, 2);

  const stopped = Date.now();
  stop();
  await disconnect;
  // gone once the bytes are no longer fresh (one interval) and the next tick ran
  assert(Date.now() - stopped <= 160, `${Date.now() - stopped}ms`);
  assertEquals(hb.timer, undefined);
});

Deno.test("heartbeat - pingTimeout waits while bytes arrive", async () => {
  const disconnect = deferred<void>();
  const ph = pm(() => {
    disconnect.resolve();
  }, () => {});

  const hb = new Heartbeat(ph, 100, 3, 40);
  hb.start();
  const stop = feed(hb, 10);
  let disconnected = false;
  disconnect.then(() => disconnected = true);
  await delay(300);
  assertEquals(disconnected, false);
  stop();
  await disconnect;
  hb.cancel();
});

Deno.test("heartbeat - hasExpired stays false while bytes arrive past the deadline", async () => {
  const ph = pm(() => {}, () => {});
  const hb = new Heartbeat(ph, 1000, 3, 30);
  hb.start();
  hb.cancel();
  // armed with a deadline in the past and no timers running
  hb.deadline = Date.now() + 20;
  hb.lastInbound = 0;
  const stop = feed(hb, 5);
  await delay(80);
  assertEquals(hb.hasExpired(), false);
  stop();
  await delay(60);
  assertEquals(hb.hasExpired(), true);
  hb.cancel();
});

Deno.test("heartbeat - a backlog read after a freeze past the deadline does not count", () => {
  const ph = pm(() => {}, () => {});
  const hb = new Heartbeat(ph, 25000, 2, 20000);
  // last pong two minutes ago, the app was frozen since
  hb.deadline = Date.now() - 75000;
  hb.lastInbound = Date.now() - 120000;
  hb.inbound();
  hb.inbound();
  assertEquals(hb.hasExpired(), true);
});

Deno.test("heartbeat - the connection reads arriving bytes as a sign of life while pongs are queued", async () => {
  const transport = new SilentPongTransport();
  setTransportFactory({ factory: () => transport });
  const nc = await NatsConnectionImpl.connect({
    servers: "127.0.0.1:1",
    pingInterval: 50,
    maxPingOut: 1,
    reconnect: false,
  });
  transport.answerPings = false;
  const statuses: string[] = [];
  const watching = (async () => {
    for await (const s of nc.status()) statuses.push(s.type);
  })();

  const feeding = setInterval(() => transport.push("+OK\r\n"), 10);
  await delay(400);
  assertEquals(nc.isClosed(), false);
  assertEquals(statuses.includes("staleConnection"), false);

  clearInterval(feeding);
  await nc.closed();
  assert(statuses.includes("staleConnection"), statuses.join(","));
  await watching;
});

Deno.test("heartbeat - a pong to a user flush after a long silence clears isStale", async () => {
  const transport = new SilentPongTransport();
  setTransportFactory({ factory: () => transport });
  const nc = await NatsConnectionImpl.connect({
    servers: "127.0.0.1:1",
    pingInterval: 60_000,
    pingTimeout: 20_000,
    reconnect: false,
  }) as NatsConnectionImpl;
  transport.answerPings = false;
  const hb = nc.protocol.heartbeats;
  // last pong two minutes ago, the app was frozen since
  hb.deadline = Date.now() - 75_000;
  const silentSince = Date.now() - 120_000;
  hb.lastInbound = silentSince;
  assertEquals(nc.isStale(), true);

  const flushed = nc.flush();
  await delay(20);
  assertEquals(nc.isStale(), true);

  transport.push("PONG\r\n");
  await flushed;
  assertEquals(hb.lastInbound, silentSince);
  assertEquals(nc.isStale(), false);
  assert(hb.deadline >= Date.now() + 75_000, `${hb.deadline - Date.now()}ms`);

  await nc.close();
});

Deno.test("heartbeat - a pong to a ping sent before a long silence leaves isStale set", async () => {
  const transport = new SilentPongTransport();
  setTransportFactory({ factory: () => transport });
  const nc = await NatsConnectionImpl.connect({
    servers: "127.0.0.1:1",
    pingInterval: 60_000,
    pingTimeout: 20_000,
    reconnect: false,
  }) as NatsConnectionImpl;
  transport.answerPings = false;
  const flushed = nc.flush();
  await delay(20);

  // frozen for ten minutes, the pong waited in the socket backlog
  const realNow = Date.now;
  const resumed = realNow() + 10 * 60_000;
  Date.now = () => resumed;
  try {
    assertEquals(nc.isStale(), true);
    transport.push("PONG\r\n");
    await flushed;
    assertEquals(nc.isStale(), true);
  } finally {
    Date.now = realNow;
  }

  await nc.close();
});

Deno.test("heartbeat - after a long silence only the pong to a fresh flush clears isStale", async () => {
  const transport = new SilentPongTransport();
  setTransportFactory({ factory: () => transport });
  const nc = await NatsConnectionImpl.connect({
    servers: "127.0.0.1:1",
    pingInterval: 60_000,
    pingTimeout: 20_000,
    reconnect: false,
  }) as NatsConnectionImpl;
  transport.answerPings = false;
  const old = nc.flush();
  await delay(20);

  const realNow = Date.now;
  const resumed = realNow() + 10 * 60_000;
  Date.now = () => resumed;
  try {
    const fresh = nc.flush();
    transport.push("PONG\r\n");
    await old;
    assertEquals(nc.isStale(), true);

    transport.push("PONG\r\n");
    await fresh;
    assertEquals(nc.isStale(), false);
  } finally {
    Date.now = realNow;
  }

  await nc.close();
});
