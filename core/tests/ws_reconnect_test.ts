import { assert, assertEquals, assertRejects } from "@std/assert";

import { delay, wsconnect } from "../src/internal_mod.ts";

import type {
  NatsConnection,
  NatsConnectionImpl,
  Status,
} from "../src/internal_mod.ts";

type Plan =
  | "serve"
  | "silent"
  | { close: number; reason: string }
  | { infoAfter: number };

const encoder = new TextEncoder();
const decoder = new TextDecoder();
const INFO =
  'INFO {"server_id":"fake","version":"2.10.0","max_payload":1048576,"headers":true,"proto":1}\r\n';

// a NATS server behind a WebSocket endpoint, one plan per upgrade
class FakeWsServer {
  upgrades = 0;
  sockets: WebSocket[] = [];
  answerPings = true;
  private server: Deno.HttpServer<Deno.NetAddr>;
  private timers = new Set<ReturnType<typeof setTimeout>>();

  constructor(private plans: Plan[] = []) {
    this.server = Deno.serve(
      { hostname: "127.0.0.1", port: 0, onListen: () => {} },
      (req) => this.upgrade(req),
    );
  }

  get url(): string {
    return `ws://127.0.0.1:${this.server.addr.port}`;
  }

  async stop(): Promise<void> {
    for (const timer of this.timers) clearTimeout(timer);
    for (const socket of this.sockets) {
      try {
        socket.close();
      } catch {
        // already closed
      }
    }
    await this.server.shutdown();
  }

  private upgrade(req: Request): Response {
    const { socket, response } = Deno.upgradeWebSocket(req);
    const plan = this.plans[this.upgrades++] ?? "serve";
    socket.binaryType = "arraybuffer";
    this.sockets.push(socket);
    socket.onopen = () => {
      if (typeof plan === "object" && "close" in plan) {
        socket.close(plan.close, plan.reason);
      } else if (typeof plan === "object") {
        const timer = setTimeout(() => {
          this.timers.delete(timer);
          this.send(socket, INFO);
        }, plan.infoAfter);
        this.timers.add(timer);
      } else if (plan === "serve") {
        this.send(socket, INFO);
      }
    };
    socket.onmessage = (e: MessageEvent) => {
      if (!this.answerPings) return;
      const text = decoder.decode(new Uint8Array(e.data as ArrayBuffer));
      const pings = text.split("PING\r\n").length - 1;
      for (let i = 0; i < pings; i++) this.send(socket, "PONG\r\n");
    };
    return response;
  }

  private send(socket: WebSocket, text: string): void {
    if (socket.readyState === WebSocket.OPEN) socket.send(encoder.encode(text));
  }
}

function connect(
  server: FakeWsServer,
  opts: Record<string, unknown> = {},
): Promise<NatsConnection> {
  return wsconnect({
    servers: server.url,
    timeout: 5000,
    maxReconnectAttempts: -1,
    reconnectDelayHandler: () => 20,
    pingInterval: 60_000,
    ...opts,
  });
}

function watch(nc: NatsConnection): Status[] {
  const statuses: Status[] = [];
  (async () => {
    for await (const s of nc.status()) statuses.push(s);
  })().then();
  return statuses;
}

async function until(what: string, fn: () => boolean, ms = 3000) {
  const start = Date.now();
  while (!fn()) {
    if (Date.now() - start > ms) throw new Error(`timed out waiting: ${what}`);
    await delay(5);
  }
}

function closeCodes(statuses: Status[]): number[] {
  return statuses.flatMap((s) =>
    s.type === "error"
      ? [(s.error as Error & { wsCloseCode?: number }).wsCloseCode ?? -1]
      : []
  );
}

Deno.test("ws reconnect - a close code on the live connection is reported as a status error", async () => {
  const server = new FakeWsServer();
  const nc = await connect(server);
  const statuses = watch(nc);

  server.sockets[0].close(4403, "forbidden: subject not allowed");
  await until("reconnect", () => statuses.some((s) => s.type === "reconnect"));

  assertEquals(closeCodes(statuses), [4403]);
  const error = statuses.find((s) => s.type === "error");
  assert(error?.type === "error");
  assertEquals(error.error.message, "forbidden: subject not allowed");
  assertEquals((error as { data?: unknown }).data, error.error);
  assert(
    statuses.indexOf(error) <
      statuses.findIndex((s) => s.type === "disconnect"),
  );
  await nc.close();
  await server.stop();
});

Deno.test("ws reconnect - a reconnect refused with a close code is reported as a status error", async () => {
  const server = new FakeWsServer([
    "serve",
    { close: 4401, reason: "unauthorized: token expired" },
    "serve",
  ]);
  const nc = await connect(server);
  const statuses = watch(nc);

  server.sockets[0].close(1001, "going away");
  await until("reconnect", () => statuses.some((s) => s.type === "reconnect"));

  assertEquals(server.upgrades, 3);
  assertEquals(closeCodes(statuses), [4401]);
  await nc.close();
  await server.stop();
});

Deno.test("ws reconnect - a first connect refused with a close code rejects with the code", async () => {
  const server = new FakeWsServer([
    { close: 4401, reason: "unauthorized: unknown token" },
  ]);
  const err = await assertRejects(() => connect(server, { reconnect: false }));
  assertEquals((err as Error & { wsCloseCode?: number }).wsCloseCode, 4401);
  await server.stop();
});

Deno.test("ws reconnect - forceReconnect leaves a reconnect handshake in flight alone", async () => {
  const server = new FakeWsServer(["serve", { infoAfter: 300 }, "serve"]);
  const nc = await connect(server);
  const statuses = watch(nc);

  server.sockets[0].close(1001, "going away");
  await until("second upgrade", () => server.upgrades === 2);
  await nc.forceReconnect();
  await until("reconnect", () => statuses.some((s) => s.type === "reconnect"));

  assertEquals(server.upgrades, 2);
  await nc.close();
  await server.stop();
});

Deno.test("ws reconnect - forceReconnect redials at once while the loop waits between attempts", async () => {
  const server = new FakeWsServer([
    "serve",
    { close: 4500, reason: "upstream lost" },
    "serve",
  ]);
  const nc = await connect(server, { reconnectDelayHandler: () => 2000 });
  const statuses = watch(nc);

  server.sockets[0].close(4500, "upstream lost");
  await delay(2100);
  await until("refused redial", () => server.upgrades === 2);
  await delay(100);

  const forced = Date.now();
  await nc.forceReconnect();
  await until("reconnect", () => statuses.some((s) => s.type === "reconnect"));
  assert(Date.now() - forced < 1000, `${Date.now() - forced}ms`);
  await nc.close();
  await server.stop();
});

Deno.test("ws reconnect - a forceReconnect answered by the handshake in flight keeps the next backoff", async () => {
  const server = new FakeWsServer(["serve", { infoAfter: 300 }, "serve"]);
  let wait = 20;
  const nc = await connect(server, { reconnectDelayHandler: () => wait });
  const statuses = watch(nc);

  server.sockets[0].close(1001, "going away");
  await until("second upgrade", () => server.upgrades === 2);
  await nc.forceReconnect();
  await until("reconnect", () => statuses.some((s) => s.type === "reconnect"));

  wait = 1000;
  const dropped = Date.now();
  server.sockets[1].close(1001, "going away");
  await until("third upgrade", () => server.upgrades === 3);
  assert(Date.now() - dropped >= 400, `${Date.now() - dropped}ms`);
  await nc.close();
  await server.stop();
});

Deno.test("ws reconnect - a handshake cut by a transport disconnect fails at once", async () => {
  const server = new FakeWsServer(["serve", "silent", "serve"]);
  const nc = await connect(server) as NatsConnectionImpl;
  const statuses = watch(nc);

  server.sockets[0].close(1001, "going away");
  await until("second upgrade", () => server.upgrades === 2);
  const cut = Date.now();
  nc.protocol.transport.disconnect();
  await until("reconnect", () => statuses.some((s) => s.type === "reconnect"));
  assert(Date.now() - cut < 1000, `${Date.now() - cut}ms`);
  assertEquals(server.upgrades, 3);
  await nc.close();
  await server.stop();
});

Deno.test("ws reconnect - isStale follows the wall clock on a live connection", async () => {
  const server = new FakeWsServer();
  const nc = await connect(server, { pingTimeout: 20_000 });
  const realNow = Date.now;
  try {
    assertEquals(nc.isStale(), false);
    const jumped = realNow() + 10 * 60_000;
    Date.now = () => jumped;
    assertEquals(nc.isStale(), true);
  } finally {
    Date.now = realNow;
  }
  assertEquals(nc.isStale(), false);
  await nc.close();
  assertEquals(nc.isStale(), false);
  await server.stop();
});

Deno.test("ws reconnect - a late heartbeat on a live connection pings instead of dropping it", async () => {
  const server = new FakeWsServer();
  const nc = await connect(server, {
    pingInterval: 200,
    pingTimeout: 1000,
    maxPingOut: 1,
  }) as NatsConnectionImpl;
  const statuses = watch(nc);

  // the next tick finds the deadline long gone, as after a throttled minute
  nc.protocol.heartbeats.deadline = Date.now() - 60_000;
  await until("re-armed", () => nc.protocol.heartbeats.deadline > Date.now());
  assertEquals(statuses.some((s) => s.type === "staleConnection"), false);
  assertEquals(nc.isClosed(), false);

  server.answerPings = false;
  nc.protocol.heartbeats.deadline = Date.now() - 60_000;
  await until(
    "staleConnection",
    () => statuses.some((s) => s.type === "staleConnection"),
  );
  await nc.close();
  await server.stop();
});
