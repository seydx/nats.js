import { assert, assertEquals } from "@std/assert";
import { constants, createDeflateRaw } from "node:zlib";

import { delay, wsconnect } from "../src/internal_mod.ts";

import type { NatsConnection, Status } from "../src/internal_mod.ts";

const encoder = new TextEncoder();
const decoder = new TextDecoder();
const INFO =
  'INFO {"server_id":"fake","version":"2.10.0","max_payload":1048576,"headers":true,"proto":1}\r\n';

function concat(...parts: Uint8Array[]): Uint8Array {
  const out = new Uint8Array(parts.reduce((n, p) => n + p.length, 0));
  let offset = 0;
  for (const part of parts) {
    out.set(part, offset);
    offset += part.length;
  }
  return out;
}

class Deflater {
  private zlib = createDeflateRaw({ level: 1 });
  private output: Uint8Array[] = [];

  constructor() {
    this.zlib.on("data", (chunk: Uint8Array) => this.output.push(chunk));
  }

  frame(bytes: Uint8Array): Promise<Uint8Array> {
    return new Promise((resolve) => {
      this.zlib.write(bytes);
      this.zlib.flush(constants.Z_SYNC_FLUSH, () => {
        const body = concat(...this.output.splice(0));
        const header = new Uint8Array(5);
        header[0] = 1;
        new DataView(header.buffer).setUint32(1, bytes.length);
        resolve(concat(header, body.subarray(0, body.length - 4)));
      });
    });
  }

  close(): void {
    this.zlib.close();
  }
}

// a camera.ui relay: tags every frame, 0 raw, 1 deflated on one stream per connection with its inflated length in front
class FakeRelay {
  urls: string[] = [];
  sockets: WebSocket[] = [];
  sids = new Map<string, string>();
  private server: Deno.HttpServer<Deno.NetAddr>;
  private deflaters = new Map<WebSocket, Deflater>();
  private chain = Promise.resolve();

  constructor(private framing: boolean) {
    this.server = Deno.serve(
      { hostname: "127.0.0.1", port: 0, onListen: () => {} },
      (req) => this.upgrade(req),
    );
  }

  get url(): string {
    return `ws://127.0.0.1:${this.server.addr.port}/api/proxy?token=abc`;
  }

  // in order: a deflated frame waits for zlib, the raw frames behind it wait too
  send(bytes: Uint8Array, deflated = false, socket = this.sockets.at(-1)!) {
    this.chain = this.chain.then(async () => {
      if (socket.readyState !== WebSocket.OPEN) return;
      if (!this.framing) socket.send(bytes);
      else if (!deflated) socket.send(concat(new Uint8Array([0]), bytes));
      else socket.send(await this.deflaters.get(socket)!.frame(bytes));
    });
    return this.chain;
  }

  sendRaw(frame: Uint8Array) {
    this.chain = this.chain.then(() => this.sockets.at(-1)!.send(frame));
    return this.chain;
  }

  publish(subject: string, payload: Uint8Array, deflated: boolean) {
    const head = encoder.encode(
      `MSG ${subject} ${this.sids.get(subject)} ${payload.length}\r\n`,
    );
    return this.send(concat(head, payload, encoder.encode("\r\n")), deflated);
  }

  async stop(): Promise<void> {
    for (const socket of this.sockets) {
      try {
        socket.close();
      } catch {
        // already closed
      }
    }
    for (const deflater of this.deflaters.values()) deflater.close();
    await this.server.shutdown();
  }

  private upgrade(req: Request): Response {
    this.urls.push(req.url);
    const { socket, response } = Deno.upgradeWebSocket(req);
    socket.binaryType = "arraybuffer";
    this.sockets.push(socket);
    this.deflaters.set(socket, new Deflater());
    socket.onopen = () => this.send(encoder.encode(INFO), true, socket);
    socket.onmessage = (e: MessageEvent) => {
      const text = decoder.decode(new Uint8Array(e.data as ArrayBuffer));
      for (const m of text.matchAll(/SUB (\S+) (\d+)\r\n/g)) {
        this.sids.set(m[1], m[2]);
      }
      const pings = text.split("PING\r\n").length - 1;
      for (let i = 0; i < pings; i++) {
        this.send(encoder.encode("PONG\r\n"), false, socket);
      }
    };
    return response;
  }
}

function connect(server: FakeRelay): Promise<NatsConnection> {
  return wsconnect({
    servers: server.url,
    timeout: 5000,
    maxReconnectAttempts: -1,
    reconnectDelayHandler: () => 20,
    pingInterval: 60_000,
  });
}

function json(bytes: number): Uint8Array {
  const row = '{"id":"cam-1","name":"Front door","labels":["person","car"]},';
  return encoder.encode(
    row.repeat(Math.ceil(bytes / row.length)).slice(0, bytes),
  );
}

async function until(what: string, fn: () => boolean, ms = 3000) {
  const start = Date.now();
  while (!fn()) {
    if (Date.now() - start > ms) throw new Error(`timed out waiting: ${what}`);
    await delay(5);
  }
}

async function received(
  nc: NatsConnection,
  subject: string,
  count: number,
  send: () => Promise<void>,
): Promise<Uint8Array[]> {
  const sub = nc.subscribe(subject, { max: count });
  await nc.flush();
  const got: Uint8Array[] = [];
  const done = (async () => {
    for await (const m of sub) got.push(m.data);
  })();
  await send();
  await done;
  return got;
}

Deno.test("ws deflate - asks the relay for frame deflate in the url", async () => {
  const server = new FakeRelay(false);
  const nc = await connect(server);
  const url = new URL(server.urls[0]);
  assertEquals(url.searchParams.get("compress"), "cui-deflate");
  assertEquals(url.searchParams.get("token"), "abc");
  await nc.close();
  await server.stop();
});

Deno.test("ws deflate - inflates deflated frames in order with raw ones", async () => {
  const server = new FakeRelay(true);
  const nc = await connect(server);
  const payloads = [
    json(20_000),
    crypto.getRandomValues(new Uint8Array(4096)),
    json(300),
    encoder.encode("short"),
    json(70_000),
  ];

  const got = await received(nc, "x", payloads.length + 1, async () => {
    for (let i = 0; i < payloads.length; i++) {
      server.publish("x", payloads[i], i % 2 === 0);
    }
    // one message across a deflated and a raw frame
    const split = json(1000);
    await server.send(
      encoder.encode(`MSG x ${server.sids.get("x")} 1000\r\n`),
      true,
    );
    await server.send(concat(split, encoder.encode("\r\n")), false);
    payloads.push(split);
  });

  assertEquals(got, payloads);
  await nc.close();
  await server.stop();
});

Deno.test("ws deflate - stays raw with a relay that does not frame", async () => {
  const server = new FakeRelay(false);
  const nc = await connect(server);
  const payload = concat(new Uint8Array([1, 0, 0, 0, 9]), json(500));

  const got = await received(nc, "x", 1, async () => {
    await server.send(
      encoder.encode(`MSG x ${server.sids.get("x")} ${payload.length}\r\n`),
    );
    await server.send(concat(payload, encoder.encode("\r\n")));
  });

  assertEquals(got, [payload]);
  await nc.close();
  await server.stop();
});

Deno.test("ws deflate - a frame that does not inflate drops the connection", async () => {
  const server = new FakeRelay(true);
  const nc = await connect(server);
  const statuses: Status[] = [];
  (async () => {
    for await (const s of nc.status()) statuses.push(s);
  })().then();

  await server.sendRaw(new Uint8Array([1, 0, 0, 1, 0, 0xff, 0xfe, 0xfd, 0xfc]));
  await until("reconnect", () => statuses.some((s) => s.type === "reconnect"));
  assert(statuses.some((s) => s.type === "disconnect"));
  assertEquals(server.sockets.length, 2);
  await nc.close();
  await server.stop();
});

Deno.test("ws deflate - a runtime without deflate-raw does not ask", async () => {
  const native = globalThis.DecompressionStream;
  globalThis.DecompressionStream = class {
    constructor() {
      throw new TypeError("Unsupported compression format: 'deflate-raw'");
    }
  } as unknown as typeof DecompressionStream;
  try {
    const server = new FakeRelay(false);
    const nc = await connect(server);
    assertEquals(new URL(server.urls[0]).searchParams.get("compress"), null);
    await nc.close();
    await server.stop();
  } finally {
    globalThis.DecompressionStream = native;
  }
});
