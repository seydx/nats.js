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

import type {
  ConnectionOptions,
  NatsConnection,
  Server,
  ServerInfo,
} from "./core.ts";
import type { Deferred } from "./util.ts";
import { deferred, delay, render } from "./util.ts";
import type { Transport, TransportFactory } from "./transport.ts";
import { extractProtocolMessage, setTransportFactory } from "./transport.ts";
import { checkOptions } from "./options.ts";
import { DataBuffer } from "./databuffer.ts";
import { INFO } from "./protocol.ts";
import { NatsConnectionImpl } from "./nats.ts";
import { version } from "./version.ts";
import { errors, InvalidArgumentError } from "./errors.ts";

const VERSION = version;
const LANG = "nats.ws";

// camera.ui fork patch: browsers behind Cloudflare lose permessage-deflate, so
// a camera.ui relay that serves this client from beyond the LAN deflates its
// frames itself when the url asks for it. It then tags every frame: 0 raw,
// 1 deflated, led by the inflated length (uint32) and sharing one stream.
const FRAME_DEFLATE_PARAM = "compress=cui-deflate";
const FRAME_RAW = 0;
const FRAME_DEFLATED = 1;
const DEFLATED_HEADER = 5;
// the empty stored block of a sync flush, the relay strips it from every frame
const SYNC_FLUSH_TAIL = new Uint8Array([0x00, 0x00, 0xff, 0xff]);

function withFrameDeflate(url: string): string {
  return `${url}${url.includes("?") ? "&" : "?"}${FRAME_DEFLATE_PARAM}`;
}

function inflateStream(): DecompressionStream | null {
  try {
    return new DecompressionStream("deflate-raw");
  } catch {
    return null;
  }
}

class FrameInflater {
  private writer: WritableStreamDefaultWriter<BufferSource>;
  private reader: ReadableStreamDefaultReader<Uint8Array>;
  private backlog: Uint8Array[] = [];
  private busy = false;
  private closed = false;

  constructor(
    stream: DecompressionStream,
    private deliver: (chunk: Uint8Array) => void,
    private fail: (err: Error) => void,
  ) {
    this.writer = stream.writable.getWriter();
    this.reader = stream.readable.getReader();
  }

  push(frame: Uint8Array): void {
    if (this.busy) {
      this.backlog.push(frame);
      return;
    }
    if (frame[0] === FRAME_RAW) {
      this.deliver(frame.subarray(1));
      return;
    }
    this.busy = true;
    this.drain(frame).catch((err) => {
      if (!this.closed) this.fail(err);
    });
  }

  close(): void {
    if (this.closed) return;
    this.closed = true;
    this.backlog = [];
    this.writer.abort().catch(() => {});
  }

  private async drain(first: Uint8Array): Promise<void> {
    let frame: Uint8Array | undefined = first;
    while (frame && !this.closed) {
      if (frame[0] === FRAME_RAW) this.deliver(frame.subarray(1));
      else if (frame[0] === FRAME_DEFLATED && frame.length > DEFLATED_HEADER) {
        await this.inflate(frame);
      } else throw new errors.ProtocolError("unexpected frame from relay");
      frame = this.backlog.shift();
    }
    this.busy = false;
  }

  private async inflate(frame: Uint8Array): Promise<void> {
    let remaining = new DataView(frame.buffer, frame.byteOffset, frame.length)
      .getUint32(1);
    const input = new Uint8Array(frame.length - 1);
    input.set(frame.subarray(DEFLATED_HEADER));
    input.set(SYNC_FLUSH_TAIL, frame.length - DEFLATED_HEADER);
    const written = this.writer.write(input);
    // a corrupt frame rejects both sides, the read below reports it
    written.catch(() => {});
    while (remaining > 0) {
      const { value, done } = await this.reader.read();
      if (done) throw new errors.ProtocolError("relay frame stream ended");
      remaining -= value.length;
      if (remaining < 0) {
        throw new errors.ProtocolError("relay frame longer than announced");
      }
      this.deliver(value);
    }
    await written;
  }
}

/**
 * WsSocketFactory is a factory that returns a WebSocket and a boolean
 * indicating if the connection is encrypted. Client code is responsible
 * for creating a W3C WebSocket compliant transport.
 *
 * @param u the url to connect to
 * @param opts the connection options
 * @returns a promise that resolves to a WebSocket and a boolean indicating if
 * the connection is encrypted
 */
export type WsSocketFactory = (u: string, opts: ConnectionOptions) => Promise<{
  socket: WebSocket;
  encrypted: boolean;
}>;

/**
 * WsConnectionOptions exposes wsconnect specific options not applicable to
 * other transports.
 */
export interface WsConnectionOptions extends ConnectionOptions {
  wsFactory?: WsSocketFactory;
}

export class WsTransport implements Transport {
  version: string;
  lang: string;
  closeError?: Error;
  connected: boolean;
  private done: boolean;
  // @ts-ignore: expecting global WebSocket
  private socket: WebSocket;
  private options!: WsConnectionOptions;
  socketClosed: boolean;
  encrypted: boolean;
  peeked: boolean;

  yields: Uint8Array[];
  signal: Deferred<void>;
  closedNotification: Deferred<void | Error>;
  // camera.ui fork patch — the in-flight connect() deferred, so _closed()
  // can settle a dial attempt that is killed mid-handshake (see below).
  private pendingConnect: Deferred<void> | null;
  // camera.ui fork patch: decided by the first frame, null for a plain server
  private inflater?: FrameInflater | null;

  constructor() {
    this.version = VERSION;
    this.lang = LANG;
    this.connected = false;
    this.done = false;
    this.socketClosed = false;
    this.encrypted = false;
    this.peeked = false;
    this.yields = [];
    this.signal = deferred();
    this.closedNotification = deferred();
    this.pendingConnect = null;
  }

  async connect(
    server: Server,
    options: WsConnectionOptions,
  ): Promise<void> {
    const connected = false;
    const ok = deferred<void>();
    this.pendingConnect = ok;

    this.options = options;
    const stream = inflateStream();
    const u = stream ? withFrameDeflate(server.src) : server.src;
    if (options.wsFactory) {
      const { socket, encrypted } = await options.wsFactory(u, options);
      this.socket = socket;
      this.encrypted = encrypted;
    } else {
      this.encrypted = u.indexOf("wss://") === 0;
      this.socket = new WebSocket(u);
    }
    this.socket.binaryType = "arraybuffer";

    this.socket.onopen = () => {
      if (this.done) {
        this._closed(new Error("aborted"));
      }
      // we don't do anything here...
    };

    const receive = (chunk: Uint8Array): void => {
      if (this.done) {
        return;
      }
      this.yields.push(chunk);
      if (this.peeked) {
        this.signal.resolve();
        return;
      }
      const t = DataBuffer.concat(...this.yields);
      const pm = extractProtocolMessage(t);
      if (pm !== "") {
        const m = INFO.exec(pm);
        if (!m) {
          if (options.debug) {
            console.error("!!!", render(t));
          }
          ok.reject(new Error("unexpected response from server"));
          return;
        }
        try {
          const info = JSON.parse(m[1]) as ServerInfo;
          checkOptions(info, this.options);
          this.peeked = true;
          this.connected = true;
          this.signal.resolve();
          ok.resolve();
        } catch (err) {
          ok.reject(err);
          return;
        }
      }
    };

    this.socket.onmessage = (me: MessageEvent) => {
      if (this.done) {
        return;
      }
      const frame = new Uint8Array(me.data);
      if (this.inflater === undefined) {
        this.inflater = stream && frame[0] <= FRAME_DEFLATED
          ? new FrameInflater(stream, receive, (err) => this._closed(err))
          : null;
      }
      if (this.inflater) this.inflater.push(frame);
      else receive(frame);
    };

    // @ts-ignore: CloseEvent is provided in browsers
    this.socket.onclose = (evt: CloseEvent) => {
      let reason: Error | undefined;
      // camera.ui fork patch — application close codes (4000-4999) carry
      // semantic information from the server (e.g. 4401 = auth required).
      // Surface them as Errors regardless of `wasClean`, since a server-
      // initiated `close(code, reason)` post-upgrade lands as a clean close
      // and the original `!evt.wasClean` gate would silently drop it.
      if (evt.code >= 4000 && evt.code <= 4999) {
        reason = new Error(evt.reason || `close code ${evt.code}`);
      } else if (!evt.wasClean && evt.reason !== "") {
        reason = new Error(evt.reason);
      }
      if (reason) {
        // camera.ui fork patch — carry the close code structurally so
        // consumers can classify without parsing reason strings
        (reason as Error & { wsCloseCode?: number }).wsCloseCode = evt.code;
      }
      this._closed(reason);
      this._cleanup();
    };

    // @ts-ignore: signature can be any
    this.socket.onerror = (e: ErrorEvent | Event): void => {
      if (this.done) {
        return;
      }
      const evt = e as ErrorEvent;
      const err = new errors.ConnectionError(evt.message);
      if (!connected) {
        ok.reject(err);
      } else {
        this._closed(err);
      }
      this._cleanup();
    };
    return ok;
  }

  _cleanup() {
    this.inflater?.close();
    if (this.socketClosed === false) {
      // node seems to not emit closed if there's an error
      // all other runtimes do.
      this.socketClosed = true;
      this.socket.onopen = null;
      this.socket.onmessage = null;
      this.socket.onerror = null;
      this.socket.onclose = null;
      this.closedNotification.resolve(this.closeError);
    }
  }

  disconnect(): void {
    this._closed(undefined, true);
  }

  private async _closed(err?: Error, _internal = true): Promise<void> {
    // camera.ui fork patch — settle any in-flight connect() first. A dial
    // attempt killed mid-handshake (forceReconnect() during a dial calls
    // transport.disconnect()) otherwise never observes the closure: onclose/
    // onerror get nulled by _cleanup() and the dial hangs on its deferred
    // until the protocol's raceTimer (options.timeout, default 20s) fires.
    // Settling after resolve/reject is a no-op, so established connections
    // are unaffected.
    this.pendingConnect?.reject(
      err ?? new errors.ConnectionError("transport closed during handshake"),
    );
    if (this.done) {
      try {
        this.socket.close();
      } catch (_) {
        // nothing
      }
      return;
    }
    this.closeError = err;
    if (!err) {
      // camera.ui fork patch — bound the graceful-drain wait.
      // A silent-dead TCP socket (5G handoff, carrier NAT-evict, etc.) keeps
      // bufferedAmount > 0 forever because the OS never gets to flush the
      // bytes and never reports the failure. Upstream waits indefinitely,
      // which traps the protocol in `staleConnection` with no recovery path.
      // After DRAIN_TIMEOUT_MS we accept the loss and force-close below.
      const DRAIN_TIMEOUT_MS = 3000;
      const drainStart = Date.now();
      while (!this.socketClosed && this.socket.bufferedAmount > 0) {
        if (Date.now() - drainStart >= DRAIN_TIMEOUT_MS) break;
        await delay(100);
      }
    }
    this.done = true;
    try {
      this.socket.close();
    } catch (_) {
      // ignore this
    }

    // camera.ui fork patch — guarantee `closedNotification` resolves even if
    // the underlying WebSocket never fires `onclose`/`onerror` (zombie sockets
    // observed in WKWebView on flaky cellular). _cleanup() is idempotent —
    // if onclose does fire later, the second call is a no-op.
    this._cleanup();

    return this.closedNotification as Promise<void>;
  }

  get isClosed(): boolean {
    return this.done;
  }

  [Symbol.asyncIterator]() {
    return this.iterate();
  }

  async *iterate(): AsyncIterableIterator<Uint8Array> {
    while (true) {
      if (this.done) {
        return;
      }
      if (this.yields.length === 0) {
        await this.signal;
      }
      const yields = this.yields;
      this.yields = [];
      for (let i = 0; i < yields.length; i++) {
        if (this.options.debug) {
          console.info(`> ${render(yields[i])}`);
        }
        yield yields[i];
      }
      // yielding could have paused and microtask
      // could have added messages. Prevent allocations
      // if possible
      if (this.done) {
        break;
      } else if (this.yields.length === 0) {
        yields.length = 0;
        this.yields = yields;
        this.signal = deferred();
      }
    }
  }

  isEncrypted(): boolean {
    return this.connected && this.encrypted;
  }

  send(frame: Uint8Array): void {
    if (this.done) {
      return;
    }
    try {
      this.socket.send(frame.buffer);
      if (this.options.debug) {
        console.info(`< ${render(frame)}`);
      }
      return;
    } catch (err) {
      // we ignore write errors because client will
      // fail on a read or when the heartbeat timer
      // detects a stale connection
      if (this.options.debug) {
        console.error(`!!! ${render(frame)}: ${err}`);
      }
    }
  }

  close(err?: Error | undefined): Promise<void> {
    return this._closed(err, false);
  }

  closed(): Promise<void | Error> {
    return this.closedNotification;
  }

  // this is to allow a force discard on a connection
  // if the connection fails during the handshake protocol.
  // Firefox for example, will keep connections going,
  // so eventually if it succeeds, the client will have
  // an additional transport running. With this
  discard() {
    this.socket?.close();
  }
}

export function wsUrlParseFn(u: string, encrypted?: boolean): string {
  const ut = /^(.*:\/\/)(.*)/;
  if (!ut.test(u)) {
    // if we have no hint to encrypted and no protocol, assume encrypted
    // else we fix the url from the update to match
    if (typeof encrypted === "boolean") {
      u = `${encrypted === true ? "https" : "http"}://${u}`;
    } else {
      u = `https://${u}`;
    }
  }
  let url = new URL(u);
  const srcProto = url.protocol.toLowerCase();
  if (srcProto === "ws:") {
    encrypted = false;
  }
  if (srcProto === "wss:") {
    encrypted = true;
  }
  if (srcProto !== "https:" && srcProto !== "http") {
    u = u.replace(/^(.*:\/\/)(.*)/gm, "$2");
    url = new URL(`http://${u}`);
  }

  let protocol;
  let port;
  const host = url.hostname;
  const path = url.pathname;
  const search = url.search || "";

  switch (srcProto) {
    case "http:":
    case "ws:":
    case "nats:":
      port = url.port || "80";
      protocol = "ws:";
      break;
    case "https:":
    case "wss:":
    case "tls:":
      port = url.port || "443";
      protocol = "wss:";
      break;
    default:
      port = url.port || encrypted === true ? "443" : "80";
      protocol = encrypted === true ? "wss:" : "ws:";
      break;
  }
  return `${protocol}//${host}:${port}${path}${search}`;
}

export function wsconnect(
  opts: ConnectionOptions | WsConnectionOptions = {},
): Promise<NatsConnection> {
  setTransportFactory({
    defaultPort: 443,
    urlParseFn: wsUrlParseFn,
    factory: (): Transport => {
      if (opts.tls) {
        throw InvalidArgumentError.format(
          "tls",
          "is not configurable on w3c websocket connections",
        );
      }
      return new WsTransport();
    },
  } as TransportFactory);

  return NatsConnectionImpl.connect(opts);
}
