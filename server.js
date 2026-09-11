import http from "http";
import fs from "fs";
import path from "path";
import os from "os";
import crypto from "crypto";
import { EventEmitter } from "events";
import { fileURLToPath } from "url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const PUBLIC_DIR = path.resolve(__dirname, "public");
const PORT = Number(process.env.PORT) || 3000;
const HOST = process.env.HOST || "0.0.0.0";
const WS_MAGIC = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";
const MAX_PAYLOAD = 2 * 1024 * 1024;

const MIME = {
  ".html": "text/html; charset=utf-8",
  ".css": "text/css; charset=utf-8",
  ".js": "text/javascript; charset=utf-8",
  ".json": "application/json; charset=utf-8",
  ".svg": "image/svg+xml",
  ".ico": "image/x-icon",
  ".png": "image/png",
  ".woff2": "font/woff2",
};

class MiniWebSocket extends EventEmitter {
  static OPEN = 1;
  static CLOSED = 3;

  constructor(socket) {
    super();
    this.socket = socket;
    this.readyState = MiniWebSocket.OPEN;
    this.buffer = Buffer.alloc(0);
    this.frag = null;
    this.fragOpcode = 0;
    this.isAlive = true;
    socket.on("data", (chunk) => this._onData(chunk));
    socket.on("end", () => this.terminate());
    socket.on("close", () => this._closed());
    socket.on("error", () => this.terminate());
  }

  send(data, options = {}) {
    if (this.readyState !== MiniWebSocket.OPEN) return;
    if (typeof data === "string") {
      this._write(0x1, Buffer.from(data));
      return;
    }
    const buf = Buffer.isBuffer(data) ? data : Buffer.from(data);
    this._write(options.binary === false ? 0x1 : 0x2, buf);
  }

  ping() {
    if (this.readyState === MiniWebSocket.OPEN) this._write(0x9, Buffer.alloc(0));
  }

  terminate() {
    this.readyState = MiniWebSocket.CLOSED;
    this.socket.destroy();
    this._closed();
  }

  _closed() {
    if (this._didClose) return;
    this._didClose = true;
    this.readyState = MiniWebSocket.CLOSED;
    this.emit("close");
  }

  _onData(chunk) {
    this.buffer = Buffer.concat([this.buffer, chunk]);
    try {
      while (this.readyState === MiniWebSocket.OPEN) {
        const frame = this._readFrame();
        if (!frame) break;
        this._handleFrame(frame);
      }
    } catch {
      this.terminate();
    }
  }

  _readFrame() {
    const buf = this.buffer;
    if (buf.length < 2) return null;
    const opcode = buf[0] & 0x0f;
    const fin = (buf[0] & 0x80) !== 0;
    const masked = (buf[1] & 0x80) !== 0;
    let length = buf[1] & 0x7f;
    let offset = 2;
    if (length === 126) {
      if (buf.length < 4) return null;
      length = buf.readUInt16BE(2);
      offset = 4;
    } else if (length === 127) {
      if (buf.length < 10) return null;
      const n = buf.readBigUInt64BE(2);
      if (n > BigInt(MAX_PAYLOAD)) throw new Error("too large");
      length = Number(n);
      offset = 10;
    }
    if (length > MAX_PAYLOAD) throw new Error("too large");
    if (masked) offset += 4;
    if (buf.length < offset + length) return null;
    let payload = buf.subarray(offset, offset + length);
    if (masked) {
      const mask = buf.subarray(offset - 4, offset);
      payload = Buffer.from(payload);
      for (let i = 0; i < payload.length; i++) payload[i] ^= mask[i & 3];
    }
    this.buffer = buf.subarray(offset + length);
    return { fin, opcode, payload };
  }

  _handleFrame(frame) {
    let { fin, opcode, payload } = frame;
    if (opcode === 0x8) {
      this.terminate();
      return;
    }
    if (opcode === 0x9) {
      this._write(0xa, payload);
      return;
    }
    if (opcode === 0xa) {
      this.emit("pong");
      return;
    }
    if (!fin) {
      if (opcode !== 0) this.fragOpcode = opcode;
      this.frag = this.frag ? Buffer.concat([this.frag, payload]) : payload;
      return;
    }
    if (opcode === 0) {
      payload = this.frag ? Buffer.concat([this.frag, payload]) : payload;
      opcode = this.fragOpcode;
      this.frag = null;
    }
    if (opcode === 0x1) this.emit("message", payload, false);
    if (opcode === 0x2) this.emit("message", payload, true);
  }

  _write(opcode, payload) {
    const len = payload.length;
    let header;
    if (len < 126) {
      header = Buffer.alloc(2);
      header[0] = 0x80 | opcode;
      header[1] = len;
    } else if (len < 65536) {
      header = Buffer.alloc(4);
      header[0] = 0x80 | opcode;
      header[1] = 126;
      header.writeUInt16BE(len, 2);
    } else {
      header = Buffer.alloc(10);
      header[0] = 0x80 | opcode;
      header[1] = 127;
      header.writeBigUInt64BE(BigInt(len), 2);
    }
    this.socket.write(Buffer.concat([header, payload]));
  }
}

const clients = new Set();
/** @type {Map<string, { id: string, name: string, device: string, ip: string, ws: MiniWebSocket }>} */
const peers = new Map();

const server = http.createServer((req, res) => {
  const url = new URL(req.url || "/", "http://localhost");
  if (url.pathname === "/api/info") {
    json(res, { hostname: os.hostname(), urls: lanUrls(PORT) });
    return;
  }
  serveStatic(url.pathname, res);
});

server.on("upgrade", (req, socket, head) => {
  const pathname = new URL(req.url || "/", "http://localhost").pathname;
  const key = req.headers["sec-websocket-key"];
  if (pathname !== "/ws" || !key) {
    socket.destroy();
    return;
  }
  const accept = crypto.createHash("sha1").update(key + WS_MAGIC).digest("base64");
  socket.write(
    "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: " +
      accept +
      "\r\n\r\n"
  );
  const ws = new MiniWebSocket(socket);
  if (head && head.length) ws._onData(head);
  onConnection(ws, req);
});

function onConnection(ws, req) {
  const id = crypto.randomUUID();
  const ip = clientIp(req);
  const peer = { id, name: "未命名", device: "desktop", ip, ws };
  clients.add(ws);

  ws.on("pong", () => {
    ws.isAlive = true;
  });

  ws.on("message", (raw, isBinary) => {
    if (isBinary) return;

    let msg;
    try {
      msg = JSON.parse(raw.toString());
    } catch {
      return;
    }

    if (msg.type === "join") {
      peer.name = sanitizeName(msg.name);
      peer.device = msg.device === "mobile" ? "mobile" : "desktop";
      peers.set(id, peer);
      sendJson(ws, {
        type: "welcome",
        id,
        ip,
        urls: lanUrls(PORT),
        hostname: os.hostname(),
        peers: publicPeers(id),
      });
      broadcast({ type: "peer-joined", peer: publicPeer(peer) }, id);
      return;
    }

    if (!peers.has(id)) return;

    if (msg.type === "rename") {
      peer.name = sanitizeName(msg.name);
      broadcast({ type: "peer-updated", peer: publicPeer(peer) }, null);
      return;
    }

    if (
      msg.type === "file-offer" ||
      msg.type === "file-chunk" ||
      msg.type === "file-complete" ||
      msg.type === "file-received" ||
      msg.type === "file-cancel" ||
      msg.type === "file-reject"
    ) {
      relay(id, msg);
    }
  });

  ws.on("close", () => {
    clients.delete(ws);
    if (!peers.has(id)) return;
    peers.delete(id);
    broadcast({ type: "peer-left", id }, null);
  });
}

const heartbeat = setInterval(() => {
  for (const ws of clients) {
    if (ws.isAlive === false) {
      ws.terminate();
      continue;
    }
    ws.isAlive = false;
    ws.ping();
  }
}, 25000);

server.on("close", () => clearInterval(heartbeat));

function relay(fromId, msg) {
  const target = peers.get(msg.to);
  if (!target || target.ws.readyState !== MiniWebSocket.OPEN) {
    const sender = peers.get(fromId);
    if (sender && msg.transferId) {
      sendJson(sender.ws, {
        type: "relay-error",
        transferId: msg.transferId,
        reason: "对方不在线或窗口已关闭",
      });
    }
    return;
  }
  const { to: _to, ...rest } = msg;
  sendJson(target.ws, { ...rest, from: fromId });
}

function broadcast(msg, exceptId) {
  const data = JSON.stringify(msg);
  for (const [id, peer] of peers) {
    if (id === exceptId) continue;
    if (peer.ws.readyState === MiniWebSocket.OPEN) peer.ws.send(data);
  }
}

function publicPeer(peer) {
  return { id: peer.id, name: peer.name, device: peer.device, ip: peer.ip };
}

function publicPeers(exceptId) {
  return [...peers.values()].filter((p) => p.id !== exceptId).map(publicPeer);
}

function sendJson(ws, msg) {
  if (ws.readyState === MiniWebSocket.OPEN) ws.send(JSON.stringify(msg));
}

function sanitizeName(name) {
  const text = String(name || "")
    .replace(/\s+/g, " ")
    .trim()
    .slice(0, 24);
  return text || "未命名";
}

function clientIp(req) {
  const raw = req.socket.remoteAddress || "";
  return raw.replace("::ffff:", "").replace("::1", "127.0.0.1");
}

function lanAddresses() {
  const nets = os.networkInterfaces();
  const result = [];
  for (const list of Object.values(nets)) {
    if (!list) continue;
    for (const net of list) {
      const ipv4 = net.family === 4 || net.family === "IPv4";
      if (ipv4 && !net.internal && !net.address.startsWith("169.254.")) {
        result.push(net.address);
      }
    }
  }
  return result;
}

function lanUrls(port) {
  const hosts = lanAddresses();
  if (hosts.length === 0) hosts.push("127.0.0.1");
  return hosts.map((host) => `http://${host}:${port}`);
}

function json(res, body) {
  const data = JSON.stringify(body);
  res.writeHead(200, {
    "Content-Type": "application/json; charset=utf-8",
    "Content-Length": Buffer.byteLength(data),
    "Cache-Control": "no-store",
  });
  res.end(data);
}

function serveStatic(pathname, res) {
  let rel = decodeURIComponent(pathname);
  if (rel === "/") rel = "/index.html";
  const file = path.resolve(PUBLIC_DIR, "." + rel);
  if (!file.startsWith(PUBLIC_DIR)) {
    res.writeHead(403).end("Forbidden");
    return;
  }
  fs.stat(file, (err, st) => {
    if (err || !st.isFile()) {
      res.writeHead(404).end("Not found");
      return;
    }
    const ext = path.extname(file);
    res.writeHead(200, {
      "Content-Type": MIME[ext] || "application/octet-stream",
      "Content-Length": st.size,
      "Cache-Control": ext === ".html" || ext === ".js" ? "no-store" : "public, max-age=3600",
    });
    fs.createReadStream(file).pipe(res);
  });
}

server.listen(PORT, HOST, () => {
  const urls = lanUrls(PORT);
  console.log("\n  邻传 SendFile 已启动\n");
  console.log("  本机打开:   http://localhost:" + PORT);
  for (const url of urls) {
    console.log("  局域网打开: " + url);
  }
  console.log("\n  另一台电脑用浏览器打开上面的局域网地址即可互传文件。");
  console.log("  若打不开，请在防火墙中放行 TCP " + PORT + " 端口。\n");
});
