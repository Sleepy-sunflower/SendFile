const CHUNK_SIZE = 24 * 1024;
const MAX_BUFFERED = 4 * 1024 * 1024;
const ADJ = ["安静的", "明亮的", "飞快的", "温暖的", "冷静的", "小小的", "大胆的", "晚风里的"];
const NOUN = ["云朵", "橡果", "彗星", "信笺", "磁铁", "灯塔", "蝉鸣", "青柠", "石子", "回声"];

const els = {
  name: document.getElementById("my-name"),
  status: document.getElementById("conn-status"),
  lanUrl: document.getElementById("lan-url"),
  copyUrl: document.getElementById("copy-url"),
  urlHint: document.getElementById("url-hint"),
  extraUrls: document.getElementById("extra-urls"),
  peerList: document.getElementById("peer-list"),
  peerCount: document.getElementById("peer-count"),
  peerEmpty: document.getElementById("peer-empty"),
  dropZone: document.getElementById("drop-zone"),
  dropTitle: document.getElementById("drop-title"),
  dropSub: document.getElementById("drop-sub"),
  pickFiles: document.getElementById("pick-files"),
  fileInput: document.getElementById("file-input"),
  transferList: document.getElementById("transfer-list"),
  transferEmpty: document.getElementById("transfer-empty"),
  clearDone: document.getElementById("clear-done"),
};

/** @type {WebSocket | null} */
let ws = null;
let myId = "";
/** @type {Map<string, {id: string, name: string, device: string, ip: string}>} */
const peers = new Map();
let selectedPeerId = "";
let reconnectTimer = 0;
/** @type {Map<string, any>} */
const transfers = new Map();
/** @type {Map<string, (ok: boolean, detail?: string) => void>} */
const pendingAcks = new Map();

const myName = loadName();
els.name.value = myName;

connect();

els.name.addEventListener("change", () => {
  const name = sanitizeName(els.name.value) || randomName();
  els.name.value = name;
  localStorage.setItem("sendfile-name", name);
  send({ type: "rename", name });
});

els.copyUrl.addEventListener("click", async () => {
  const url = els.lanUrl.textContent || "";
  let ok = false;
  try {
    await navigator.clipboard.writeText(url);
    ok = true;
  } catch {
    const ta = document.createElement("textarea");
    ta.value = url;
    ta.setAttribute("readonly", "");
    ta.style.position = "fixed";
    ta.style.left = "-9999px";
    document.body.append(ta);
    ta.select();
    ok = document.execCommand("copy");
    ta.remove();
  }
  els.copyUrl.textContent = ok ? "已复制" : "复制失败";
  setTimeout(() => (els.copyUrl.textContent = "复制"), 1200);
});

els.pickFiles.addEventListener("click", (e) => {
  e.stopPropagation();
  if (!canSend()) return;
  els.fileInput.click();
});

els.dropZone.addEventListener("click", () => {
  if (!canSend()) return;
  els.fileInput.click();
});

els.fileInput.addEventListener("change", () => {
  const files = [...(els.fileInput.files || [])];
  els.fileInput.value = "";
  sendFilesTo(targetPeerId(), files);
});

["dragenter", "dragover"].forEach((type) => {
  els.dropZone.addEventListener(type, (e) => {
    e.preventDefault();
    if (canSend()) els.dropZone.classList.add("dragover");
  });
});

["dragleave", "drop"].forEach((type) => {
  els.dropZone.addEventListener(type, (e) => {
    e.preventDefault();
    els.dropZone.classList.remove("dragover");
  });
});

els.dropZone.addEventListener("drop", (e) => {
  const files = [...(e.dataTransfer?.files || [])];
  sendFilesTo(targetPeerId(), files);
});

els.clearDone.addEventListener("click", () => {
  for (const [id, item] of transfers) {
    if (item.status === "done" || item.status === "error") {
      if (item.url) URL.revokeObjectURL(item.url);
      transfers.delete(id);
      item.el?.remove();
    }
  }
  syncTransferEmpty();
});

function connect() {
  const proto = location.protocol === "https:" ? "wss" : "ws";
  ws = new WebSocket(`${proto}://${location.host}/ws`);

  ws.addEventListener("open", () => {
    setStatus("已连接", "ok");
    send({
      type: "join",
      name: sanitizeName(els.name.value) || randomName(),
      device: isMobileUA() ? "mobile" : "desktop",
    });
  });

  ws.addEventListener("message", (ev) => {
    if (typeof ev.data !== "string") return;
    const msg = JSON.parse(ev.data);
    if (msg.type === "welcome") {
      myId = msg.id;
      showUrls(msg.urls || []);
      peers.clear();
      for (const peer of msg.peers || []) peers.set(peer.id, peer);
      selectedPeerId = pickDefaultPeer();
      renderPeers();
      return;
    }
    if (msg.type === "peer-joined") {
      peers.set(msg.peer.id, msg.peer);
      if (!selectedPeerId || !peers.has(selectedPeerId)) selectedPeerId = pickDefaultPeer();
      renderPeers();
      return;
    }
    if (msg.type === "peer-updated") {
      peers.set(msg.peer.id, msg.peer);
      renderPeers();
      return;
    }
    if (msg.type === "peer-left") {
      peers.delete(msg.id);
      if (selectedPeerId === msg.id) selectedPeerId = pickDefaultPeer();
      renderPeers();
      return;
    }
    if (msg.type === "file-offer") {
      onOffer(msg);
      return;
    }
    if (msg.type === "file-chunk") {
      onChunk(msg);
      return;
    }
    if (msg.type === "file-complete") {
      completeReceive(msg.transferId);
      return;
    }
    if (msg.type === "file-received") {
      const ack = pendingAcks.get(msg.transferId);
      if (ack) ack(true);
      return;
    }
    if (msg.type === "relay-error") {
      const ack = pendingAcks.get(msg.transferId);
      if (ack) ack(false, msg.reason || "对方未收到");
      failTransfer(msg.transferId, msg.reason || "对方未收到");
      return;
    }
    if (msg.type === "file-cancel") {
      failTransfer(msg.transferId, "对方取消了传输");
    }
  });

  ws.addEventListener("close", () => {
    setStatus("已断开，重连中", "bad");
    peers.clear();
    renderPeers();
    clearTimeout(reconnectTimer);
    reconnectTimer = window.setTimeout(connect, 1200);
  });

  ws.addEventListener("error", () => {
    ws?.close();
  });
}

function send(msg) {
  if (ws && ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify(msg));
}

function canSend() {
  return ws?.readyState === WebSocket.OPEN && peers.size > 0;
}

function targetPeerId() {
  if (selectedPeerId && peers.has(selectedPeerId)) return selectedPeerId;
  return pickDefaultPeer();
}

function renderPeers() {
  els.peerCount.textContent = String(peers.size);
  els.peerEmpty.hidden = peers.size > 0;
  els.peerList.replaceChildren();
  els.dropZone.classList.toggle("ready", canSend());
  els.pickFiles.disabled = !canSend();

  if (peers.size === 0) {
    els.dropTitle.textContent = "等待另一台电脑加入";
    els.dropSub.textContent = "用浏览器打开上方地址后，就可以在这里互传文件";
    return;
  }

  const target = peers.get(targetPeerId());
  els.dropTitle.textContent = "把文件拖到这里";
  els.dropSub.textContent = target
    ? `将发送给 ${target.name}`
    : "选择一个设备后再发送";

  for (const peer of peers.values()) {
    const li = document.createElement("li");
    const selected = peer.id === targetPeerId();
    li.className = "peer-card" + (selected ? " selected" : "");
    li.innerHTML = `
      <div class="avatar">${escapeHtml(initial(peer.name))}</div>
      <div class="peer-meta">
        <strong></strong>
        <span></span>
      </div>
      <div class="pick-mark">${peer.device === "mobile" ? "手机" : "电脑"}</div>
    `;
    li.querySelector("strong").textContent = peer.name;
    li.querySelector("span").textContent = peer.ip || "局域网设备";
    li.addEventListener("click", () => {
      selectedPeerId = peer.id;
      renderPeers();
    });
    li.addEventListener("dragover", (e) => {
      e.preventDefault();
      li.classList.add("dragover");
    });
    li.addEventListener("dragleave", () => li.classList.remove("dragover"));
    li.addEventListener("drop", (e) => {
      e.preventDefault();
      li.classList.remove("dragover");
      selectedPeerId = peer.id;
      renderPeers();
      sendFilesTo(peer.id, [...(e.dataTransfer?.files || [])]);
    });
    els.peerList.append(li);
  }
}

async function sendFilesTo(peerId, files) {
  if (!peerId || !files.length || !canSend()) return;
  for (const file of files) {
    await sendFile(peerId, file);
  }
}

async function sendFile(peerId, file) {
  const transferId = makeId();
  const item = createTransfer({
    id: transferId,
    direction: "out",
    name: file.name || "未命名文件",
    size: file.size,
    peerName: peers.get(peerId)?.name || "对方",
  });

  const started = performance.now();
  try {
    if (!ws || ws.readyState !== WebSocket.OPEN) throw new Error("连接中断");

    let offset = 0;
    let index = 0;
    let sent = 0;
    const first = file.size === 0 ? await readBlob(file) : null;
    const total = first ? first.byteLength : file.size;

    item.size = total;
    item.meta.textContent = `${fmtSize(total)} · 发给 ${item.peerName}`;

    send({
      type: "file-offer",
      to: peerId,
      transferId,
      name: file.name || "未命名文件",
      size: total,
      mime: file.type || "application/octet-stream",
    });

    if (total === 0) {
      send({ type: "file-complete", to: peerId, transferId });
    } else if (first) {
      send({
        type: "file-chunk",
        to: peerId,
        transferId,
        index: 0,
        data: toBase64(first),
      });
      sent = first.byteLength;
      updateProgress(item, sent, total, started);
      send({ type: "file-complete", to: peerId, transferId });
    } else {
      while (offset < total) {
        if (!ws || ws.readyState !== WebSocket.OPEN) throw new Error("连接中断");
        while (ws.bufferedAmount > MAX_BUFFERED) await sleep(16);
        const slice = file.slice(offset, offset + CHUNK_SIZE);
        const buf = await readBlob(slice);
        send({
          type: "file-chunk",
          to: peerId,
          transferId,
          index,
          data: toBase64(buf),
        });
        offset += buf.byteLength;
        sent += buf.byteLength;
        index += 1;
        updateProgress(item, sent, total, started);
        await sleep(0);
      }
      send({ type: "file-complete", to: peerId, transferId });
    }

    await waitForAck(transferId);
    finishSend(item);
  } catch (err) {
    send({ type: "file-cancel", to: peerId, transferId });
    failTransfer(transferId, err.message || "发送失败");
  }
}

function onOffer(msg) {
  const item = createTransfer({
    id: msg.transferId,
    direction: "in",
    name: msg.name,
    size: msg.size,
    mime: msg.mime,
    peerName: peers.get(msg.from)?.name || "对方",
    chunks: [],
    received: 0,
    started: performance.now(),
  });
  item.from = msg.from;
}

function onChunk(msg) {
  const item = transfers.get(msg.transferId);
  if (!item || item.direction !== "in" || !msg.data) return;
  const chunk = fromBase64(msg.data);
  item.chunks.push(chunk);
  item.received += chunk.byteLength;
  updateProgress(item, item.received, item.size, item.started);
}

function completeReceive(transferId) {
  const item = transfers.get(transferId);
  if (!item || item.direction !== "in") return;
  const blob = new Blob(item.chunks, { type: item.mime || "application/octet-stream" });
  item.chunks = [];
  item.url = URL.createObjectURL(blob);
  item.status = "done";
  item.el.classList.add("done");
  item.meta.textContent = `${fmtSize(item.received || item.size)} · 来自 ${item.peerName} · 已完成`;
  const link = document.createElement("a");
  link.className = "download";
  link.href = item.url;
  link.download = item.name;
  link.textContent = "保存";
  item.actions.replaceChildren(link);
  if (blob.type.startsWith("image/")) {
    const img = document.createElement("img");
    img.className = "thumb";
    img.src = item.url;
    img.alt = item.name;
    item.actions.prepend(img);
  }
  if (item.from) {
    send({
      type: "file-received",
      to: item.from,
      transferId,
      received: item.received,
    });
  }
}

function finishSend(item) {
  item.status = "done";
  item.el.classList.add("done");
  item.bar.style.width = "100%";
  item.meta.textContent = `${fmtSize(item.size)} · 发给 ${item.peerName} · 已完成`;
}

function failTransfer(id, reason) {
  const item = transfers.get(id);
  if (!item || item.status === "done") return;
  item.status = "error";
  item.el.classList.add("error");
  item.meta.textContent = reason;
}

function waitForAck(transferId) {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => {
      pendingAcks.delete(transferId);
      reject(new Error("电脑没有收到文件，请确认只开了一个邻传页面后重试"));
    }, 20000);
    pendingAcks.set(transferId, (ok, detail) => {
      clearTimeout(timer);
      pendingAcks.delete(transferId);
      if (ok) resolve();
      else reject(new Error(detail || "对方未收到文件"));
    });
  });
}

function createTransfer({ id, direction, name, size, peerName, mime, chunks, received, started }) {
  const li = document.createElement("li");
  li.className = "transfer";
  li.innerHTML = `
    <div>
      <div class="transfer-name"></div>
      <div class="transfer-meta"></div>
    </div>
    <div class="transfer-actions"></div>
    <div class="progress"><span></span></div>
  `;
  const nameEl = li.querySelector(".transfer-name");
  const meta = li.querySelector(".transfer-meta");
  const bar = li.querySelector(".progress > span");
  const actions = li.querySelector(".transfer-actions");
  nameEl.textContent = `${direction === "out" ? "发送" : "接收"} · ${name}`;
  meta.textContent = `${fmtSize(size)} · ${direction === "out" ? "发给" : "来自"} ${peerName}`;
  els.transferList.prepend(li);
  els.transferEmpty.hidden = true;
  const item = {
    id,
    direction,
    name,
    size,
    mime,
    peerName,
    chunks: chunks || [],
    received: received || 0,
    started: started || performance.now(),
    status: "active",
    el: li,
    meta,
    bar,
    actions,
  };
  transfers.set(id, item);
  return item;
}

function updateProgress(item, current, total, started) {
  const ratio = total ? Math.min(1, current / total) : 1;
  item.bar.style.width = `${Math.round(ratio * 100)}%`;
  const seconds = (performance.now() - started) / 1000;
  item.meta.textContent = `${fmtSize(current)} / ${fmtSize(total)} · ${fmtSpeed(current, seconds)}`;
}

function syncTransferEmpty() {
  els.transferEmpty.hidden = els.transferList.children.length > 0;
}

function showUrls(urls) {
  const preferred =
    urls.find((u) => !u.includes("127.0.0.1") && !u.includes("localhost")) ||
    urls[0] ||
    location.origin;
  els.lanUrl.textContent = preferred;
  const rest = urls.filter((u) => u !== preferred);
  els.extraUrls.hidden = rest.length === 0;
  els.extraUrls.textContent = rest.length ? "其他地址：" + rest.join("  ·  ") : "";
  if (!urls.some((u) => !/127\.0\.0\.1|localhost/.test(u))) {
    els.urlHint.textContent = "未检测到局域网网卡地址，请确认电脑已连接 Wi-Fi / 网线。";
  }
}

function setStatus(text, kind) {
  els.status.textContent = text;
  els.status.className = "status " + kind;
}

function pickDefaultPeer() {
  const list = [...peers.values()];
  if (!list.length) return "";
  const want = isMobileUA() ? "desktop" : "mobile";
  const match = list.filter((p) => p.device === want);
  const pool = match.length ? match : list;
  return pool[pool.length - 1].id;
}

function isMobileUA() {
  return /Mobi|Android|iPhone/i.test(navigator.userAgent);
}

async function readBlob(blob) {
  if (blob.arrayBuffer) return blob.arrayBuffer();
  return new Promise((resolve, reject) => {
    const reader = new FileReader();
    reader.onload = () => resolve(reader.result);
    reader.onerror = () => reject(reader.error);
    reader.readAsArrayBuffer(blob);
  });
}

function toBase64(buffer) {
  const bytes = buffer instanceof Uint8Array ? buffer : new Uint8Array(buffer);
  let binary = "";
  const step = 8192;
  for (let i = 0; i < bytes.length; i += step) {
    const slice = bytes.subarray(i, i + step);
    const parts = new Array(slice.length);
    for (let j = 0; j < slice.length; j++) parts[j] = slice[j];
    binary += String.fromCharCode.apply(null, parts);
  }
  return btoa(binary);
}

function fromBase64(text) {
  const binary = atob(text);
  const out = new Uint8Array(binary.length);
  for (let i = 0; i < binary.length; i++) out[i] = binary.charCodeAt(i);
  return out;
}

function loadName() {
  const saved = localStorage.getItem("sendfile-name");
  return sanitizeName(saved) || randomName();
}

function randomName() {
  return ADJ[Math.floor(Math.random() * ADJ.length)] + NOUN[Math.floor(Math.random() * NOUN.length)];
}

function sanitizeName(name) {
  return String(name || "")
    .replace(/\s+/g, " ")
    .trim()
    .slice(0, 24);
}

function initial(name) {
  return (name || "?").slice(0, 1);
}

function makeId() {
  if (globalThis.crypto?.randomUUID) return crypto.randomUUID();
  const bytes = new Uint8Array(16);
  crypto.getRandomValues(bytes);
  bytes[6] = (bytes[6] & 0x0f) | 0x40;
  bytes[8] = (bytes[8] & 0x3f) | 0x80;
  const hex = [...bytes].map((b) => b.toString(16).padStart(2, "0")).join("");
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
}

function fmtSize(bytes) {
  if (bytes < 1024) return bytes + " B";
  const units = ["KB", "MB", "GB", "TB"];
  let n = bytes / 1024;
  let i = 0;
  while (n >= 1024 && i < units.length - 1) {
    n /= 1024;
    i += 1;
  }
  return n.toFixed(n >= 10 ? 0 : 1) + " " + units[i];
}

function fmtSpeed(bytes, seconds) {
  if (seconds <= 0.05) return "传输中";
  return fmtSize(bytes / seconds) + "/s";
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function escapeHtml(s) {
  return String(s)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;");
}
