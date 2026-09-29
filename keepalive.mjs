import fs from "node:fs/promises";
import WebSocket from "ws";

const email = process.env.IRCLOUD_EMAIL;
const password = process.env.IRCLOUD_PASSWORD;
if (!email || !password) throw new Error("Missing IRCLOUD_EMAIL or IRCLOUD_PASSWORD");


async function login() {
  const tokenRes = await fetch("https://www.irccloud.com/chat/auth-formtoken", {
    method: "POST",
    headers: { "content-length": "0" }
  });
  const token = await tokenRes.json();
  if (!tokenRes.ok || !token.success || !token.token) {
    throw new Error("Could not obtain IRCCloud auth token");
  }

  const body = new URLSearchParams({
    email, password, token: token.token
  });
  const res = await fetch("https://www.irccloud.com/chat/login", {
    method: "POST",
    headers: {
      "content-type": "application/x-www-form-urlencoded",
      "x-auth-formtoken": token.token
    },
    body
  });
  const data = await res.json();
  if (!res.ok || !data.success || !data.session) {
    throw new Error("IRCCloud login failed");
  }

  return {
    session: data.session,
    wsUrl: `wss://${data.websocket_host || "api.irccloud.com"}${data.websocket_path || "/"}`
  };
}

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function jsonLines(text) {
  const raw = String(text).trim();
  if (!raw) return [];
  try {
    const whole = JSON.parse(raw);
    if (Array.isArray(whole)) return whole;
    if (whole && typeof whole === "object") return [whole];
  } catch {}
  return raw.split(/\r?\n/).flatMap(line => {
    try { return line.trim() ? [JSON.parse(line)] : []; }
    catch { return []; }
  });
}

class IRCCloud {
  constructor(session, wsUrl) {
    this.session = session;
    this.wsUrl = wsUrl;
    this.ws = null;
    this.req = 0;
    this.pending = new Map();
    this.connections = new Map();
    this.buffers = new Map();
    this.backlogDone = false;
    this.processingOob = false;
    this.queued = [];
    this.recentMessages = [];
  }

  async connect() {
    this.ws = new WebSocket(this.wsUrl, {
      headers: {
        Cookie: `session=${this.session}`,
        Origin: "https://www.irccloud.com",
        "User-Agent": "irccloud-keepalive/1.0"
      },
      perMessageDeflate: true
    });

    this.ws.on("message", data => {
      for (const msg of jsonLines(data.toString())) this.handle(msg);
    });

    await new Promise((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error("WebSocket timeout")), 30000);
      this.ws.once("open", () => { clearTimeout(timer); resolve(); });
      this.ws.once("error", e => { clearTimeout(timer); reject(e); });
    });

    await new Promise((resolve, reject) => {
      this.backlogResolve = resolve;
      this.backlogReject = reject;
      setTimeout(() => reject(new Error("Timed out loading IRCCloud state")), 45000);
    });
  }

  handle(msg) {
    if (msg._reqid && this.pending.has(msg._reqid)) {
      const p = this.pending.get(msg._reqid);
      this.pending.delete(msg._reqid);
      if (msg.success === false || msg.error) p.reject(new Error(JSON.stringify(msg)));
      else p.resolve(msg);
      return;
    }

    if (msg.type === "oob_include") {
      this.processingOob = true;
      const base = `https://${new URL(this.wsUrl).host}`;
      const backlogUrl = new URL(msg.url, base).href;
      console.log(`Loading IRCCloud backlog from ${new URL(backlogUrl).pathname}`);
      fetch(backlogUrl, {
        headers: {
          cookie: `session=${this.session}`,
          "accept-encoding": "gzip, deflate"
        }
      })
        .then(async r => {
          if (!r.ok) throw new Error(`Backlog HTTP ${r.status}`);
          return r.text();
        })
        .then(text => {
          const items = jsonLines(text);
          console.log(`Loaded ${items.length} backlog message(s).`);
          for (const item of items) this.update(item);
          this.processingOob = false;
          for (const item of this.queued.splice(0)) this.update(item);
          this.maybeReady();
        })
        .catch(e => this.backlogReject?.(e));
      return;
    }

    if (this.processingOob) {
      this.queued.push(msg);
      return;
    }

    this.update(msg);
  }

  update(msg) {
    if (msg.type === "buffer_msg") {
      this.recentMessages.push({ ...msg, receivedAt: Date.now() });
      if (this.recentMessages.length > 100) this.recentMessages.shift();
    }
    if (msg.type === "set_shard" && msg.cookie) this.session = msg.cookie;
    if (msg.type === "makeserver") this.connections.set(Number(msg.cid), msg);
    if (msg.type === "server_details_changed") {
      const cid = Number(msg.cid);
      this.connections.set(cid, { ...(this.connections.get(cid) || {}), ...msg });
    }
    if (msg.type === "status_changed") {
      const c = this.connections.get(Number(msg.cid));
      if (c) c.status = msg.new_status;
    }
    if (msg.type === "makebuffer") this.buffers.set(Number(msg.bid), msg);
    if (msg.type === "backlog_complete") this.backlogDone = true;
    this.maybeReady();
  }

  maybeReady() {
    if (this.backlogDone && !this.processingOob) this.backlogResolve?.();
  }

  rpc(method, data = {}, timeout = 20000) {
    const id = ++this.req;
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        this.pending.delete(id);
        reject(new Error(`RPC timed out: ${method}`));
      }, timeout);
      this.pending.set(id, {
        resolve: value => { clearTimeout(timer); resolve(value); },
        reject: value => { clearTimeout(timer); reject(value); }
      });
      this.ws.send(JSON.stringify({ ...data, _method: method, _reqid: id }));
    });
  }

  async waitForStatus(cid, wanted, timeout = 60000) {
    if (this.connections.get(cid)?.status === wanted) return;
    const start = Date.now();
    while (Date.now() - start < timeout) {
      if (this.connections.get(cid)?.status === wanted) return;
      await sleep(500);
    }
    throw new Error(`Connection ${cid} did not become ${wanted}`);
  }

  close() {
    try { this.ws?.close(); } catch {}
  }
}

const auth = await login();
const client = new IRCCloud(auth.session, auth.wsUrl);
await client.connect();

const target = [...client.connections.values()].find(c =>
  String(c.name || "").toLowerCase() === "undernet" &&
  !String(c.name || "").includes(":")
);

if (!target) {
  client.close();
  throw new Error(
    `Could not find the Undernet connection represented by "undernet". Available: ${[...client.connections.values()]
      .map(c => `cid=${c.cid} ${c.name || ""} (${c.hostname || ""})`)
      .join(", ") || "none"}`
  );
}

const cid = Number(target.cid);
console.log(`Undernet connection cid=${cid}, status=${target.status}`);

if (!["connected_ready", "connected", "connected_joining"].includes(target.status)) {
  console.log("Reconnecting Undernet...");
  await client.rpc("reconnect", { cid });
  await client.waitForStatus(cid, "connected_ready");
}

console.log("Sending hi to CoachHardy...");
await client.rpc("say", { cid, to: "CoachHardy", msg: "hi" });

await sleep(1000);

const buffer = [...client.buffers.values()].find(b =>
  Number(b.cid) === cid &&
  b.buffer_type === "conversation" &&
  String(b.name || "").toLowerCase() === "coachhardy"
);

if (buffer?.bid) {
  try {
    await client.rpc("archive-buffer", { cid, id: Number(buffer.bid) });
    console.log("Archived PM buffer.");
  } catch (e) {
    console.warn("Message succeeded; buffer archive failed:", e.message);
  }
}

client.close();

await fs.writeFile(
  stateFile,
  JSON.stringify({ lastSuccessMs: Date.now() }, null, 2) + "\n"
);

console.log("IRCCloud keepalive completed successfully.");
