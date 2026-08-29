import { WebSocketServer, WebSocket } from "ws";
import { randomUUID } from "crypto";
import { createServer, IncomingMessage, ServerResponse } from "http";
import * as admin from "firebase-admin";

/**
 * ClipboardSync relay server — v4 (adds persistent storage via Firestore)
 *
 * No auth, no encryption yet. Two devices connect with the same `pair` id
 * and a unique `device` id. Anything one device sends gets forwarded to
 * the OTHER device(s) in the same pair via WebSocket if they're currently
 * connected, with an FCM push fallback for Android when it isn't.
 *
 * Background push delivery to Android turned out to be unreliable on some
 * devices (OS-level background execution throttling, not fixable from the
 * app side) — so the relay also remembers the last event per pair and
 * exposes it via GET /latest. The Android app pulls this every time it
 * opens, which is a deterministic fallback: it doesn't matter whether a
 * push succeeded, failed, or never arrived, opening the app always
 * correctly syncs to whatever's actually on Windows right now.
 *
 * Registered FCM tokens and the last-event-per-pair state are now backed
 * by Firestore (same Firebase project/credentials as FCM — no separate
 * setup beyond enabling Firestore in the Firebase Console), with an
 * in-memory cache in front for speed. Previously these lived only in
 * process memory and got wiped on every Render restart/sleep, silently
 * breaking push delivery and pull-on-open until the Android app was
 * reopened once. If Firestore isn't configured, behavior degrades
 * gracefully to the old in-memory-only behavior rather than breaking.
 *
 * Connect example:
 *   ws://localhost:8080?pair=test-pair-1&device=android-01
 *   ws://localhost:8080?pair=test-pair-1&device=windows-01
 *
 * HTTP endpoints:
 *   POST /register  { pairId, deviceId, fcmToken }           — Android push token registration
 *   POST /send       { pairId, deviceId, payload }           — one-shot send without a WebSocket
 *                                                                (used by the Android accessibility
 *                                                                 service, which runs outside Flutter)
 *   GET  /latest?pairId=...                                  — last known clipboard state for a pair
 */

const PORT = process.env.PORT ? Number(process.env.PORT) : 8080;

interface ClipboardEvent {
  id: string;
  originDevice: string;
  timestamp: number;
  type: "text";
  payload: string;
}

// pairId -> set of connected sockets in that pair
const pairs = new Map<string, Set<WebSocket>>();
// socket -> { pairId, deviceId } for cleanup on close
const meta = new Map<WebSocket, { pairId: string; deviceId: string }>();
// pairId -> most recently registered Android FCM token. Single-Android-
// device-per-pair is the assumption for v1 (matches personal use — one
// phone, one PC); a second Android device registering to the same pair
// would just overwrite the token, last-registered wins.
const fcmTokensByPair = new Map<string, string>();
// pairId -> most recent clipboard event seen, from either device. This is
// what makes pull-on-open possible — it's the source of truth regardless
// of whether any push notification around it succeeded.
const lastEventByPair = new Map<string, ClipboardEvent>();

// FCM is optional — the relay runs fine without it (WebSocket delivery
// for Android -> Windows and any live-connected Windows -> Android still
// works), it just can't do the background push fallback until configured.
// The same Firebase Admin credentials also back Firestore persistence
// below — one env var covers both, no separate account needed.
let fcmReady = false;
let firestoreDb: admin.firestore.Firestore | null = null;
const serviceAccountB64 = process.env.FCM_SERVICE_ACCOUNT_BASE64;
if (serviceAccountB64) {
  try {
    const json = Buffer.from(serviceAccountB64, "base64").toString("utf-8");
    const serviceAccount = JSON.parse(json);
    admin.initializeApp({ credential: admin.credential.cert(serviceAccount) });
    fcmReady = true;
    firestoreDb = admin.firestore();
    console.log("[fcm] initialized from FCM_SERVICE_ACCOUNT_BASE64");
  } catch (err) {
    console.error("[fcm] failed to initialize — check FCM_SERVICE_ACCOUNT_BASE64 is valid base64-encoded JSON:", err);
  }
} else {
  console.log("[fcm] FCM_SERVICE_ACCOUNT_BASE64 not set — push fallback disabled, WebSocket-only for now");
}

/**
 * Persistence for the two things that used to live only in memory (and
 * so got wiped every Render restart/sleep): each pair's registered FCM
 * token, and the last clipboard event seen (the pull-on-open source of
 * truth). The in-memory Maps stay as a fast cache for the common case —
 * these functions only hit Firestore on a cache miss (i.e. right after a
 * restart) or to persist a write. If Firestore isn't configured, behavior
 * falls back to exactly the old in-memory-only behavior — nothing breaks,
 * it just loses durability across restarts again.
 */
async function getFcmToken(pairId: string): Promise<string | undefined> {
  const cached = fcmTokensByPair.get(pairId);
  if (cached) return cached;
  if (!firestoreDb) return undefined;
  try {
    const doc = await firestoreDb.collection("fcmTokens").doc(pairId).get();
    const token = doc.exists ? (doc.data()?.token as string | undefined) : undefined;
    if (token) fcmTokensByPair.set(pairId, token);
    return token;
  } catch (err) {
    console.error(`[firestore] getFcmToken failed for pair=${pairId}:`, err);
    return undefined;
  }
}

async function setFcmToken(pairId: string, token: string): Promise<void> {
  fcmTokensByPair.set(pairId, token);
  if (!firestoreDb) return;
  try {
    await firestoreDb.collection("fcmTokens").doc(pairId).set({ token, updatedAt: Date.now() });
  } catch (err) {
    console.error(`[firestore] setFcmToken failed for pair=${pairId}:`, err);
  }
}

async function getLastEvent(pairId: string): Promise<ClipboardEvent | undefined> {
  const cached = lastEventByPair.get(pairId);
  if (cached) return cached;
  if (!firestoreDb) return undefined;
  try {
    const doc = await firestoreDb.collection("lastEvents").doc(pairId).get();
    if (!doc.exists) return undefined;
    const event = doc.data() as ClipboardEvent;
    lastEventByPair.set(pairId, event);
    return event;
  } catch (err) {
    console.error(`[firestore] getLastEvent failed for pair=${pairId}:`, err);
    return undefined;
  }
}

function setLastEvent(pairId: string, event: ClipboardEvent): void {
  lastEventByPair.set(pairId, event);
  if (!firestoreDb) return;
  // Fire-and-forget — the caller (relayClipboardEvent) already delivered
  // the message via WS/FCM by this point; persisting it is best-effort
  // durability for pull-on-open, not on the critical delivery path.
  firestoreDb
    .collection("lastEvents")
    .doc(pairId)
    .set(event)
    .catch((err) => console.error(`[firestore] setLastEvent failed for pair=${pairId}:`, err));
}

async function sendFcmFallback(pairId: string, event: ClipboardEvent) {
  if (!fcmReady) {
    console.log(`[fcm] not configured (FCM_SERVICE_ACCOUNT_BASE64 missing) — can't push for pair=${pairId}`);
    return;
  }
  const token = await getFcmToken(pairId);
  if (!token) {
    console.log(`[fcm] no registered token for pair=${pairId}, can't push`);
    return;
  }
  try {
    await admin.messaging().send({
      token,
      data: {
        payload: event.payload,
        originDevice: event.originDevice,
        timestamp: String(event.timestamp),
      },
      android: { priority: "high" },
    });
    console.log(`[fcm] pushed to pair=${pairId}: "${event.payload.slice(0, 60)}"`);
  } catch (err) {
    console.error(`[fcm] push failed for pair=${pairId}:`, err);
  }
}

function getQueryParam(req: IncomingMessage, key: string): string | null {
  const url = new URL(req.url ?? "", `http://${req.headers.host}`);
  return url.searchParams.get(key);
}

function readBody(req: IncomingMessage): Promise<string> {
  return new Promise((resolve, reject) => {
    let body = "";
    req.on("data", (chunk) => (body += chunk));
    req.on("end", () => resolve(body));
    req.on("error", reject);
  });
}

/**
 * The core relay logic, shared between the WebSocket message handler and
 * the HTTP POST /send endpoint (used by the Android accessibility service,
 * which sends outside of Flutter/the WebSocket connection entirely).
 * Forwards to any live WS peers, falls back to FCM if there are none, and
 * always updates lastEventByPair for the pull-on-open mechanism.
 */
function relayClipboardEvent(pairId: string, originDeviceId: string, text: string, excludeSocket?: WebSocket) {
  const event: ClipboardEvent = {
    id: randomUUID(),
    originDevice: originDeviceId,
    timestamp: Date.now(),
    type: "text",
    payload: text,
  };

  setLastEvent(pairId, event);

  const peers = pairs.get(pairId) ?? new Set();
  const forwardedTo: string[] = [];
  for (const peer of peers) {
    if (peer !== excludeSocket && peer.readyState === WebSocket.OPEN) {
      peer.send(JSON.stringify(event));
      forwardedTo.push(meta.get(peer)?.deviceId ?? "unknown");
    }
  }
  console.log(
    `[relay] from=${originDeviceId} pair=${pairId} -> [${forwardedTo.join(", ") || "nobody"}]: "${text.slice(0, 60)}"`
  );

  if (forwardedTo.length === 0) {
    void sendFcmFallback(pairId, event);
  }

  return event;
}

// Plain HTTP server for Render's health check (a normal GET) and for
// Android's token registration (POST /register). The WebSocket server
// attaches to this same server/port.
const httpServer = createServer(async (req, res: ServerResponse) => {
  const url = new URL(req.url ?? "", `http://${req.headers.host}`);

  if (req.method === "POST" && url.pathname === "/register") {
    try {
      const body = JSON.parse(await readBody(req));
      const { pairId, deviceId, fcmToken } = body;
      if (!pairId || !fcmToken) {
        res.writeHead(400, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ error: "pairId and fcmToken are required" }));
        return;
      }
      await setFcmToken(pairId, fcmToken);
      console.log(`[register] pair=${pairId} device=${deviceId ?? "unknown"} — fcm token stored`);
      res.writeHead(200, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ ok: true }));
    } catch (err) {
      res.writeHead(400, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ error: "invalid JSON body" }));
    }
    return;
  }

  if (req.method === "POST" && url.pathname === "/send") {
    try {
      const body = JSON.parse(await readBody(req));
      const { pairId, deviceId, payload } = body;
      if (!pairId || !deviceId || typeof payload !== "string") {
        res.writeHead(400, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ error: "pairId, deviceId, and payload (string) are required" }));
        return;
      }
      relayClipboardEvent(pairId, deviceId, payload);
      res.writeHead(200, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ ok: true }));
    } catch (err) {
      res.writeHead(400, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ error: "invalid JSON body" }));
    }
    return;
  }

  if (req.method === "GET" && url.pathname === "/latest") {
    const pairId = url.searchParams.get("pairId");
    if (!pairId) {
      res.writeHead(400, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ error: "pairId query param is required" }));
      return;
    }
    const event = await getLastEvent(pairId);
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(JSON.stringify({ event: event ?? null }));
    return;
  }

  res.writeHead(200, { "Content-Type": "text/plain" });
  res.end("ClipboardSync relay is running");
});

const wss = new WebSocketServer({ server: httpServer });

// How often we ping every open socket, and how it's used to detect dead
// connections: each socket starts "alive", a pong response resets it back
// to alive, and any socket still marked dead at the NEXT interval tick
// (i.e. didn't respond to the previous ping) gets forcibly terminated and
// removed from its pair. This is what was missing before — without it, a
// client that disconnects uncleanly (process killed, network drop, no
// clean close frame) stays in the pair indefinitely and can silently
// swallow messages meant for the real, still-connected device.
const HEARTBEAT_INTERVAL_MS = 15000;
const aliveFlags = new WeakMap<WebSocket, boolean>();

wss.on("connection", (ws: WebSocket, req: IncomingMessage) => {
  const pairId = getQueryParam(req, "pair");
  const deviceId = getQueryParam(req, "device");

  if (!pairId || !deviceId) {
    ws.close(4000, "missing pair or device query param");
    return;
  }

  if (!pairs.has(pairId)) pairs.set(pairId, new Set());
  pairs.get(pairId)!.add(ws);
  meta.set(ws, { pairId, deviceId });
  aliveFlags.set(ws, true);

  console.log(`[connect] device=${deviceId} pair=${pairId} (pair size=${pairs.get(pairId)!.size})`);

  ws.on("pong", () => aliveFlags.set(ws, true));

  ws.on("message", (raw) => {
    let text: string;
    try {
      // Accept either a raw string payload or a pre-built ClipboardEvent JSON
      const parsed = JSON.parse(raw.toString());
      text = typeof parsed === "string" ? parsed : parsed.payload;
    } catch {
      text = raw.toString();
    }

    relayClipboardEvent(pairId, deviceId, text, ws);
  });

  ws.on("close", () => {
    const m = meta.get(ws);
    if (m) {
      pairs.get(m.pairId)?.delete(ws);
      if (pairs.get(m.pairId)?.size === 0) pairs.delete(m.pairId);
      console.log(`[disconnect] device=${m.deviceId} pair=${m.pairId}`);
    }
    meta.delete(ws);
  });

  ws.on("error", (err) => console.error(`[error] device=${deviceId}:`, err.message));
});

// Heartbeat sweep: ping everyone, terminate anyone who didn't pong since
// the last sweep. `terminate()` forces the close event to fire, which
// runs our normal cleanup (removes from `pairs`, logs disconnect).
setInterval(() => {
  for (const ws of wss.clients) {
    if (aliveFlags.get(ws) === false) {
      const m = meta.get(ws);
      console.log(`[heartbeat] terminating unresponsive device=${m?.deviceId ?? "unknown"} pair=${m?.pairId ?? "?"}`);
      ws.terminate();
      continue;
    }
    aliveFlags.set(ws, false);
    ws.ping();
  }
}, HEARTBEAT_INTERVAL_MS);

httpServer.listen(PORT, () => {
  console.log(`ClipboardSync relay listening on ws://localhost:${PORT}`);
});
