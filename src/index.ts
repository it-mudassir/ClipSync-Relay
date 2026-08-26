import { WebSocketServer, WebSocket } from "ws";
import { randomUUID } from "crypto";
import { createServer, IncomingMessage } from "http";

/**
 * ClipboardSync relay server — v1 (proof of concept)
 *
 * Purpose: prove the pipe works. No auth, no encryption, no persistence.
 * Two devices connect with the same `pair` id (a shared secret string for now,
 * stand-in for a real pairing code later) and a unique `device` id.
 * Anything one device sends gets forwarded to the OTHER device(s) in the
 * same pair — never echoed back to the sender, so there's no sync loop.
 *
 * Connect example:
 *   ws://localhost:8080?pair=test-pair-1&device=android-01
 *   ws://localhost:8080?pair=test-pair-1&device=windows-01
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

function getQueryParam(req: IncomingMessage, key: string): string | null {
  const url = new URL(req.url ?? "", `http://${req.headers.host}`);
  return url.searchParams.get(key);
}

// Plain HTTP server so Render's health check (a normal GET request) gets a
// 200 instead of hitting a connection that only understands WebSocket
// upgrades. The WebSocket server attaches to this same server/port.
const httpServer = createServer((req, res) => {
  res.writeHead(200, { "Content-Type": "text/plain" });
  res.end("ClipboardSync relay is running");
});

const wss = new WebSocketServer({ server: httpServer });

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

  console.log(`[connect] device=${deviceId} pair=${pairId} (pair size=${pairs.get(pairId)!.size})`);

  ws.on("message", (raw) => {
    let text: string;
    try {
      // Accept either a raw string payload or a pre-built ClipboardEvent JSON
      const parsed = JSON.parse(raw.toString());
      text = typeof parsed === "string" ? parsed : parsed.payload;
    } catch {
      text = raw.toString();
    }

    const event: ClipboardEvent = {
      id: randomUUID(),
      originDevice: deviceId,
      timestamp: Date.now(),
      type: "text",
      payload: text,
    };

    const peers = pairs.get(pairId) ?? new Set();
    let forwarded = 0;
    for (const peer of peers) {
      if (peer !== ws && peer.readyState === WebSocket.OPEN) {
        peer.send(JSON.stringify(event));
        forwarded++;
      }
    }
    console.log(`[relay] from=${deviceId} pair=${pairId} -> ${forwarded} peer(s): "${text.slice(0, 60)}"`);
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

httpServer.listen(PORT, () => {
  console.log(`ClipboardSync relay listening on ws://localhost:${PORT}`);
});
