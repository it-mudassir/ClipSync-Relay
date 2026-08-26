# ClipboardSync — Relay Server (v1 proof of concept)

Minimal WebSocket relay. No auth, no encryption, no database — just proves
the transport: whatever one paired device sends, the other receives.

## Run it

```bash
npm install
npm run build
npm start
```

Server listens on `ws://localhost:8080`.

## Connect

Each device connects with two query params:

```
ws://<server>:8080?pair=<shared-pair-id>&device=<device-id>
```

- `pair` — shared id both devices use (stand-in for a real pairing code later)
- `device` — unique id for that device, used for origin tracking / loop prevention

Anything one device sends is JSON-wrapped and forwarded to the *other*
device(s) in the same pair — never echoed back to the sender.

## Test it

```bash
node test-roundtrip.js
```

Simulates an "Android" client sending text and a "Windows" client receiving
it, and confirms the payload + origin arrive correctly.

## What's next (not built yet)

1. **Windows tray agent** (C#/.NET) — connects to this relay, writes
   received payloads to the Windows clipboard via `Clipboard.SetText()`.
2. **Android stub app** — declares an `ACTION_PROCESS_TEXT` intent filter so
   "ClipboardSync" shows up in the text-selection menu; on tap, opens a
   WebSocket connection, sends the selected text, closes.
3. Once Android → Windows works end-to-end: FCM push for Windows → Android
   delivery, real pairing (QR code + key exchange), encryption, history.

## Deploy to Render (free)

1. Push this folder to a new GitHub repo.
2. In Render, **New > Blueprint**, connect the repo — it reads `render.yaml`
   automatically and configures the service.
3. Deploy. You'll get a URL like `clipboard-sync-relay.onrender.com`.
4. Connect over WSS (not WS) once deployed:
   `wss://clipboard-sync-relay.onrender.com?pair=<id>&device=<id>`

Free tier spins down after 15 min idle; first connection after that takes
30-60s to wake up. Fine for on-demand personal use — just don't build
anything that assumes instant delivery after a long idle gap.

Don't deploy this to Vercel — it needs a persistent WebSocket connection,
which serverless functions don't support well.
