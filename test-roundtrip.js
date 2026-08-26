const WebSocket = require("ws");

const PAIR = "test-pair-1";

const windows = new WebSocket(`ws://localhost:8080?pair=${PAIR}&device=windows-01`);
const android = new WebSocket(`ws://localhost:8080?pair=${PAIR}&device=android-01`);

windows.on("open", () => console.log("[windows] connected"));
android.on("open", () => console.log("[android] connected"));

windows.on("message", (data) => {
  const event = JSON.parse(data.toString());
  console.log(`[windows] RECEIVED: "${event.payload}" (from ${event.originDevice})`);
  console.log(event.originDevice === "android-01" ? "PASS: round trip works" : "FAIL: wrong origin");
  process.exit(0);
});

// Give both sockets a moment to connect, then simulate Android sending
// selected text (as the ACTION_PROCESS_TEXT flow would do).
setTimeout(() => {
  console.log('[android] sending "Hello from Android"');
  android.send(JSON.stringify({ payload: "Hello from Android" }));
}, 500);

setTimeout(() => {
  console.log("FAIL: no message received within timeout");
  process.exit(1);
}, 3000);
