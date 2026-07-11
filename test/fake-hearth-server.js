const http = require("http");

const host = "127.0.0.1";
const port = Number(process.env.FAKE_HEARTH_PORT || 3210);
const expectedToken = String(process.env.HEARTH_INTERNAL_TOKEN || "").trim();

http.createServer((request, response) => {
  if (request.method !== "POST" || request.url !== "/internal/channel/companion") return response.writeHead(404).end();
  if (!expectedToken || request.headers.authorization !== `Bearer ${expectedToken}`) return response.writeHead(401).end();
  let raw = "";
  request.on("data", (chunk) => { raw += chunk; });
  request.on("end", () => {
    try {
      const body = JSON.parse(raw);
      if (body?.channel !== "weixin" || !body?.message_id || !body?.text || "context_token" in body) throw new Error("invalid envelope");
      console.log(JSON.stringify({ type: "fake_hearth", status: "accepted", message_id: String(body.message_id) }));
      response.writeHead(200, { "Content-Type": "application/json" });
      response.end(JSON.stringify({ request_id: "fake-local", action: "send", reply_text: "hearth stub ok", segments: ["hearth stub ok"], gateway_processed_at: new Date().toISOString() }));
    } catch { response.writeHead(400).end(); }
  });
}).listen(port, host, () => console.log(JSON.stringify({ type: "fake_hearth", status: "ready", host, port })));
