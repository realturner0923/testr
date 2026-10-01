import http from "node:http";
import { WebSocketServer } from "ws";
import { createClient } from "redis";
import "dotenv/config";

const PORT = process.env.PORT || 3003;

const REDIS_URL = process.env.REDIS_URL;

if (!REDIS_URL) {
  throw new Error("REDIS_URL is not configured");
}


const server = http.createServer((req, res) => {
  if (req.url === "/health") {
    res.writeHead(200, {
      "Content-Type": "application/json"
    });

    res.end(
      JSON.stringify({
        service: "websocket",
        status: "ok"
      })
    );

    return;
  }

  res.writeHead(404);
  res.end();
});

const websocketServer = new WebSocketServer({
  server
});

const clients = new Set();

websocketServer.on("connection", (socket) => {
  clients.add(socket);

  console.log(
    `WebSocket client connected. Clients: ${clients.size}`
  );

  socket.send(
    JSON.stringify({
      type: "connected"
    })
  );

  socket.on("close", () => {
    clients.delete(socket);

    console.log(
      `WebSocket client disconnected. Clients: ${clients.size}`
    );
  });
});

const redisSubscriber = createClient({
  url: REDIS_URL
});

redisSubscriber.on("error", (error) => {
  console.error(
    "Redis subscriber error:",
    error
  );
});

await redisSubscriber.connect();

console.log("WebSocket service connected to Redis");

await redisSubscriber.subscribe(
  "notifications",
  (message) => {
    console.log(
      "Notification received from Redis"
    );

    for (const client of clients) {
      if (client.readyState === 1) {
        client.send(message);
      }
    }
  }
);

server.listen(PORT, "0.0.0.0", () => {
  console.log(
    `WebSocket service running on ws://0.0.0.0:${PORT}`
  );
});

