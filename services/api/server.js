import express from "express";
import "dotenv/config";

const app = express();
const PORT = process.env.PORT || 3000;

app.use(express.json());

app.get("/health", (req, res) => {
  res.json({
    service: "api",
    status: "ok"
  });
});

app.get("/api/notifications", (req, res) => {
  res.json({
    notifications: []
  });
});

app.listen(PORT, () => {
  console.log(`API service running on http://localhost:${PORT}`);
});
