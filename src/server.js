require("dotenv").config();
const express = require("express");
const { connectProducer } = require("./kafka/producer");
const { createBullBoard } = require("@bull-board/api");
const { BullMQAdapter } = require("@bull-board/api/bullMQAdapter");
const { ExpressAdapter } = require("@bull-board/express");
const { Queue } = require("bullmq");
const { redisConnection } = require("./config/redis");

const app = express();
const PORT = process.env.PORT || 3001;

// BullMQ Dashboard
const mailQueue = new Queue("mail-queue", { connection: redisConnection });
const serverAdapter = new ExpressAdapter();
serverAdapter.setBasePath("/admin/queues");

createBullBoard({
  queues: [new BullMQAdapter(mailQueue)],
  serverAdapter,
});

app.use(express.json());

app.get("/health", (req, res) => {
  res.json({
    status: "ok",
    service: "redpanda-connect-backend",
    time: new Date().toISOString(),
  });
});

app.use("/admin/queues", serverAdapter.getRouter());
app.use("/api/publish", require("./routes/publish.routes"));
app.use("/api/users", require("./routes/users.routes"));

app.use((req, res) => {
  res.status(404).json({
    success: false,
    error: "Route not found",
  });
});

app.use((err, req, res, next) => {
  console.error("[Server] Unhandled error:", err);
  res.status(500).json({
    success: false,
    error: "Internal server error",
    detail: err.message,
  });
});

const start = async () => {
  await connectProducer();
  app.listen(PORT, () => {
    console.log("Redpanda Connect Backend");
    console.log(`Server  : http://localhost:${PORT}`);
    console.log(`BullMQ  : http://localhost:${PORT}/admin/queues`);
    console.log("Health  : GET  /health");
    console.log("Publish : POST /api/publish/:topic");
    console.log("Users   : POST /api/users | POST /api/users/register");
    console.log("Confirm : GET  /api/users/confirm?userId=...&token=...");
    console.log("Mail x5 : POST /api/users/send-five-mails");
  });
};

start().catch((err) => {
  console.error("[Server] Khong the khoi dong:", err);
  process.exit(1);
});
