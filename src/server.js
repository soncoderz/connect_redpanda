// ============================================================
// server.js - Entry point hoàn chỉnh
// Hỗ trợ dynamic topic, database, collection qua API
// ============================================================
require("dotenv").config();
const express = require("express");
const { connectProducer } = require("./kafka/producer");

const app = express();
const PORT = process.env.PORT || 3000;

app.use(express.json());

// ── Health check ──────────────────────────────────────────
app.get("/health", (req, res) => {
  res.json({
    status: "ok",
    service: "redpanda-connect-backend",
    time: new Date().toISOString(),
  });
});

// ── Routes ────────────────────────────────────────────────
// Dynamic: POST /api/publish/:topic
app.use("/api/publish", require("./routes/publish.routes"));

// CRUD shorthand routes
app.use("/api/users", require("./routes/crud.routes")("users"));
app.use("/api/orders", require("./routes/crud.routes")("orders"));
app.use("/api/payments", require("./routes/crud.routes")("payments"));

// Legacy appointment route
app.use("/api/appointments", require("./routes/appointment.routes"));

// ── Start ─────────────────────────────────────────────────
const start = async () => {
  await connectProducer();
  app.listen(PORT, () => {
    console.log("╔══════════════════════════════════════════════════╗");
    console.log("║         Redpanda Connect Backend                 ║");
    console.log("╠══════════════════════════════════════════════════╣");
    console.log(`║  Server   : http://localhost:${PORT}                 ║`);
    console.log(`║  Health   : GET  /health                         ║`);
    console.log(`║  Publish  : POST /api/publish/:topic             ║`);
    console.log(`║  Users    : POST /api/users  (add/update/delete) ║`);
    console.log(`║  Orders   : POST /api/orders                     ║`);
    console.log(`║  Payments : POST /api/payments                   ║`);
    console.log("╚══════════════════════════════════════════════════╝");
  });
};

start().catch((err) => {
  console.error("[Server] Không thể khởi động:", err);
  process.exit(1);
});
