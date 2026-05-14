// ============================================================
// routes/crud.routes.js - Factory cho CRUD routes
// Tạo router có đầy đủ add/update/delete/upsert
// eventType tương ứng với pipeline 08-crud-operations.yaml
// ============================================================
const { Router } = require("express");
const crypto = require("crypto");
const { sendMessage } = require("../kafka/producer");

/**
 * createCrudRouter(defaultTopic)
 * Trả về Router với 4 endpoints:
 *   POST   /          → eventType: "add"
 *   PUT    /:id       → eventType: "update"
 *   DELETE /:id       → eventType: "delete"
 *   PATCH  /:id       → eventType: "upsert"
 */
const createCrudRouter = (defaultTopic) => {
  const router = Router();

  // ── Hàm gửi message vào topic ────────────────────────────
  const publish = async (topic, eventType, id, data) => {
    const event = {
      id,
      eventType,                              // "add" | "update" | "delete" | "upsert"
      eventTime: new Date().toISOString(),
      ...data,                                // Spread toàn bộ data vào event
    };
    await sendMessage(topic, event);
    return event;
  };

  // ── POST / → eventType: "add" (Insert mới) ───────────────
  router.post("/", async (req, res) => {
    try {
      const topic = req.body.topic || defaultTopic;
      const event = await publish(topic, "add", crypto.randomUUID(), req.body);
      res.status(201).json({
        success: true,
        message: `Đã gửi event "add" vào topic "${topic}"`,
        eventType: "add",
        eventId: event.id,
      });
    } catch (err) {
      res.status(500).json({ success: false, error: err.message });
    }
  });

  // ── PUT /:id → eventType: "update" (Cập nhật) ────────────
  router.put("/:id", async (req, res) => {
    try {
      const topic = req.body.topic || defaultTopic;
      const event = await publish(topic, "update", req.params.id, req.body);
      res.json({
        success: true,
        message: `Đã gửi event "update" vào topic "${topic}"`,
        eventType: "update",
        eventId: event.id,
      });
    } catch (err) {
      res.status(500).json({ success: false, error: err.message });
    }
  });

  // ── DELETE /:id → eventType: "delete" (Xóa) ──────────────
  router.delete("/:id", async (req, res) => {
    try {
      const topic = req.body?.topic || defaultTopic;
      const event = await publish(topic, "delete", req.params.id, {});
      res.json({
        success: true,
        message: `Đã gửi event "delete" vào topic "${topic}"`,
        eventType: "delete",
        eventId: event.id,
      });
    } catch (err) {
      res.status(500).json({ success: false, error: err.message });
    }
  });

  // ── PATCH /:id → eventType: "upsert" (Tạo mới hoặc update) ─
  router.patch("/:id", async (req, res) => {
    try {
      const topic = req.body.topic || defaultTopic;
      const event = await publish(topic, "upsert", req.params.id, req.body);
      res.json({
        success: true,
        message: `Đã gửi event "upsert" vào topic "${topic}"`,
        eventType: "upsert",
        eventId: event.id,
      });
    } catch (err) {
      res.status(500).json({ success: false, error: err.message });
    }
  });

  return router;
};

module.exports = createCrudRouter;
