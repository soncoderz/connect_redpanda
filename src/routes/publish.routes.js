// ============================================================
// routes/publish.routes.js
// POST /api/publish/:topic
// Body có thể chứa: { eventType, database, collection, ...data }
// Tất cả đều tuỳ chọn - topic từ URL param
// ============================================================
const { Router } = require("express");
const crypto = require("crypto");
const { sendMessage } = require("../kafka/producer");

const router = Router();

/**
 * POST /api/publish/:topic
 *
 * Body JSON (tất cả field đều tuỳ chọn):
 * {
 *   "eventType": "add" | "update" | "delete" | "upsert" | string,
 *   "database":  "my_db",          // Redpanda Connect sẽ đọc field này
 *   "collection": "my_collection", // Redpanda Connect sẽ đọc field này
 *   "id": "...",                   // dùng cho update/delete
 *   ...payload                     // bất kỳ data nào khác
 * }
 *
 * Luồng:
 *   API → Publish vào Redpanda topic
 *       → Redpanda Connect đọc topic
 *       → Lưu vào MongoDB (database/collection theo config yaml hoặc field)
 */
router.post("/:topic", async (req, res) => {
  try {
    const { topic } = req.params;
    const body = req.body || {};

    // Xây dựng event với đầy đủ metadata
    const event = {
      id: body.id || crypto.randomUUID(),
      eventType: body.eventType || "add",       // Mặc định là "add"
      database: body.database || null,          // Tuỳ chọn: Redpanda Connect dùng field này
      collection: body.collection || null,      // Tuỳ chọn
      groupId: body.groupId || null,            // Dùng cho filter theo groupId
      eventTime: new Date().toISOString(),
      payload: body.payload || body,            // Data thực tế
    };

    await sendMessage(topic, event);

    res.status(202).json({
      success: true,
      message: `Event đã được gửi vào topic "${topic}"`,
      topic,
      eventType: event.eventType,
      eventId: event.id,
      note: "Redpanda Connect sẽ xử lý và lưu vào MongoDB tự động",
    });
  } catch (err) {
    console.error("[Publish Route] Lỗi:", err.message);
    res.status(500).json({
      success: false,
      error: "Không thể gửi message vào Redpanda",
      detail: err.message,
    });
  }
});

module.exports = router;
