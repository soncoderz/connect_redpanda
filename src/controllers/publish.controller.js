const crypto = require("crypto");
const { sendMessage } = require("../kafka/producer");

const publishToTopic = async (req, res) => {
  try {
    const { topic } = req.params;
    const body = req.body || {};

    const event = {
      id: body.id || crypto.randomUUID(),
      eventType: body.eventType || "add",
      database: body.database || null,
      collection: body.collection || null,
      groupId: body.groupId || null,
      eventTime: new Date().toISOString(),
      payload: body.payload || body,
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
    console.error("[Publish Controller] Lỗi:", err.message);
    res.status(500).json({
      success: false,
      error: "Không thể gửi message vào Redpanda",
      detail: err.message,
    });
  }
};

module.exports = { publishToTopic };
