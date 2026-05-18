const crypto = require("crypto");
const { sendMessage } = require("../kafka/producer");

const publishToTopic = async (req, res) => {
  try {
    const { topic } = req.params;
    const body = req.body || {};

    const event = {
      ...body,
      id: body.id || crypto.randomUUID(),
      eventType: body.eventType || "add",
      eventTime: new Date().toISOString(),
    };

    await sendMessage(topic, event);

    res.status(202).json({
      success: true,
      message: `Event da duoc gui vao topic "${topic}"`,
      topic,
      eventId: event.id,
      event,
      note: "Redpanda Connect se doc topic, xu ly pipeline va luu vao MongoDB.",
    });
  } catch (err) {
    console.error("[Publish Controller] Loi:", err.message);
    res.status(500).json({
      success: false,
      error: "Khong the gui message vao Redpanda",
      detail: err.message,
    });
  }
};

module.exports = { publishToTopic };
