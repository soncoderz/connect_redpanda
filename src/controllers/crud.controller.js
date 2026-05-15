const crypto = require("crypto");
const { sendMessage } = require("../kafka/producer");

const publish = async (topic, eventType, id, data) => {
  const event = {
    id,
    eventType,
    eventTime: new Date().toISOString(),
    ...data,
  };

  await sendMessage(topic, event);
  return event;
};

const createCrudController = (defaultTopic) => ({
  create: async (req, res) => {
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
  },

  update: async (req, res) => {
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
  },

  remove: async (req, res) => {
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
  },

  upsert: async (req, res) => {
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
  },
});

module.exports = createCrudController;
