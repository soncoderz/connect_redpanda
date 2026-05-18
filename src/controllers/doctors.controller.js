const crypto = require("crypto");
const { sendMessage } = require("../kafka/producer");

const TOPIC = "doctors-events";

const createDoctor = async (req, res) => {
  try {
    const { name, specialty, departmentId, departmentName, phone } = req.body;

    const event = {
      id: crypto.randomUUID(),
      eventType: "add",
      name: name || "Unknown",
      specialty: specialty || "General",
      departmentId: departmentId || null,
      departmentName: departmentName || null,
      phone: phone || "",
      eventTime: new Date().toISOString(),
    };

    await sendMessage(TOPIC, event);

    res.status(201).json({
      success: true,
      message: `Doctor "add" event đã gửi vào topic "${TOPIC}"`,
      event,
    });
  } catch (err) {
    console.error("[Doctors Controller] Lỗi:", err.message);
    res.status(500).json({ success: false, error: err.message });
  }
};

const updateDoctor = async (req, res) => {
  try {
    const event = {
      id: req.params.id,
      eventType: "update",
      ...req.body,
      eventTime: new Date().toISOString(),
    };

    await sendMessage(TOPIC, event);

    res.json({
      success: true,
      message: `Doctor "update" event đã gửi vào topic "${TOPIC}"`,
      event,
    });
  } catch (err) {
    console.error("[Doctors Controller] Lỗi:", err.message);
    res.status(500).json({ success: false, error: err.message });
  }
};

const deleteDoctor = async (req, res) => {
  try {
    const event = {
      id: req.params.id,
      eventType: "delete",
      departmentId: req.body.departmentId || null,
      eventTime: new Date().toISOString(),
    };

    await sendMessage(TOPIC, event);

    res.json({
      success: true,
      message: `Doctor "delete" event đã gửi vào topic "${TOPIC}"`,
      event,
    });
  } catch (err) {
    console.error("[Doctors Controller] Lỗi:", err.message);
    res.status(500).json({ success: false, error: err.message });
  }
};

module.exports = {
  createDoctor,
  updateDoctor,
  deleteDoctor,
};
