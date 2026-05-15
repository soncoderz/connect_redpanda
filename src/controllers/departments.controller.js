const crypto = require("crypto");
const { sendMessage } = require("../kafka/producer");

const TOPIC = "department-events";

const createDepartment = async (req, res) => {
  try {
    const { name, floor, building, headDoctorId } = req.body;

    const event = {
      id: crypto.randomUUID(),
      eventType: "add",
      name: name || "Unknown Department",
      floor: floor || 1,
      building: building || "A",
      headDoctorId: headDoctorId || null,
      doctorIds: [],
      eventTime: new Date().toISOString(),
    };

    await sendMessage(TOPIC, event);

    res.status(201).json({
      success: true,
      message: `Department "add" event đã gửi vào topic "${TOPIC}"`,
      event,
    });
  } catch (err) {
    console.error("[Departments Controller] Lỗi:", err.message);
    res.status(500).json({ success: false, error: err.message });
  }
};

const updateDepartment = async (req, res) => {
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
      message: `Department "update" event đã gửi vào topic "${TOPIC}"`,
      event,
    });
  } catch (err) {
    console.error("[Departments Controller] Lỗi:", err.message);
    res.status(500).json({ success: false, error: err.message });
  }
};

const deleteDepartment = async (req, res) => {
  try {
    const event = {
      id: req.params.id,
      eventType: "delete",
      eventTime: new Date().toISOString(),
    };

    await sendMessage(TOPIC, event);

    res.json({
      success: true,
      message: `Department "delete" event đã gửi vào topic "${TOPIC}"`,
      event,
    });
  } catch (err) {
    console.error("[Departments Controller] Lỗi:", err.message);
    res.status(500).json({ success: false, error: err.message });
  }
};

module.exports = {
  createDepartment,
  updateDepartment,
  deleteDepartment,
};
