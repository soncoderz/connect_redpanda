// ============================================================
// routes/appointment.routes.js (updated)
// POST /api/appointments - Backward compatible
// ============================================================
const { Router } = require("express");
const crypto = require("crypto");
const { sendMessage } = require("../kafka/producer");

const router = Router();

const TOPIC = process.env.APPOINTMENT_TOPIC || "appointment-events";

router.post("/", async (req, res) => {
  try {
    const { patientName, doctorName, status, amount } = req.body;

    const event = {
      id: crypto.randomUUID(),
      eventType: "add",                         // Phân loại event
      patientName: patientName || "Unknown",
      doctorName: doctorName || "Unknown",
      status: status || "CREATED",
      amount: amount || 0,
      eventTime: new Date().toISOString(),
    };

    await sendMessage(TOPIC, event);

    res.status(201).json({
      success: true,
      message: `Appointment event đã được gửi vào topic "${TOPIC}"`,
      event,
    });
  } catch (err) {
    console.error("[Appointment Route] Lỗi:", err.message);
    res.status(500).json({
      success: false,
      error: "Không thể gửi event vào Redpanda",
    });
  }
});

module.exports = router;
