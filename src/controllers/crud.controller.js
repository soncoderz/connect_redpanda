const crypto = require("crypto");
const { sendMessage } = require("../kafka/producer");

// ── Khai báo tường minh 3 topic ──────────────────────────
const TOPICS = {
  users:    "users",
  orders:   "orders",
  payments: "payments",
};

// ── Hàm helper dùng chung ────────────────────────────────
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

// ============================================================
// USERS
// ============================================================
const createUser = async (req, res) => {
  try {
    const event = await publish(TOPICS.users, "add", crypto.randomUUID(), req.body);
    res.status(201).json({ success: true, message: `Đã gửi "add" vào topic "${TOPICS.users}"`, eventId: event.id });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
};

const updateUser = async (req, res) => {
  try {
    const event = await publish(TOPICS.users, "update", req.params.id, req.body);
    res.json({ success: true, message: `Đã gửi "update" vào topic "${TOPICS.users}"`, eventId: event.id });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
};

const deleteUser = async (req, res) => {
  try {
    const event = await publish(TOPICS.users, "delete", req.params.id, {});
    res.json({ success: true, message: `Đã gửi "delete" vào topic "${TOPICS.users}"`, eventId: event.id });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
};

// ============================================================
// ORDERS
// ============================================================
const createOrder = async (req, res) => {
  try {
    const event = await publish(TOPICS.orders, "add", crypto.randomUUID(), req.body);
    res.status(201).json({ success: true, message: `Đã gửi "add" vào topic "${TOPICS.orders}"`, eventId: event.id });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
};

const updateOrder = async (req, res) => {
  try {
    const event = await publish(TOPICS.orders, "update", req.params.id, req.body);
    res.json({ success: true, message: `Đã gửi "update" vào topic "${TOPICS.orders}"`, eventId: event.id });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
};

const deleteOrder = async (req, res) => {
  try {
    const event = await publish(TOPICS.orders, "delete", req.params.id, {});
    res.json({ success: true, message: `Đã gửi "delete" vào topic "${TOPICS.orders}"`, eventId: event.id });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
};

// ============================================================
// PAYMENTS
// ============================================================
const createPayment = async (req, res) => {
  try {
    const event = await publish(TOPICS.payments, "add", crypto.randomUUID(), req.body);
    res.status(201).json({ success: true, message: `Đã gửi "add" vào topic "${TOPICS.payments}"`, eventId: event.id });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
};

const updatePayment = async (req, res) => {
  try {
    const event = await publish(TOPICS.payments, "update", req.params.id, req.body);
    res.json({ success: true, message: `Đã gửi "update" vào topic "${TOPICS.payments}"`, eventId: event.id });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
};

const deletePayment = async (req, res) => {
  try {
    const event = await publish(TOPICS.payments, "delete", req.params.id, {});
    res.json({ success: true, message: `Đã gửi "delete" vào topic "${TOPICS.payments}"`, eventId: event.id });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
};

module.exports = {
  // Users
  createUser,
  updateUser,
  deleteUser,
  // Orders
  createOrder,
  updateOrder,
  deleteOrder,
  // Payments
  createPayment,
  updatePayment,
  deletePayment,
};
