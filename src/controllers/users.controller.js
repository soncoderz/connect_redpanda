const crypto = require("crypto");
const { sendMessage } = require("../kafka/producer");

const USERS_TOPIC = process.env.USERS_TOPIC || "users";
const SEND_MAIL_TOPIC = process.env.SEND_MAIL_TOPIC || "send-mail";
const EMAIL_PATTERN = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;

const sanitizeUserPayload = (body) => {
  const { password, confirmPassword, ...safeBody } = body || {};
  return safeBody;
};


const parseMailCount = (value) => {
  const parsed = Number(value || 5);

  if (!Number.isFinite(parsed)) {
    return 5;
  }

  return Math.min(Math.max(Math.trunc(parsed), 1), 5);
};

const createUserEvent = ({ body, userId, email, name, token }) => {
  const safeBody = sanitizeUserPayload(body);

  return {
    ...safeBody,
    id: userId,
    eventType: "add",
    eventTime: new Date().toISOString(),
    name,
    email,
    status: "pending_email_confirmation",
    emailVerified: false,
    emailVerificationToken: token,
  };
};

const registerUser = async (req, res) => {
  const body = req.body || {};
  const count = body.count ? Math.min(Math.max(parseInt(body.count) || 1, 1), 100) : 1;
  const key = body.key || null;

  const results = [];

  for (let i = 1; i <= count; i++) {
    let email = String(body.email || "").trim().toLowerCase();
    let name = body.name || body.fullName || body.username || email;

    if (count > 1) {
      const randomId = crypto.randomUUID().slice(0, 8);
      const computedKey = key || null;

      email = email ? email.replace("@", `_${randomId}_${i}@`) : `user_${randomId}_${i}@example.com`;
      name = `${name} Bulk ${i}`;

      const userId = crypto.randomUUID();
      const token = crypto.randomBytes(32).toString("hex");
      const mailRequestId = crypto.randomUUID();
      const subject = process.env.MAIL_CONFIRM_SUBJECT || "Xac nhan tai khoan";
      const userEvent = createUserEvent({ body, userId, email, name, token });

      try {
        await sendMessage(USERS_TOPIC, userEvent, computedKey || userId);

        const sendMailEvent = {
          id: mailRequestId,
          eventType: "send_confirmation",
          userId,
          email,
          name,
          token,
          subject,
          eventTime: new Date().toISOString(),
        };

        await sendMessage(SEND_MAIL_TOPIC, sendMailEvent, computedKey || userId);

        results.push({
          email,
          userId,
          mailRequestId,
          key: computedKey,
          status: "success",
        });
      } catch (err) {
        results.push({
          email,
          key: computedKey,
          status: "failed",
          error: err.message,
        });
      }
    } else {
      const userId = crypto.randomUUID();
      const token = crypto.randomBytes(32).toString("hex");
      const mailRequestId = crypto.randomUUID();
      const subject = process.env.MAIL_CONFIRM_SUBJECT || "Xac nhan tai khoan";
      const userEvent = createUserEvent({ body, userId, email, name, token });

      try {
        await sendMessage(USERS_TOPIC, userEvent, key || userId);
      } catch (err) {
        console.error("[Users] Khong the gui user event:", err.message);
        return res.status(500).json({
          success: false,
          error: "Khong the tao user tren Redpanda",
          detail: err.message,
        });
      }

      try {
        const sendMailEvent = {
          id: mailRequestId,
          eventType: "send_confirmation",
          userId,
          email,
          name,
          token,
          subject,
          eventTime: new Date().toISOString(),
        };

        await sendMessage(SEND_MAIL_TOPIC, sendMailEvent, key || userId);
        results.push({
          email,
          userId,
          mailRequestId,
          key: key || userId,
          status: "success",
        });
      } catch (err) {
        console.error("[Users] Khong the gui send-mail event:", err.message);
        return res.status(202).json({
          success: true,
          message: "User da duoc tao, nhung khong the day event gui mail vao Redpanda.",
          userId,
          mailError: err.message,
        });
      }
    }
  }

  if (count > 1) {
    return res.status(201).json({
      success: true,
      message: `Da tao hang loat ${count} users thanh cong.`,
      results,
    });
  } else {
    return res.status(201).json({
      success: true,
      message: "Tao user thanh cong. Mail xac nhan dang duoc xu ly ngam.",
      userId: results[0].userId,
      mailRequestId: results[0].mailRequestId,
    });
  }
};

const confirmUser = async (req, res) => {
  const userId = req.query.userId;
  const token = req.query.token;

  if (!userId || !token) {
    return res.status(400).json({
      success: false,
      error: "Thieu userId hoac token",
    });
  }

  const event = {
    id: userId,
    eventType: "update",
    eventTime: new Date().toISOString(),
    status: "email_confirmed",
    emailVerified: true,
    emailVerifiedAt: new Date().toISOString(),
    emailVerificationToken: token,
  };

  await sendMessage(USERS_TOPIC, event, userId);

  return res.json({
    success: true,
    message: "Da nhan yeu cau xac nhan tai khoan.",
    userId,
  });
};

// =====================================================================
// CẬP NHẬT THÔNG TIN USER
// PUT /api/users/:id
// Body: { name?, email?, status? } — chỉ cần truyền trường muốn sửa
// =====================================================================
const updateUser = async (req, res) => {
  const userId = req.params.id;

  if (!userId) {
    return res.status(400).json({
      success: false,
      error: "Thieu userId trong URL (PUT /api/users/:id)",
    });
  }

  const body = req.body || {};
  const key = body.key || null;
  const safeBody = sanitizeUserPayload(body);

  // Tạo event update và gửi vào Redpanda
  const event = {
    ...safeBody,
    id: userId,
    eventType: "update",
    eventTime: new Date().toISOString(),
  };

  try {
    await sendMessage(USERS_TOPIC, event, key || userId);
  } catch (err) {
    console.error("[Users] Khong the gui update event:", err.message);
    return res.status(500).json({
      success: false,
      error: "Khong the gui event cap nhat user len Redpanda",
      detail: err.message,
    });
  }

  return res.json({
    success: true,
    message: "Da gui event cap nhat user vao Redpanda.",
    userId,
    updatedFields: Object.keys(safeBody),
  });
};

// =====================================================================
// XÓA USER
// DELETE /api/users/:id
// Không cần body — chỉ cần truyền userId trong URL
// =====================================================================
const deleteUser = async (req, res) => {
  const userId = req.params.id;

  if (!userId) {
    return res.status(400).json({
      success: false,
      error: "Thieu userId trong URL (DELETE /api/users/:id)",
    });
  }

  const event = {
    id: userId,
    eventType: "delete",
    eventTime: new Date().toISOString(),
  };

  try {
    await sendMessage(USERS_TOPIC, event, userId);
  } catch (err) {
    console.error("[Users] Khong the gui delete event:", err.message);
    return res.status(500).json({
      success: false,
      error: "Khong the gui event xoa user len Redpanda",
      detail: err.message,
    });
  }

  return res.json({
    success: true,
    message: "Da gui event xoa user vao Redpanda.",
    userId,
  });
};

module.exports = {
  confirmUser,
  registerUser,
  updateUser,
  deleteUser,
};


