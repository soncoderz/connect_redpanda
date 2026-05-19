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
  const email = String(body.email || "").trim().toLowerCase();
  const name = body.name || body.fullName || body.username || email;
  const subject = process.env.MAIL_CONFIRM_SUBJECT || "Xac nhan tai khoan";
  const results = [];

  console.log(`[Register] Bat dau dang ky 50 lan cho email: ${email}`);

  try {
    for (let index = 1; index <= 50; index++) {
      const userId = crypto.randomUUID();
      const token = crypto.randomBytes(32).toString("hex");
      const mailRequestId = crypto.randomUUID();
      
      const userEvent = createUserEvent({ body, userId, email, name, token });

      // 1. Gửi user event lên Redpanda
      await sendMessage(USERS_TOPIC, userEvent);

      // 2. Đẩy event send-mail lên Redpanda
      const sendMailEvent = {
        id: mailRequestId,
        eventType: "send_confirmation",
        userId,
        email,
        name,
        token,
        subject,
        sequence: index,
        count: 50,
        eventTime: new Date().toISOString(),
      };

      await sendMessage(SEND_MAIL_TOPIC, sendMailEvent);

      results.push({
        sequence: index,
        userId,
        mailRequestId
      });
    }
  } catch (err) {
    console.error("[Users] Loi trong qua trinh dang ky 50 lan:", err.message);
    return res.status(500).json({
      success: false,
      error: "Loi he thong khi dang ky so luong lon",
      detail: err.message,
    });
  }

  return res.status(201).json({
    success: true,
    message: "Da tao 50 users va day 50 event gui mail vao Redpanda.",
    results,
  });
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

  await sendMessage(USERS_TOPIC, event);

  return res.json({
    success: true,
    message: "Da nhan yeu cau xac nhan tai khoan.",
    userId,
  });
};

// const sendFiveConfirmationEmails = async (req, res) => {
//   const body = req.body || {};
//   const email = String(body.email || body.to || "").trim().toLowerCase();
//   const name = body.name || body.fullName || body.username || email;
//   const userId = body.userId || body.id || crypto.randomUUID();
//   const count = parseMailCount(body.count);
//   const subject = body.subject || process.env.MAIL_CONFIRM_SUBJECT || "Xac nhan tai khoan";
//   const results = [];


//   // Đẩy nhiều event send-mail lên Redpanda (BullMQ worker sẽ xử lý ngầm)
//   for (let index = 1; index <= count; index += 1) {
//     const token = crypto.randomBytes(32).toString("hex");
//     const mailRequestId = crypto.randomUUID();

//     try {
//       const sendMailEvent = {
//         id: mailRequestId,
//         eventType: "send_confirmation",
//         userId,
//         email,
//         name,
//         token,
//         subject,
//         sequence: index,
//         count,
//         eventTime: new Date().toISOString(),
//       };

//       await sendMessage(SEND_MAIL_TOPIC, sendMailEvent);

//       results.push({
//         sequence: index,
//         status: "queued",
//         mailRequestId,
//       });
//     } catch (err) {
//       results.push({
//         sequence: index,
//         status: "queue_failed",
//         error: err.message,
//       });
//     }
//   }

//   return res.json({
//     success: true,
//     message: `Da day ${count} event gui mail vao Redpanda. BullMQ dang xu ly ngam.`,
//     userId,
//     results,
//   });
// };

module.exports = {
  confirmUser,
  registerUser,
  // sendFiveConfirmationEmails,
};
