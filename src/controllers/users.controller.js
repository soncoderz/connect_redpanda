const crypto = require("crypto");
const { sendMessage } = require("../kafka/producer");
const { sendConfirmationEmail } = require("../services/mail.service");
const { publishMailLog } = require("../services/mail-log.service");

const USERS_TOPIC = process.env.USERS_TOPIC || "users";
const EMAIL_PATTERN = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;

const sanitizeUserPayload = (body) => {
  const { password, confirmPassword, ...safeBody } = body || {};
  return safeBody;
};

const validateEmail = (email) => typeof email === "string" && EMAIL_PATTERN.test(email.trim());

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

const logMailResult = async ({
  mailRequestId,
  userId,
  email,
  subject,
  sequence,
  count,
  info,
  error,
  metadata,
}) => {
  const status = info ? "success" : "failed";

  return publishMailLog({
    mailRequestId,
    providerMessageId: info?.providerMessageId,
    userId,
    to: email,
    subject,
    status,
    sequence,
    count,
    smtp: info
      ? {
          accepted: info.accepted,
          rejected: info.rejected,
          response: info.response,
          envelope: info.envelope,
        }
      : null,
    error,
    metadata,
  });
};

const registerUser = async (req, res) => {
  const body = req.body || {};
  const email = String(body.email || "").trim().toLowerCase();
  const name = body.name || body.fullName || body.username || email;

  if (!validateEmail(email)) {
    return res.status(400).json({
      success: false,
      error: "Email khong hop le",
    });
  }

  const userId = body.id || crypto.randomUUID();
  const token = crypto.randomBytes(32).toString("hex");
  const mailRequestId = crypto.randomUUID();
  const subject = process.env.MAIL_CONFIRM_SUBJECT || "Xac nhan tai khoan";
  const userEvent = createUserEvent({ body, userId, email, name, token });

  try {
    await sendMessage(USERS_TOPIC, userEvent);
  } catch (err) {
    console.error("[Users] Khong the gui user event:", err.message);
    return res.status(500).json({
      success: false,
      error: "Khong the tao user tren Redpanda",
      detail: err.message,
    });
  }

  try {
    const info = await sendConfirmationEmail({
      to: email,
      name,
      userId,
      token,
      subject,
    });
    const logEvent = await logMailResult({
      mailRequestId,
      userId,
      email,
      subject: info.subject,
      info,
      metadata: { source: "register_user" },
    });

    return res.status(201).json({
      success: true,
      message: "Tao user thanh cong. Vui long check mail de xac nhan tai khoan.",
      userId,
      userEventId: userEvent.id,
      mail: {
        status: "success",
        mailId: logEvent.mailId,
        logId: logEvent.id,
      },
    });
  } catch (err) {
    console.error("[Users] Gui mail xac nhan that bai:", err.message);

    const logEvent = await logMailResult({
      mailRequestId,
      userId,
      email,
      subject,
      error: err,
      metadata: { source: "register_user" },
    });

    return res.status(202).json({
      success: true,
      message: "User da duoc tao, nhung gui mail xac nhan that bai. Da ghi log that bai vao Redpanda.",
      userId,
      userEventId: userEvent.id,
      mail: {
        status: "failed",
        mailId: logEvent.mailId,
        logId: logEvent.id,
        error: err.message,
      },
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

  await sendMessage(USERS_TOPIC, event);

  return res.json({
    success: true,
    message: "Da nhan yeu cau xac nhan tai khoan.",
    userId,
  });
};

const sendFiveConfirmationEmails = async (req, res) => {
  const body = req.body || {};
  const email = String(body.email || body.to || "").trim().toLowerCase();
  const name = body.name || body.fullName || body.username || email;
  const userId = body.userId || body.id || crypto.randomUUID();
  const count = parseMailCount(body.count);
  const subject = body.subject || process.env.MAIL_CONFIRM_SUBJECT || "Xac nhan tai khoan";
  const results = [];

  if (!validateEmail(email)) {
    return res.status(400).json({
      success: false,
      error: "Email khong hop le",
    });
  }

  for (let index = 1; index <= count; index += 1) {
    const token = crypto.randomBytes(32).toString("hex");
    const mailRequestId = crypto.randomUUID();

    try {
      const info = await sendConfirmationEmail({
        to: email,
        name,
        userId,
        token,
        subject,
        sequence: index,
        count,
      });
      const logEvent = await logMailResult({
        mailRequestId,
        userId,
        email,
        subject: info.subject,
        sequence: index,
        count,
        info,
        metadata: { source: "send_five_confirmation_emails" },
      });

      results.push({
        sequence: index,
        status: "success",
        mailId: logEvent.mailId,
        logId: logEvent.id,
      });
    } catch (err) {
      const logEvent = await logMailResult({
        mailRequestId,
        userId,
        email,
        subject,
        sequence: index,
        count,
        error: err,
        metadata: { source: "send_five_confirmation_emails" },
      });

      results.push({
        sequence: index,
        status: "failed",
        mailId: logEvent.mailId,
        logId: logEvent.id,
        error: err.message,
      });
    }
  }

  const successCount = results.filter((item) => item.status === "success").length;
  const failureCount = results.length - successCount;

  return res.json({
    success: failureCount === 0,
    message: `Da gui lien tuc ${count} mail va ghi log vao Redpanda.`,
    userId,
    successCount,
    failureCount,
    results,
  });
};

module.exports = {
  confirmUser,
  registerUser,
  sendFiveConfirmationEmails,
};
