const crypto = require("crypto");
const { sendMessage } = require("../kafka/producer");

const MAIL_LOG_TOPIC = process.env.MAIL_LOG_TOPIC || "mail-logs";

const serializeError = (err) => {
  if (!err) {
    return null;
  }

  return {
    message: err.message || String(err),
    code: err.code || null,
    command: err.command || null,
    responseCode: err.responseCode || null,
    response: err.response || null,
  };
};

const publishMailLog = async ({
  mailRequestId,
  providerMessageId,
  userId,
  to,
  subject,
  template,
  status,
  sequence,
  count,
  smtp,
  error,
  metadata,
}) => {
  const now = new Date().toISOString();
  const event = {
    id: crypto.randomUUID(),
    eventType: "add",
    mailRequestId,
    mailId: providerMessageId || mailRequestId,
    providerMessageId: providerMessageId || null,
    userId: userId || null,
    to,
    subject,
    template: template || "account_confirmation",
    status,
    sequence: sequence || null,
    count: count || null,
    smtp: smtp || null,
    error: serializeError(error),
    metadata: metadata || {},
    eventTime: now,
    sentAt: status === "success" ? now : null,
    failedAt: status === "failed" ? now : null,
  };

  await sendMessage(MAIL_LOG_TOPIC, event);
  return event;
};

module.exports = {
  MAIL_LOG_TOPIC,
  publishMailLog,
};
