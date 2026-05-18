const nodemailer = require("nodemailer");

let cachedTransporter = null;
let cachedKey = null;

const parseBool = (value, fallback) => {
  if (value === undefined || value === null || value === "") {
    return fallback;
  }

  return String(value).toLowerCase() === "true";
};

const getSmtpConfig = () => {
  const host = process.env.SMTP_HOST || "smtp.gmail.com";
  const port = Number(process.env.SMTP_PORT || 465);
  const secure = parseBool(process.env.SMTP_SECURE, port === 465);
  const user = process.env.SMTP_USER;
  const pass = process.env.SMTP_PASS;
  const from = process.env.SMTP_FROM || user;

  if (!user || !pass) {
    throw new Error("Missing SMTP_USER or SMTP_PASS. Use an SMTP app password in .env.");
  }

  return {
    host,
    port,
    secure,
    auth: { user, pass },
    from,
  };
};

const getTransporter = () => {
  const config = getSmtpConfig();
  const key = JSON.stringify({
    host: config.host,
    port: config.port,
    secure: config.secure,
    user: config.auth.user,
    pass: config.auth.pass,
  });

  if (!cachedTransporter || cachedKey !== key) {
    cachedTransporter = nodemailer.createTransport({
      host: config.host,
      port: config.port,
      secure: config.secure,
      auth: config.auth,
    });
    cachedKey = key;
  }

  return { transporter: cachedTransporter, from: config.from };
};

const escapeHtml = (value) =>
  String(value)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");

const buildConfirmationUrl = ({ userId, token }) => {
  const baseUrl = (process.env.APP_BASE_URL || `http://localhost:${process.env.PORT || 3001}`).replace(/\/$/, "");
  const rawPath = process.env.MAIL_CONFIRM_PATH || "/api/users/confirm";
  const path = rawPath.startsWith("/") ? rawPath : `/${rawPath}`;
  const url = new URL(`${baseUrl}${path}`);

  url.searchParams.set("userId", userId);
  url.searchParams.set("token", token);

  return url.toString();
};

const sendConfirmationEmail = async ({
  to,
  name,
  userId,
  token,
  sequence,
  count,
  subject = process.env.MAIL_CONFIRM_SUBJECT || "Xac nhan tai khoan",
}) => {
  const { transporter, from } = getTransporter();
  const confirmationUrl = buildConfirmationUrl({ userId, token });
  const displayName = name || to;
  const safeDisplayName = escapeHtml(displayName);
  const safeConfirmationUrl = escapeHtml(confirmationUrl);
  const sequenceLine = sequence && count ? `\nLan gui: ${sequence}/${count}` : "";

  const text = [
    `Xin chao ${displayName},`,
    "",
    "Tai khoan cua ban da duoc tao.",
    "Vui long xac nhan tai khoan bang lien ket sau:",
    confirmationUrl,
    sequenceLine,
  ]
    .filter(Boolean)
    .join("\n");

  const html = `
    <div style="font-family:Arial,sans-serif;line-height:1.5;color:#111827">
      <h2>Xac nhan tai khoan</h2>
      <p>Xin chao ${safeDisplayName},</p>
      <p>Tai khoan cua ban da duoc tao. Vui long bam nut ben duoi de xac nhan.</p>
      <p>
        <a href="${safeConfirmationUrl}" style="display:inline-block;background:#2563eb;color:#ffffff;padding:10px 16px;border-radius:6px;text-decoration:none">
          Xac nhan tai khoan
        </a>
      </p>
      <p>Neu nut khong hoat dong, mo link nay:</p>
      <p><a href="${safeConfirmationUrl}">${safeConfirmationUrl}</a></p>
      ${sequence && count ? `<p>Lan gui: ${sequence}/${count}</p>` : ""}
    </div>
  `;

  const info = await transporter.sendMail({
    from,
    to,
    subject,
    text,
    html,
  });

  return {
    subject,
    confirmationUrl,
    providerMessageId: info.messageId,
    accepted: info.accepted || [],
    rejected: info.rejected || [],
    response: info.response || null,
    envelope: info.envelope || null,
  };
};

module.exports = {
  buildConfirmationUrl,
  sendConfirmationEmail,
};
