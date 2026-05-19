// BullMQ Worker: xử lý job gửi mail, validate email, đẩy kết quả lên Redpanda mail-logs
const { Worker } = require("bullmq");
const { redisConnection } = require("../config/redis");
const { sendConfirmationEmail } = require("../services/mail.service");
const { publishMailLog } = require("../services/mail-log.service");

const EMAIL_PATTERN = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
const MAIL_QUEUE_NAME = "mail-queue";

const validateEmail = (email) =>
  typeof email === "string" && EMAIL_PATTERN.test(email.trim());

const startMailWorker = () => {
  const worker = new Worker(
    MAIL_QUEUE_NAME,
    async (job) => {
      const {
        id: mailRequestId,
        userId,
        email,
        name,
        token,
        subject,
        sequence,
        count,
      } = job.data;

      // Kiểm tra email hợp lệ
      if (!validateEmail(email)) {
        await publishMailLog({
          mailRequestId,
          userId,
          to: email,
          subject,
          status: "failed",
          sequence,
          count,
          error: new Error("Email khong hop le"),
          metadata: { source: "bullmq_worker" },
        });

        console.log(`[MailWorker] Email khong hop le: ${email}`);
        return { status: "failed", reason: "invalid_email" };
      }

      // Gửi mail
      const info = await sendConfirmationEmail({
        to: email,
        name,
        userId,
        token,
        subject,
        sequence,
        count,
      });

      await publishMailLog({
        mailRequestId,
        providerMessageId: info.providerMessageId,
        userId,
        to: email,
        subject: info.subject,
        status: "success",
        sequence,
        count,
        smtp: {
          accepted: info.accepted,
          rejected: info.rejected,
          response: info.response,
          envelope: info.envelope,
        },
        metadata: { source: "bullmq_worker" },
      });

      console.log(`[MailWorker] Da gui mail thanh cong toi ${email}`);
      return { status: "success" };
    },
    {
      connection: redisConnection,
      concurrency: 5,
    }
  );

  worker.on("completed", (job) => {
    console.log(`[MailWorker] Job ${job.id} hoan thanh`);
  });

  worker.on("failed", (job, err) => {
    console.error(`[MailWorker] Job ${job.id} that bai: ${err.message}`);
  });

  console.log("[MailWorker] BullMQ worker da san sang");
  return worker;
};

module.exports = { startMailWorker };
