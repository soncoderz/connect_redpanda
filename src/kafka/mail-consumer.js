// Kafka Consumer: lắng nghe topic "send-mail", validate email, gửi mail, đẩy kết quả lên mail-logs
const kafka = require("../config/kafka");
const { sendConfirmationEmail } = require("../services/mail.service");
const { publishMailLog } = require("../services/mail-log.service");

const SEND_MAIL_TOPIC = process.env.SEND_MAIL_TOPIC || "send-mail";
const EMAIL_PATTERN = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;

const validateEmail = (email) =>
  typeof email === "string" && EMAIL_PATTERN.test(email.trim());

const consumer = kafka.consumer({
  groupId: process.env.MAIL_CONSUMER_GROUP || "mail-consumer-group",
});

const startMailConsumer = async () => {
  await consumer.connect();
  await consumer.subscribe({ topic: SEND_MAIL_TOPIC, fromBeginning: true });

  await consumer.run({
    eachMessage: async ({ message }) => {
      const data = JSON.parse(message.value.toString());
      const {
        id: mailRequestId,
        userId,
        email,
        name,
        token,
        subject,
        sequence,
        count,
      } = data;

      console.log(`[MailConsumer] Nhan event gui mail: ${mailRequestId} -> ${email}`);

      // 1. Kiểm tra email hợp lệ
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
          metadata: { source: "mail_consumer" },
        });

        console.log(`[MailConsumer] Email khong hop le: ${email}`);
        return;
      }

      // 2. Gửi mail
      try {
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
          metadata: { source: "mail_consumer" },
        });

        console.log(`[MailConsumer] Gui mail thanh cong toi ${email}`);
      } catch (err) {
        await publishMailLog({
          mailRequestId,
          userId,
          to: email,
          subject,
          status: "failed",
          sequence,
          count,
          error: err,
          metadata: { source: "mail_consumer" },
        }); 

        console.error(`[MailConsumer] Gui mail that bai toi ${email}: ${err.message}`);
      }
    },
  });

  console.log(`[MailConsumer] Dang lang nghe topic "${SEND_MAIL_TOPIC}"`);
};

module.exports = { startMailConsumer };
