// Kafka Consumer: listen to topic "send-mail" and fake sending Gmail by logging.
const kafka = require("../config/kafka");

const SEND_MAIL_TOPIC = process.env.SEND_MAIL_TOPIC || "send-mail";

const consumer = kafka.consumer({
  groupId: process.env.MAIL_CONSUMER_GROUP || "mail-consumer-group",
});

const startMailConsumer = async () => {
  await consumer.connect();
  await consumer.subscribe({ topic: SEND_MAIL_TOPIC, fromBeginning: true });

  await consumer.run({
    eachMessage: async ({ partition, message }) => {
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

      console.log(
        `[MailConsumer] [P${partition}] Nhan event gui mail: ${mailRequestId} -> ${email}`
      );

      console.log("oke", {
        mailRequestId,
        userId,
        email,
        name,
        token,
        subject,
        sequence,
        count,
      });
    },
  });

  console.log(`[MailConsumer] Dang lang nghe topic "${SEND_MAIL_TOPIC}"`);
};

module.exports = { startMailConsumer };
