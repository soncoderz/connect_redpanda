// Consumer 2: Lang nghe event de gui Notification (Telegram/Slack)
const kafka = require("../config/kafka");

const SEND_MAIL_TOPIC = process.env.SEND_MAIL_TOPIC || "send-mail";

const consumer = kafka.consumer({
  groupId: process.env.NOTIFICATION_CONSUMER_GROUP || "notification-consumer-group",
});

const startNotificationConsumer = async () => {
  await consumer.connect();
  await consumer.subscribe({ topic: SEND_MAIL_TOPIC, fromBeginning: true });

  await consumer.run({
    eachMessage: async ({ partition, message }) => {
      const data = JSON.parse(message.value.toString());
      const {
        id: mailRequestId,
        email,
        name,
        token,
      } = data;

      console.log(
        `[NotificationConsumer] [P${partition}] 🔔 NHAN EVENT: ${mailRequestId}`
      );
      console.log(
        `[NotificationConsumer] ---> Tien hanh gui thong bao qua Telegram/Slack cho User: ${name} (${email})`
      );
    },
  });

  console.log(`[NotificationConsumer] Dang lang nghe topic "${SEND_MAIL_TOPIC}"`);
};

module.exports = { startNotificationConsumer };
