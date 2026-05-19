// Kafka consumer: lắng nghe topic "send-mail" từ Redpanda, đẩy job vào BullMQ
const kafka = require("../config/kafka");
const { Queue } = require("bullmq");
const { redisConnection } = require("../config/redis");

const SEND_MAIL_TOPIC = process.env.SEND_MAIL_TOPIC || "send-mail";
const MAIL_QUEUE_NAME = "mail-queue";

const mailQueue = new Queue(MAIL_QUEUE_NAME, { connection: redisConnection });

const consumer = kafka.consumer({
  groupId: process.env.MAIL_CONSUMER_GROUP || "mail-consumer-group",
});

const startMailConsumer = async () => {
  await consumer.connect();
  await consumer.subscribe({ topic: SEND_MAIL_TOPIC, fromBeginning: false });

  await consumer.run({
    eachMessage: async ({ message }) => {
      const data = JSON.parse(message.value.toString());

      await mailQueue.add("send-confirmation", data, {
        jobId: data.id,
        attempts: 3,
        backoff: { type: "exponential", delay: 2000 },
      });

      console.log(`[MailConsumer] Da them job vao BullMQ: ${data.id}`);
    },
  });

  console.log(`[MailConsumer] Dang lang nghe topic "${SEND_MAIL_TOPIC}"`);
};

module.exports = { startMailConsumer };
