// Cấu hình Kafka client dùng KafkaJS
const { Kafka } = require("kafkajs");

// Tạo Kafka client kết nối tới Redpanda
const kafka = new Kafka({
  clientId: process.env.KAFKA_CLIENT_ID || "connect-redpanda-backend",
  brokers: (process.env.KAFKA_BROKERS || "localhost:19092").split(","),
});

module.exports = kafka;
