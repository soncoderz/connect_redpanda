// Entry point cho worker process (tách riêng khỏi API server)
// Chạy: node src/worker.js
require("dotenv").config();
const { connectProducer } = require("./kafka/producer");
const { startMailConsumer } = require("./kafka/mail-consumer");
const { startMailWorker } = require("./workers/mail.worker");

const start = async () => {
  // Kết nối producer để worker có thể đẩy kết quả lên Redpanda mail-logs
  await connectProducer();

  // Khởi động BullMQ worker
  startMailWorker();

  // Khởi động Kafka consumer lắng nghe topic send-mail
  await startMailConsumer();

  console.log("[Worker] Mail consumer + BullMQ worker da khoi dong");
  console.log("[Worker] Dang cho event tu Redpanda topic 'send-mail'...");
};

start().catch((err) => {
  console.error("[Worker] Khong the khoi dong:", err);
  process.exit(1);
});
