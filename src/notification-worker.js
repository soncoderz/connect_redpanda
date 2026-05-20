// Worker độc lập thứ 2 - chỉ chạy Notification Consumer (Telegram/Slack)
// Tách biệt hoàn toàn khỏi Mail Worker và API Server
// Chạy bằng: npm run notification-worker

require("dotenv").config();
const { connectProducer } = require("./kafka/producer");
const { startNotificationConsumer } = require("./kafka/notification-consumer");

const start = async () => {
  // Kết nối producer
  await connectProducer();

  // Khởi động consumer lắng nghe topic "send-mail" để gửi notification
  await startNotificationConsumer();

  console.log("=== Notification Worker (xu ly gui Telegram/Slack doc lap) ===");
  console.log("Dang cho event tu Redpanda topic 'send-mail'...");
  console.log("==============================================================");
};

start().catch((err) => {
  console.error("[Notification Worker] Khong the khoi dong:", err);
  process.exit(1);
});
