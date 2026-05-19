// Worker độc lập - chỉ chạy Mail Consumer
// Tách biệt hoàn toàn khỏi API Server
// Chạy bằng: npm run worker

require("dotenv").config();
const { connectProducer } = require("./kafka/producer");
const { startMailConsumer } = require("./kafka/mail-consumer");

const start = async () => {
  // Producer cần để ghi kết quả gửi mail lên topic "mail-logs"
  await connectProducer();

  // Khởi động consumer lắng nghe topic "send-mail"
  await startMailConsumer();

  console.log("=== Mail Worker (xu ly gui mail doc lap) ===");
  console.log("Dang cho event tu Redpanda topic 'send-mail'...");
  console.log("=============================================");
};

start().catch((err) => {
  console.error("[Worker] Khong the khoi dong:", err);
  process.exit(1);
});
