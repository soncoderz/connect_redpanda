// Scripts đo thời gian xử lý gửi mail (Benchmark tool)
// Đo chính xác thời gian hoàn thành gửi X email bất kể số lượng partition hay worker
// Chạy bằng: npm run benchmark [số lượng_mail] (mặc định là 50)

require("dotenv").config();
const kafka = require("../src/config/kafka");

const MAIL_LOG_TOPIC = process.env.MAIL_LOG_TOPIC || "mail-logs";
const targetCount = parseInt(process.argv[2], 10) || 30;

// Tạo groupId ngẫu nhiên để luôn bắt đầu đọc từ thời điểm hiện tại (latest offset)
const consumer = kafka.consumer({
  groupId: `benchmark-group-${Math.random().toString(36).substring(2, 9)}`,
});

let processedCount = 0;
let startTime = null;

const run = async () => {
  console.log(`\n================ BENCHMARK TOOL ================`);
  console.log(`Topic giám sát   : ${MAIL_LOG_TOPIC}`);
  console.log(`Mục tiêu đo      : ${targetCount} email`);
  console.log(`Trạng thái       : Đang chờ loạt email mới...`);
  console.log(`================================================\n`);

  await consumer.connect();

  // Đọc từ tin nhắn MỚI phát sinh (fromBeginning: false)
  await consumer.subscribe({ topic: MAIL_LOG_TOPIC, fromBeginning: false });

  await consumer.run({
    eachMessage: async ({ message }) => {
      const log = JSON.parse(message.value.toString());

      // Khởi động đồng hồ khi nhận email đầu tiên của đợt test
      if (processedCount === 0) {
        startTime = Date.now();
        console.log(`🚀 [BẮT ĐẦU] Đã nhận email đầu tiên. Đang tính giờ...`);
      }

      processedCount++;
      const percent = Math.round((processedCount / targetCount) * 100);
      const progressBar = "█".repeat(Math.round(percent / 5)) + "░".repeat(20 - Math.round(percent / 5));

      console.log(
        `[${progressBar}] ${percent}% | Tiến độ: ${processedCount}/${targetCount} | Email: ${log.to} | Trạng thái: ${log.status}`
      );

      // Đạt mục tiêu
      if (processedCount >= targetCount) {
        const endTime = Date.now();
        const durationSeconds = (endTime - startTime) / 1000;
        const avgSpeed = (targetCount / durationSeconds).toFixed(2);

        console.log(`\n🎉 ================================================`);
        console.log(`📊 KẾT QUẢ ĐO LƯỜNG (BENCHMARK):`);
        console.log(`- Tổng số email xử lý  : ${targetCount}`);
        console.log(`- Tổng thời gian chạy  : ${durationSeconds.toFixed(2)} giây`);
        console.log(`- Tốc độ trung bình    : ${avgSpeed} email/giây`);
        console.log(`================================================\n`);

        // Reset để chuẩn bị cho đợt đo tiếp theo
        processedCount = 0;
        startTime = null;
        console.log(`🔄 Trạng thái: Đang chờ loạt email mới...\n`);
      }
    },
  });
};

run().catch((err) => {
  console.error("Lỗi đo lường:", err);
  process.exit(1);
});
