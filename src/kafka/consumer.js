// Consumer demo: đọc message từ Redpanda topic (chạy riêng để xem message)
require("dotenv").config();
const kafka = require("../config/kafka");

const topic = process.env.APPOINTMENT_TOPIC || "appointment-events";
const groupId = process.env.NODE_CONSUMER_GROUP || "node-appointment-consumer-group";

const consumer = kafka.consumer({ groupId });

const run = async () => {
  await consumer.connect();
  console.log(`[Consumer] Da ket noi Redpanda, group: "${groupId}"`);

  // Subscribe vào topic
  await consumer.subscribe({ topic, fromBeginning: true });
  console.log(`[Consumer] Dang lang nghe topic: "${topic}"...\n`);

  // Mỗi khi có message mới -> in ra console
  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      const value = JSON.parse(message.value.toString());
      console.log("--- Nhan duoc message ---");
      console.log("Topic:", topic);
      console.log("Partition:", partition);
      console.log("Offset:", message.offset);
      console.log("Data:", JSON.stringify(value, null, 2));
      console.log("-------------------------\n");
    },
  });
};

run().catch((err) => {
  console.error("[Consumer] Loi:", err);
  process.exit(1);
});
