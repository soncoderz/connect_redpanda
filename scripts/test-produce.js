// Script test: gửi thử 1 appointment event vào Redpanda
require("dotenv").config();
const crypto = require("crypto");
const kafka = require("../src/config/kafka");

const topic = process.env.APPOINTMENT_TOPIC || "appointment-events";

const run = async () => {
  const producer = kafka.producer();
  await producer.connect();

  // Tạo 1 event mẫu
  const event = {
    id: crypto.randomUUID(),
    patientName: "Nguyen Van A",
    doctorName: "Dr. Redpanda",
    status: "CREATED",
    amount: 150000,
    eventTime: new Date().toISOString(),
  };

  // Gửi vào topic
  await producer.send({
    topic,
    messages: [{ key: event.id, value: JSON.stringify(event) }],
  });

  console.log("Da gui event vao topic:", topic);
  console.log(JSON.stringify(event, null, 2));

  await producer.disconnect();
  process.exit(0);
};

run().catch((err) => {
  console.error("Loi:", err);
  process.exit(1);
});
