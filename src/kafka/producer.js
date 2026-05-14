// Producer: gửi message vào Redpanda topic
const kafka = require("../config/kafka");

const producer = kafka.producer();

// Kết nối producer tới Redpanda
const connectProducer = async () => {
  await producer.connect();
  console.log("[Producer] Da ket noi Redpanda");
};

// Gửi 1 message vào topic
const sendMessage = async (topic, message) => {
  await producer.send({
    topic,
    messages: [
      {
        key: message.id, // dùng id làm key để cùng patient vào cùng partition
        value: JSON.stringify(message),
      },
    ],
  });
  console.log(`[Producer] Da gui message vao topic "${topic}":`, message.id);
};

module.exports = { connectProducer, sendMessage };
