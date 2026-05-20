// Producer: gửi message vào Redpanda topic
const kafka = require("../config/kafka");

const producer = kafka.producer();

// Kết nối producer tới Redpanda
const connectProducer = async () => {
  await producer.connect();
  console.log("[Producer] Da ket noi Redpanda");
};

const sendMessage = async (topic, message, key = null) => {
  const kafkaMessage = {
    value: JSON.stringify(message),
  };

  // Dùng key truyền vào hoặc lấy key từ thuộc tính của message nếu có
      // const messageKey = key || message.key || null;
      // if (messageKey) {
      //   kafkaMessage.key = String(messageKey);
      // } 
  const messageKey = key || null;
  await producer.send({
    topic,
    messages: [kafkaMessage],
  });
  console.log(`[Producer] Da gui message vao topic "${topic}" (key: ${messageKey || "none"}):`, message.id);
};

module.exports = { connectProducer, sendMessage };
