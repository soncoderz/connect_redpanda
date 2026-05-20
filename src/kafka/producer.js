const { Partitioners } = require("kafkajs");
const kafka = require("../config/kafka");

// Custom Partitioner:
const customPartitioner = () => {
  const defaultPartitioner = Partitioners.LegacyPartitioner();

  return ({ topic, partitionMetadata, message }) => {
    const key = message.key ? message.key.toString() : null;
    const numPartitions = partitionMetadata.length;

    
    if (key && ["A", "B", "C"].includes(key) && numPartitions > 3) {
      return 3;
    }

    if (key && ["D"].includes(key) && numPartitions > 0) {
      return 0;
    }


    // Các trường hợp còn lại dùng mặc định (MurmurHash2)
    return defaultPartitioner({ topic, partitionMetadata, message });
  };
};

const producer = kafka.producer({
  createPartitioner: customPartitioner,
});


const connectProducer = async () => {
  await producer.connect();
  console.log("[Producer] Da ket noi Redpanda");
};

const sendMessage = async (topic, message, key = null) => {
  const kafkaMessage = {
    value: JSON.stringify(message),
  };

  // Thiết lập key cho message để partitioner nhận biết được
  const messageKey = key || message.key || null;
  if (messageKey) {
    kafkaMessage.key = String(messageKey);
  }

  await producer.send({
    topic,
    messages: [kafkaMessage],
  });
  console.log(`[Producer] Da gui message vao topic "${topic}" (key: ${messageKey || "none"}):`, message.id);
};

module.exports = { connectProducer, sendMessage };
