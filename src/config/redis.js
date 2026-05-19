// Redis connection config for BullMQ
const redisConnection = {
  host: process.env.REDIS_HOST || "localhost",
  port: Number(process.env.REDIS_PORT || 6379),
};

module.exports = { redisConnection };
