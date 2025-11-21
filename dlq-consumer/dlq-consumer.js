import { Kafka } from "kafkajs";

const KAFKA_BROKER = process.env.KAFKA_BROKER || "localhost:9092";
const DLQ_TOPIC = process.env.DLQ_TOPIC || "orders-dlq";

const kafka = new Kafka({
  clientId: "dlq-reader",
  brokers: [KAFKA_BROKER],
});

const consumer = kafka.consumer({ groupId: "dlq-group" });

async function run() {
  await consumer.connect();

  await consumer.subscribe({
    topic: DLQ_TOPIC,
    fromBeginning: true,
  });

  console.log(`Listening to DLQ topic: ${DLQ_TOPIC}`);

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      const key = message.key?.toString();
      const value = message.value?.toString();
      console.log("---- DLQ MESSAGE ----");
      console.log("Key:", key);
      console.log("Value:", value);
      console.log("---------------------");
    },
  });
}

run().catch((err) => {
  console.error("DLQ consumer error:", err);
  process.exit(1);
});
