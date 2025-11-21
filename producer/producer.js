import { Kafka } from "kafkajs";
import { SchemaRegistry, readAVSC } from "@kafkajs/confluent-schema-registry";
import { v4 as uuidv4 } from "uuid";
import fs from "fs";
import path from "path";

const KAFKA_BROKER = process.env.KAFKA_BROKER || "localhost:9092";
const SCHEMA_REGISTRY = process.env.SCHEMA_REGISTRY || "http://localhost:8081";
const TOPIC = process.env.TOPIC || "orders";

const kafka = new Kafka({ brokers: [KAFKA_BROKER] });
const producer = kafka.producer();
const registry = new SchemaRegistry({ host: SCHEMA_REGISTRY });

async function ensureTopic() {
  const admin = kafka.admin();
  await admin.connect();
  const topics = await admin.listTopics();
  if (!topics.includes(TOPIC)) {
    await admin.createTopics({
      topics: [{ topic: TOPIC, numPartitions: 3, replicationFactor: 1 }],
    });
  }
  await admin.disconnect();
}

async function run() {
  await producer.connect();
  await ensureTopic();

  // register schema
  const schemaPath = path.join(process.cwd(), "../schemas/order.avsc");
  const schema = fs.readFileSync(schemaPath, "utf-8");
  const { id: schemaId } = await registry.register({ type: "AVRO", schema });

  console.log("Schema registered id:", schemaId);

  // produce random orders every second
  setInterval(async () => {
    const order = {
      orderId: uuidv4(),
      product: ["Item1", "Item2", "Item3"][Math.floor(Math.random() * 3)],
      price: Math.round((Math.random() * 100 + 1) * 100) / 100,
    };

    const buffer = await registry.encode(schemaId, order);

    try {
      await producer.send({
        topic: TOPIC,
        messages: [{ key: order.orderId, value: buffer }],
      });
      console.log("Produced", order);
    } catch (err) {
      console.error("Produce error", err);
    }
  }, 1000);
}

run().catch((err) => {
  console.error(err);
  process.exit(1);
});
