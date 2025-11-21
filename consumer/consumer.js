import { Kafka, logLevel } from "kafkajs";
import { SchemaRegistry } from "@kafkajs/confluent-schema-registry";

const KAFKA_BROKER = process.env.KAFKA_BROKER || "localhost:9092";
const SCHEMA_REGISTRY = process.env.SCHEMA_REGISTRY || "http://localhost:8081";
const TOPIC = process.env.TOPIC || "orders";
const DLQ_TOPIC = process.env.DLQ_TOPIC || "orders-dlq";

const kafka = new Kafka({
  clientId: "order-consumer",
  brokers: [KAFKA_BROKER],
  logLevel: logLevel.INFO,
});
const consumer = kafka.consumer({ groupId: "order-processing-group" });
const producer = kafka.producer();
const registry = new SchemaRegistry({ host: SCHEMA_REGISTRY });

// Running average state per product
const stats = {}; // { product: { count: N, sum: S, avg: S/N } }

// Retry settings
const MAX_RETRIES = 5;
const BASE_BACKOFF_MS = 500;

function sleep(ms) {
  return new Promise((res) => setTimeout(res, ms));
}

async function processOrder(order) {
  // Simulate transient failure and permanent failure conditions for demo:
  // throw new Error("transient") to simulate retryable; throw { permanent: true } to simulate permanent failure.
  // Replace with real business logic (DB write, external call...).
  // For demo, randomly throw transient error 10% and permanent 5%
  const r = Math.random();
  if (r < 0.05) {
    const e = new Error("permanent");
    e.permanent = true;
    throw e;
  } else if (r < 0.15) {
    const e = new Error("transient");
    e.permanent = false;
    throw e;
  }

  // Update running average
  const product = order.product;
  stats[product] ??= { count: 0, sum: 0, avg: 0 };
  const s = stats[product];
  s.count += 1;
  s.sum += order.price;
  s.avg = s.sum / s.count;

  // Return aggregated result for logging
  return { product, avg: s.avg, count: s.count };
}

async function handleMessage({ topic, partition, message }) {
  // decode Avro
  const decoded = await registry.decode(message.value);
  const order = decoded;

  let attempt = 0;
  while (attempt <= MAX_RETRIES) {
    try {
      attempt++;
      const result = await processOrder(order);
      console.log(
        `Processed order ${order.orderId} product=${order.product} price=${
          order.price
        } -> runningAvg=${result.avg.toFixed(2)} (n=${result.count})`
      );
      return; // success
    } catch (err) {
      const permanent = err.permanent === true;
      if (permanent) {
        console.error(
          `Permanent failure for order ${order.orderId}:`,
          err.message || err
        );
        // send to DLQ
        await producer.send({
          topic: DLQ_TOPIC,
          messages: [
            {
              key: order.orderId,
              value: Buffer.from(
                JSON.stringify({ order, error: String(err), attempts: attempt })
              ),
            },
          ],
        });
        return;
      } else {
        if (attempt > MAX_RETRIES) {
          // Exceeded retries -> DLQ
          console.error(
            `Exceeded retries for order ${order.orderId}. Sending to DLQ.`
          );
          await producer.send({
            topic: DLQ_TOPIC,
            messages: [
              {
                key: order.orderId,
                value: Buffer.from(
                  JSON.stringify({
                    order,
                    error: String(err),
                    attempts: attempt,
                  })
                ),
              },
            ],
          });
          return;
        } else {
          const backoff = BASE_BACKOFF_MS * Math.pow(2, attempt - 1);
          console.warn(
            `Transient failure for order ${order.orderId} attempt=${attempt}/${MAX_RETRIES}, retrying after ${backoff}ms. err=${err.message}`
          );
          await sleep(backoff);
          continue;
        }
      }
    }
  }
}

async function run() {
  await producer.connect();
  await consumer.connect();
  await consumer.subscribe({ topic: TOPIC, fromBeginning: true });

  // ensure DLQ topic exists
  const admin = kafka.admin();
  await admin.connect();
  const topics = await admin.listTopics();
  if (!topics.includes(DLQ_TOPIC)) {
    await admin.createTopics({
      topics: [{ topic: DLQ_TOPIC, numPartitions: 1, replicationFactor: 1 }],
    });
  }
  await admin.disconnect();

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      try {
        await handleMessage({ topic, partition, message });
      } catch (e) {
        console.error("Unhandled error in message handler:", e);
      }
    },
  });
}

run().catch((err) => {
  console.error(err);
  process.exit(1);
});
