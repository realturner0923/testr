import express from "express";
import amqp from "amqplib";
import { createClient } from "redis";
import "dotenv/config";

const app = express();

const PORT = process.env.PORT || 3002;

const RABBITMQ_URL = process.env.RABBITMQ_URL;
const REDIS_URL = process.env.REDIS_URL;

app.use(express.json());

let redis;

async function start() {
  if (!RABBITMQ_URL) {
    throw new Error(
      "RABBITMQ_URL is not configured"
    );
  }

  if (!REDIS_URL) {
    throw new Error(
      "REDIS_URL is not configured"
    );
  }

  /*
   * RabbitMQ
   */

  const rabbitConnection =
    await amqp.connect(RABBITMQ_URL);

  const rabbitChannel =
    await rabbitConnection.createChannel();

  await rabbitChannel.assertQueue(
    "notification.jobs",
    {
      durable: true
    }
  );

  /*
   * Redis
   */

  redis = createClient({
    url: REDIS_URL
  });

  redis.on("error", (error) => {
    console.error(
      "Redis error:",
      error
    );
  });

  await redis.connect();

  console.log(
    "Notification service connected to RabbitMQ"
  );

  console.log(
    "Notification service connected to Redis"
  );

  /*
   * Consume notification jobs.
   */

  rabbitChannel.consume(
    "notification.jobs",
    async (message) => {
      if (!message) {
        return;
      }

      try {
        const notification =
          JSON.parse(
            message.content.toString()
          );

        console.log(
          "\nCreating notification:"
        );

        console.log(notification);

        /*
         * Publish the notification through
         * Redis Pub/Sub.
         */

        await redis.publish(
          "notifications",
          JSON.stringify(notification)
        );

        console.log(
          "Notification published to Redis"
        );

        rabbitChannel.ack(message);
      } catch (error) {
        console.error(
          "Notification processing failed:",
          error
        );

        rabbitChannel.nack(
          message,
          false,
          false
        );
      }
    }
  );

  /*
   * Health endpoint.
   */

  app.get("/health", (req, res) => {
    res.json({
      service: "notification",
      status: "ok"
    });
  });

  /*
   * Temporary direct test endpoint.
   *
   * Keep this for now so we can compare:
   *
   * /test -> Redis
   *
   * versus:
   *
   * webhook -> RabbitMQ -> worker -> notification -> Redis
   */

  app.post("/test", async (req, res) => {
    const notification = {
      type: "notification.create",

      title:
        req.body.title ??
        "Test Notification",

      message:
        req.body.message ??
        "Hello from the notification service!",

      timestamp:
        new Date().toISOString()
    };

    await redis.publish(
      "notifications",
      JSON.stringify(notification)
    );

    console.log(
      "Test notification published:"
    );

    console.log(notification);

    res.status(202).json({
      published: true,
      notification
    });
  });

  app.listen(PORT, () => {
    console.log(
      `Notification service running on http://localhost:${PORT}`
    );
  });
}

start().catch((error) => {
  console.error(error);
  process.exit(1);
});