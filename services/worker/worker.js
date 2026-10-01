import amqp from "amqplib";
import "dotenv/config";

const RABBITMQ_URL =
  process.env.RABBITMQ_URL;

async function start() {
  if (!RABBITMQ_URL) {
    throw new Error(
      "RABBITMQ_URL is not configured"
    );
  }

  const connection =
    await amqp.connect(RABBITMQ_URL);

  const channel =
    await connection.createChannel();

  /*
   * Queue containing events from external systems.
   */
  await channel.assertQueue("github.events", {
    durable: true
  });

  /*
   * Queue containing work for the notification service.
   */
  await channel.assertQueue("notification.jobs", {
    durable: true
  });

  /*
   * Only give this worker one unacknowledged
   * message at a time.
   */
  channel.prefetch(1);

  console.log("Worker connected to RabbitMQ");
  console.log("Worker waiting for GitHub events...");

  channel.consume(
    "github.events",
    async (message) => {
      if (!message) {
        return;
      }

      try {
        const event = JSON.parse(
          message.content.toString()
        );

        console.log("\nReceived GitHub event:");
        console.log(event);

        /*
         * This is the worker's job:
         *
         * Take a raw event and turn it into
         * something the notification service understands.
         */

        const notification = {
          type: "notification.create",

          title: "🚀 New GitHub push",

          message:
            `${event.commits} commit(s) pushed to ` +
            `${event.branch}`,

          repository: event.repository,

          sender: event.sender,

          timestamp: new Date().toISOString()
        };

        channel.sendToQueue(
          "notification.jobs",
          Buffer.from(
            JSON.stringify(notification)
          ),
          {
            persistent: true
          }
        );

        console.log(
          "Notification job created:"
        );

        console.log(notification);

        /*
         * Tell RabbitMQ:
         *
         * "I successfully processed this message."
         */
        channel.ack(message);
      } catch (error) {
        console.error(
          "Worker failed:",
          error
        );

        /*
         * Don't retry malformed messages yet.
         * We'll add proper retry/dead-letter handling later.
         */
        channel.nack(
          message,
          false,
          false
        );
      }
    }
  );
}

start().catch((error) => {
  console.error(error);
  process.exit(1);
});