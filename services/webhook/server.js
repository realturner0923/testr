import express from "express";
import amqp from "amqplib";
import { verify } from "@octokit/webhooks-methods";
import "dotenv/config";

const app = express();

const PORT = process.env.PORT || 3001;

const RABBITMQ_URL = process.env.RABBITMQ_URL;

const GITHUB_WEBHOOK_SECRET =
  process.env.GITHUB_WEBHOOK_SECRET;


const GITHUB_QUEUE = "github.events";
let rabbitConnection;
let rabbitChannel;

/*
 * GitHub signature verification requires the
 * ORIGINAL request body.
 *
 * Express parses the JSON for us, but we also
 * save the raw body as a UTF-8 string.
 */
app.use(
  express.json({
    verify: (req, res, buffer) => {
      req.rawBody = buffer.toString("utf8");
    }
  })
);

/*
 * ------------------------------------------------------
 * HEALTH CHECK
 * ------------------------------------------------------
 */

app.get("/health", (req, res) => {
  res.json({
    service: "webhook",
    status: "ok"
  });
});

/*
 * ------------------------------------------------------
 * GITHUB WEBHOOK
 * ------------------------------------------------------
 *
 * GitHub sends POST requests here.
 *
 * GitHub
 *    ↓
 * /webhooks/github
 *    ↓
 * RabbitMQ
 */

app.post(
  "/webhooks/github",
  async (req, res) => {
    try {
      /*
       * --------------------------------------------------
       * 1. Get GitHub signature
       * --------------------------------------------------
       */

      const signature =
        req.headers["x-hub-signature-256"];

      if (!signature) {
        console.warn(
          "GitHub webhook rejected: missing signature"
        );

        return res.status(401).json({
          error: "Missing GitHub signature"
        });
      }

      /*
       * --------------------------------------------------
       * 2. Make sure our secret exists
       * --------------------------------------------------
       */

      if (!GITHUB_WEBHOOK_SECRET) {
        console.error(
          "GITHUB_WEBHOOK_SECRET is not configured"
        );

        return res.status(500).json({
          error: "Webhook secret is not configured"
        });
      }

      /*
       * --------------------------------------------------
       * 3. Verify the request actually came from GitHub
       * --------------------------------------------------
       */

      const valid = await verify(
        GITHUB_WEBHOOK_SECRET,
        req.rawBody,
        signature
      );

      if (!valid) {
        console.warn(
          "GitHub webhook rejected: invalid signature"
        );

        return res.status(401).json({
          error: "Invalid GitHub signature"
        });
      }

      /*
       * --------------------------------------------------
       * 4. Determine GitHub event type
       * --------------------------------------------------
       */

      const githubEvent =
        req.headers["x-github-event"];

      console.log(
        `Received GitHub event: ${githubEvent}`
      );

      /*
       * We're only interested in pushes right now.
       *
       * GitHub may send other events such as:
       *
       * ping
       * pull_request
       * issues
       * etc.
       */

      if (githubEvent !== "push") {
        console.log(
          `Ignoring GitHub event: ${githubEvent}`
        );

        return res.status(200).json({
          received: true,
          ignored: true,
          event: githubEvent
        });
      }

      /*
       * --------------------------------------------------
       * 5. Read the GitHub payload
       * --------------------------------------------------
       */

      const payload = req.body;

      /*
       * --------------------------------------------------
       * 6. Convert GitHub's payload into OUR event format
       * --------------------------------------------------
       *
       * We don't want the worker tightly coupled to the
       * entire GitHub API payload.
       */

      const event = {
        type: "github.push",

        repository:
          payload.repository?.full_name ??
          "unknown",

        branch:
          payload.ref?.replace(
            "refs/heads/",
            ""
          ) ?? "unknown",

        commits:
          payload.commits?.length ?? 0,

        sender:
          payload.sender?.login ??
          "unknown",

        commitMessages:
          payload.commits?.map(
            (commit) => commit.message
          ) ?? [],

        before:
          payload.before ?? null,

        after:
          payload.after ?? null,

        timestamp:
          new Date().toISOString()
      };

      /*
       * --------------------------------------------------
       * 7. Send event to RabbitMQ
       * --------------------------------------------------
       */

      if (!rabbitChannel) {
        throw new Error(
          "RabbitMQ channel is not available"
        );
      }

      rabbitChannel.sendToQueue(
        GITHUB_QUEUE,
        Buffer.from(
          JSON.stringify(event)
        ),
        {
          persistent: true
        }
      );

      /*
       * --------------------------------------------------
       * 8. Log event
       * --------------------------------------------------
       */

      console.log(
        "GitHub push queued:"
      );

      console.log(
        JSON.stringify(
          event,
          null,
          2
        )
      );

      /*
       * 202 means:
       *
       * "We received the event and accepted it
       *  for asynchronous processing."
       */

      return res.status(202).json({
        received: true,
        queued: true
      });
    } catch (error) {
      console.error(
        "GitHub webhook error:",
        error
      );

      return res.status(500).json({
        error: "Failed to process webhook"
      });
    }
  }
);

/*
 * ------------------------------------------------------
 * TEMPORARY TEST ENDPOINT
 * ------------------------------------------------------
 *
 * This allows us to test the pipeline without GitHub.
 *
 * POST /test
 *
 * Eventually we can remove this.
 */

app.post("/test", async (req, res) => {
  try {
    if (!rabbitChannel) {
      throw new Error(
        "RabbitMQ channel is not available"
      );
    }

    const event = {
      type: "github.push",

      repository:
        req.body.repository ??
        "notification-platform",

      branch:
        req.body.branch ??
        "main",

      commits:
        req.body.commits ??
        1,

      sender:
        req.body.sender ??
        "developer",

      commitMessages:
        req.body.commitMessages ??
        ["Test commit"],

      before: null,

      after: null,

      timestamp:
        new Date().toISOString()
    };

    rabbitChannel.sendToQueue(
      GITHUB_QUEUE,
      Buffer.from(
        JSON.stringify(event)
      ),
      {
        persistent: true
      }
    );

    console.log(
      "Test GitHub event queued:"
    );

    console.log(
      JSON.stringify(
        event,
        null,
        2
      )
    );

    return res.status(202).json({
      queued: true,
      event
    });
  } catch (error) {
    console.error(
      "Test webhook error:",
      error
    );

    return res.status(500).json({
      error: "Failed to queue test event"
    });
  }
});

/*
 * ------------------------------------------------------
 * RABBITMQ CONNECTION
 * ------------------------------------------------------
 */

async function connectRabbitMQ() {
  if (!RABBITMQ_URL) {
    throw new Error(
      "RABBITMQ_URL is not configured"
    );
  }

  rabbitConnection =
    await amqp.connect(RABBITMQ_URL);

  /*
   * Connection-level errors
   */

  rabbitConnection.on(
    "error",
    (error) => {
      console.error(
        "RabbitMQ connection error:",
        error
      );
    }
  );

  /*
   * Connection closed
   */

  rabbitConnection.on(
    "close",
    () => {
      console.error(
        "RabbitMQ connection closed"
      );
    }
  );

  /*
   * Create channel
   */

  rabbitChannel =
    await rabbitConnection.createChannel();

  /*
   * Make sure our queue exists.
   */

  await rabbitChannel.assertQueue(
    GITHUB_QUEUE,
    {
      durable: true
    }
  );

  console.log(
    "Webhook service connected to RabbitMQ"
  );

  console.log(
    `RabbitMQ queue ready: ${GITHUB_QUEUE}`
  );
}

/*
 * ------------------------------------------------------
 * START SERVER
 * ------------------------------------------------------
 */

async function start() {
  await connectRabbitMQ();

  app.listen(PORT, () => {
    console.log(
      `Webhook service running on http://localhost:${PORT}`
    );

    console.log(
      `GitHub webhook endpoint: http://localhost:${PORT}/webhooks/github`
    );
  });
}

/*
 * ------------------------------------------------------
 * START APPLICATION
 * ------------------------------------------------------
 */

start().catch((error) => {
  console.error(
    "Failed to start webhook service:",
    error
  );

  process.exit(1);
});