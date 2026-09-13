import {
  Inject,
  Injectable,
  OnModuleDestroy,
  OnModuleInit,
} from "@nestjs/common";

import type {
  QueueClient,
} from "@pague-co-uk/sms-gateway-queue-client";

import {
  getComponentLogger,
  recordException,
  withSpan,
} from "@pague-co-uk/sms-gateway-telemetry";

import {
  AppConfigService,
} from "../config/config.service.js";

import {
  QUEUE_CLIENT,
} from "../queue/constants/queue.constants.js";

import {
  WebhookService,
} from "./webhook.service.js";

import type {
  ClientDlr,
} from "./types/client-dlr.js";

@Injectable()
export class WebhookConsumer
  implements
  OnModuleInit,
  OnModuleDestroy {
  private readonly logger =
    getComponentLogger(
      WebhookConsumer.name,
    );

  private running = false;

  constructor(
    @Inject(QUEUE_CLIENT)
    private readonly queue:
      QueueClient,

    private readonly config:
      AppConfigService,

    private readonly webhook:
      WebhookService,
  ) { }

  // =========================================================================
  // Start
  // =========================================================================

  async onModuleInit(): Promise<void> {
    this.running = true;

    const queue =
      this.config.webhook.clientDlrQueue;

    this.logger.info(
      {
        queue,
      },
      "Webhook consumer starting.",
    );

    try {
      await this.queue.connect();

      this.logger.info(
        {
          queue,

          queueClientState:
            this.queue.currentState,
        },
        "Webhook consumer connected to RabbitMQ.",
      );
    } catch (error) {
      recordException(error);

      this.logger.error(
        {
          queue,

          queueClientState:
            this.queue.currentState,

          err:
            error,
        },
        "Webhook consumer failed to connect to RabbitMQ.",
      );

      throw error;
    }

    try {
      const consumer =
        await this.queue.subscribe<ClientDlr>(
          queue,

          async (dlr) => {
            if (!this.running) {
              this.logger.warn(
                {
                  queue,
                },
                "Client DLR received while webhook consumer is stopping.",
              );

              return;
            }

            await this.handleClientDlr(
              dlr,
            );
          },

          {
            noAck: false,
          },
        );

      this.logger.info(
        {
          queue,

          consumerTag:
            consumer.consumerTag,

          queueClientState:
            this.queue.currentState,
        },
        "Webhook consumer successfully bound to client DLR queue.",
      );
    } catch (error) {
      recordException(error);

      this.logger.error(
        {
          queue,

          queueClientState:
            this.queue.currentState,

          err:
            error,
        },
        "Webhook consumer failed to bind to client DLR queue.",
      );

      throw error;
    }
  }

  // =========================================================================
  // Stop
  // =========================================================================

  async onModuleDestroy(): Promise<void> {
    this.running = false;

    const queue =
      this.config.webhook.clientDlrQueue;

    this.logger.info(
      {
        queue,
      },
      "Webhook consumer stopping.",
    );

    this.logger.info(
      {
        queue,
      },
      "Webhook consumer stopped.",
    );
  }

  // =========================================================================
  // Process client DLR
  // =========================================================================

  private async handleClientDlr(
    dlr: ClientDlr,
  ): Promise<void> {
    await withSpan(
      "WebhookConsumer.handleClientDlr",
      async (span) => {
        span.setAttributes({
          "webhook.message_id":
            dlr.messageId,

          "webhook.provider_message_id":
            dlr.providerMessageId,

          "webhook.status":
            dlr.status,

          "webhook.queue":
            this.config.webhook
              .clientDlrQueue,
        });

        this.logger.info(
          {
            messageId:
              dlr.messageId,

            providerMessageId:
              dlr.providerMessageId,

            status:
              dlr.status,
          },
          "Client delivery receipt received.",
        );

        try {
          await this.webhook.process(
            dlr,
          );

          this.logger.info(
            {
              messageId:
                dlr.messageId,

              providerMessageId:
                dlr.providerMessageId,

              status:
                dlr.status,
            },
            "Client delivery receipt processed.",
          );
        } catch (error) {
          recordException(error);

          this.logger.error(
            {
              messageId:
                dlr.messageId,

              providerMessageId:
                dlr.providerMessageId,

              status:
                dlr.status,

              err:
                error,
            },
            "Client delivery receipt processing failed.",
          );

          throw error;
        }

        void span;
      },
    );
  }
}