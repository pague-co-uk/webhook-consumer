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

    const exchange =
      this.config.webhook.clientDlrExchange;

    const queue =
      this.config.webhook.clientDlrQueue;

    this.logger.info(
      {
        exchange,
        queue,
      },
      "Webhook consumer starting.",
    );

    try {
      await this.queue.connect();

      this.logger.info(
        {
          exchange,
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
          exchange,
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
      await this.queue.bindQueueToExchange(
        queue,
        exchange,
        "",
        {
          durable: true,
        },
      );

      this.logger.info(
        {
          exchange,
          queue,
        },
        "Webhook delivery receipt queue bound to client DLR exchange.",
      );
    } catch (error) {
      recordException(error);

      this.logger.error(
        {
          exchange,
          queue,

          err:
            error,
        },
        "Webhook consumer failed to bind queue to client DLR exchange.",
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
                  exchange,
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
        );

      this.logger.info(
        {
          exchange,
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
          exchange,
          queue,

          queueClientState:
            this.queue.currentState,

          err:
            error,
        },
        "Webhook consumer failed to subscribe to client DLR queue.",
      );

      throw error;
    }
  }

  // =========================================================================
  // Stop
  // =========================================================================

  async onModuleDestroy(): Promise<void> {
    this.running = false;

    const exchange =
      this.config.webhook.clientDlrExchange;

    const queue =
      this.config.webhook.clientDlrQueue;

    this.logger.info(
      {
        exchange,
        queue,
      },
      "Webhook consumer stopping.",
    );

    this.logger.info(
      {
        exchange,
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

          "webhook.exchange":
            this.config.webhook
              .clientDlrExchange,
        });

        this.logger.info(
          {
            messageId:
              dlr.messageId,

            providerMessageId:
              dlr.providerMessageId,

            status:
              dlr.status,

            queue:
              this.config.webhook
                .clientDlrQueue,

            exchange:
              this.config.webhook
                .clientDlrExchange,
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