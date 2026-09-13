import { Injectable } from "@nestjs/common";

import {
  getComponentLogger,
  recordException,
  withSpan,
} from "@pague-co-uk/sms-gateway-telemetry";

import { WebhookRepository } from "../repositories/webhook-repository.js";

import type { ClientDlr } from "./types/client-dlr.js";

@Injectable()
export class WebhookService {
  private readonly logger =
    getComponentLogger(
      WebhookService.name,
    );

  constructor(
    private readonly repository:
      WebhookRepository,
  ) { }

  // =========================================================================
  // Process client DLR
  // =========================================================================

  async process(
    dlr: ClientDlr,
  ): Promise<void> {
    await withSpan(
      "WebhookService.process",
      async (span) => {
        span.setAttributes({
          "webhook.message_id":
            dlr.messageId,

          "webhook.provider_message_id":
            dlr.providerMessageId,

          "webhook.status":
            dlr.status,
        });

        try {
          this.logger.info(
            {
              messageId:
                dlr.messageId,

              providerMessageId:
                dlr.providerMessageId,

              status:
                dlr.status,
            },
            "Processing client delivery receipt.",
          );

          // =================================================================
          // Find message and client
          // =================================================================

          const message =
            await this.repository.findMessage(
              dlr.messageId,
            );

          if (!message) {
            this.logger.warn(
              {
                messageId:
                  dlr.messageId,
              },
              "Message not found for client delivery receipt.",
            );

            return;
          }

          span.setAttribute(
            "webhook.client_id",
            message.clientId,
          );

          // =================================================================
          // Find enabled webhook endpoints
          // =================================================================

          const endpoints =
            await this.repository
              .findEnabledEndpoints(
                message.clientId,
              );

          if (
            endpoints.length === 0
          ) {
            this.logger.info(
              {
                messageId:
                  message.id,

                clientId:
                  message.clientId,
              },
              "No enabled webhook endpoints found for client.",
            );

            return;
          }

          this.logger.info(
            {
              messageId:
                message.id,

              clientId:
                message.clientId,

              endpointCount:
                endpoints.length,
            },
            "Enabled webhook endpoints found.",
          );

          // =================================================================
          // Deliver to endpoints
          // =================================================================

          for (
            const endpoint of endpoints
          ) {
            await this.deliver(
              endpoint,
              message.id,
              dlr,
            );
          }

          this.logger.info(
            {
              messageId:
                message.id,

              clientId:
                message.clientId,

              endpointCount:
                endpoints.length,
            },
            "Client delivery receipt processing completed.",
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

          /*
           * Webhook delivery is best effort. Do not propagate
           * exceptions to the RabbitMQ consumer.
           */
          return;
        }
      },
    );
  }

  // =========================================================================
  // Deliver webhook
  // =========================================================================

  private async deliver(
    endpoint: {
      id: string;
      url: string;
    },
    messageId: string,
    dlr: ClientDlr,
  ): Promise<void> {
    await withSpan(
      "WebhookService.deliver",
      async () => {
        try {
          const delivery =
            await this.repository.createDelivery({
              webhookEndpointId:
                endpoint.id,

              messageId,
            });

          this.logger.info(
            {
              endpointId:
                endpoint.id,

              messageId,

              attemptNumber: 1,
            },
            "Sending client delivery receipt to webhook endpoint.",
          );

          const response =
            await fetch(
              endpoint.url,
              {
                method: "POST",

                headers: {
                  "content-type":
                    "application/json",
                },

                body:
                  JSON.stringify(dlr),
              },
            );

          const responseBody =
            await response.text();

          await this.repository.updateDelivery(
            delivery.id,
            {
              responseCode:
                response.status,

              responseBody,

              attemptedAt:
                new Date(),
            },
          );

          if (
            response.status === 200
          ) {
            this.logger.info(
              {
                endpointId:
                  endpoint.id,

                messageId,

                responseCode:
                  response.status,
              },
              "Client delivery receipt successfully delivered to webhook endpoint.",
            );

            return;
          }

          this.logger.warn(
            {
              endpointId:
                endpoint.id,

              messageId,

              responseCode:
                response.status,
            },
            "Webhook endpoint did not acknowledge client delivery receipt.",
          );
        } catch (error) {
          recordException(error);

          this.logger.error(
            {
              endpointId:
                endpoint.id,

              messageId,

              err:
                error,
            },
            "Webhook delivery failed.",
          );

          /*
           * Webhook delivery is best effort. Never propagate
           * the exception to the caller.
           */
          return;
        }
      },
    );
  }
}