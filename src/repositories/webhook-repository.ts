import {
  Inject,
  Injectable,
} from "@nestjs/common";

import {
  Prisma,
  PrismaClient,
} from "@prisma/client";

import {
  DATABASE,
} from "../database/database.constants.js";

import {
  DatabaseRepository,
} from "../database/database.repository.js";

@Injectable()
export class WebhookRepository
  extends DatabaseRepository {
  constructor(
    @Inject(DATABASE)
    db:
      | PrismaClient
      | Prisma.TransactionClient,
  ) {
    super(db);
  }

  public withDatabase(
    db: Prisma.TransactionClient,
  ): this {
    return new WebhookRepository(
      db,
    ) as this;
  }

  // =========================================================================
  // Find message
  // =========================================================================

  async findMessage(
    messageId: string,
  ) {
    return this.execute(
      "SELECT",
      "messages",
      async () => {
        const message =
          await this.db.message.findUnique({
            where: {
              id: messageId,
            },

            select: {
              id: true,
              clientId: true,
            },
          });

        return {
          result: message,

          rowsAffected:
            message
              ? 1
              : 0,
        };
      },
    );
  }

  // =========================================================================
  // Find enabled webhook endpoints
  // =========================================================================

  async findEnabledEndpoints(
    clientId: string,
  ) {
    return this.execute(
      "SELECT",
      "webhook_endpoints",
      async () => {
        const endpoints =
          await this.db.webhookEndpoint.findMany({
            where: {
              clientId,

              enabled: true,
            },

            orderBy: {
              createdAt: "asc",
            },
          });

        return {
          result: endpoints,

          rowsAffected:
            endpoints.length,
        };
      },
    );
  }

  // =========================================================================
  // Create webhook delivery
  // =========================================================================

  async createDelivery(
    data: {
      webhookEndpointId: string;
      messageId: string;
    },
  ) {
    return this.execute(
      "INSERT",
      "webhook_deliveries",
      async () => {
        const delivery =
          await this.db.webhookDelivery.create({
            data: {
              webhookEndpointId:
                data.webhookEndpointId,

              messageId:
                data.messageId,

              attemptNumber: 1,
            },
          });

        return {
          result: delivery,

          rowsAffected: 1,
        };
      },
    );
  }

  // =========================================================================
  // Update webhook delivery
  // =========================================================================

  async updateDelivery(
    id: string,
    data: {
      responseCode?: number;
      responseBody?: string;
      attemptedAt?: Date;
    },
  ) {
    return this.execute(
      "UPDATE",
      "webhook_deliveries",
      async () => {
        const result =
          await this.db.webhookDelivery.updateMany({
            where: {
              id,
            },

            data: {
              responseCode:
                data.responseCode,

              responseBody:
                data.responseBody,

              attemptedAt:
                data.attemptedAt,
            },
          });

        return {
          result,

          rowsAffected:
            result.count,
        };
      },
    );
  }
}