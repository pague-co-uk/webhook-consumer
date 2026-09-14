import { Module } from "@nestjs/common";

import { WebhookRepository } from "../repositories/webhook-repository.js";

import { DatabaseModule } from "../database/database.module.js";
import { QueueModule } from "../queue/queue.module.js";
import { WebhookConsumer } from "./webhook.consumer.js";
import { WebhookService } from "./webhook.service.js";

@Module({
  providers: [
    WebhookRepository,
    WebhookService,
    WebhookConsumer,
  ],
  exports: [
    WebhookService,
  ],
  imports: [QueueModule, DatabaseModule]
})
export class WebhookModule { }