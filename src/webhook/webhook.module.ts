import { Module } from "@nestjs/common";

import { WebhookRepository } from "../repositories/webhook-repository.js";

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
})
export class WebhookModule { }