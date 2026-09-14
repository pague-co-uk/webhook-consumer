import { Module } from "@nestjs/common";

import { ConfigModule } from "./config/config.module.js";
import { DatabaseModule } from "./database/database.module.js";
import { HealthModule } from "./health/health.module.js";
import { QueueModule } from "./queue/queue.module.js";
import { WebhookModule } from "./webhook/webhook.module.js";

@Module({
  imports: [
    ConfigModule,
    DatabaseModule,
    WebhookModule,
    HealthModule,
    QueueModule
  ],
})
export class AppModule { }