import { Module } from "@nestjs/common";
import { ArchivalService } from "./archival.service";
import { ArchivalJob } from "./archival.job";
import { ArchivalIntegrityJob } from "./archival-integrity.job";
import { MetricsModule } from "../metrics/metrics.module";

/**
 * ArchivalModule (#413).
 *
 * Registers the daily archival job, the scheduled integrity re-verification
 * job (#697), and the ArchivalService.
 * Import in AppModule; the jobs are no-ops when ARCHIVAL_ENABLED=false.
 *
 * Requires PrismaModule (global) and @nestjs/schedule (via ScheduleModule).
 * MetricsModule is imported so the integrity job can inject MetricsService.
 */
@Module({
  imports: [MetricsModule],
  providers: [ArchivalService, ArchivalJob, ArchivalIntegrityJob],
  exports: [ArchivalService],
})
export class ArchivalModule {}