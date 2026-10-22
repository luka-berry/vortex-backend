import { Injectable, Logger } from "@nestjs/common";
import { ConfigService } from "@nestjs/config";
import { PrismaService } from "../prisma/prisma.service";
import { AppConfig } from "../config/configuration";
import { MetricsService } from "../metrics/metrics.service";
import { ArchivalConfig } from "./archival-config";
import { ArchivalS3Client } from "./s3-client";
import { writeIntentsParquet, writeAuditParquet } from "./parquet-writer";
import {
  ArchivalManifest,
  ManifestFile,
  sha256Hex,
  verifyManifest,
} from "./archival-manifest";

/**
 * Result of one archival run.
 */
export interface ArchivalRunResult {
  date: string;
  skipped: boolean;
  intentRowsArchived: number;
  auditRowsArchived: number;
  intentRowsDeleted: number;
  auditRowsDeleted: number;
  durationMs: number;
  manifestKey: string | null;
}

/**
 * Result of one integrity re-verification sweep (#697).
 */
export interface ArchivalVerifyResult {
  /** Number of partitions whose manifests were checked this run. */
  partitionsChecked: number;
  /** Number of partitions whose files all matched their recorded digests. */
  partitionsOk: number;
  /** Number of partitions with at least one mismatch / missing file. */
  partitionsFailed: number;
  /** Per-partition failure details (empty when everything verified). */
  failures: Array<{
    date: string;
    key: string;
    reason: string;
    expected?: string | number;
    actual?: string | number;
  }>;
  /** Rotation cursor persisted for the next run (last verified date). */
  cursor: string | null;
  durationMs: number;
}

/**
 * ArchivalService (#413).
 *
 * Exports terminal intents (filled / cancelled / expired / slashed) that are
 * older than `retentionDays` to partitioned Parquet files in S3-compatible
 * storage, then deletes them from Postgres ONLY after verifying the upload.
 *
 * Design principles:
 *   - Streaming export: rows are fetched in cursor-based batches so a 1M-row
 *     day never loads all data into memory at once.
 *   - Idempotent: if the manifest already exists in S3 for a date, the job
 *     skips the export rather than re-running it.
 *   - Safe deletion: rows are deleted from Postgres only after the manifest
 *     checksum passes verification.  A failed upload / verify never deletes.
 *   - Audit entries go with their intents so the export is self-contained.
 *   - Integrity re-verification: `verifyArchivedPartitions()` re-checks a
 *     rolling sample of archived manifests (#697); see
 *     `docs/archival-design.md` for the rotation/sampling design notes.
 */
@Injectable()
export class ArchivalService {
  private readonly logger = new Logger(ArchivalService.name);
  private readonly archivalConfig: ArchivalConfig;
  private readonly s3: ArchivalS3Client;

  constructor(
    private readonly prisma: PrismaService,
    private readonly configService: ConfigService<AppConfig, true>,
    private readonly metrics: MetricsService,
  ) {
    this.archivalConfig = this.configService.get("archival", { infer: true });
    this.s3 = new ArchivalS3Client(this.archivalConfig);
  }

  /**
   * Archive all eligible intents for `date` (ISO-8601, YYYY-MM-DD).
   *
   * - Eligible: terminal state AND `created_at` falls within the date AND
   *   `created_at` is older than `retentionDays` days.
   * - Idempotent: if a manifest for `date` already exists in S3, the run is
   *   skipped immediately.
   *
   * @param date  The partition date, e.g. "2026-09-01".
   * @returns     A summary of what was exported and deleted.
   */
  async archiveDate(date: string): Promise<ArchivalRunResult> {
    const start = Date.now();
    const manifestKey = this.manifestKey(date);

    // Idempotency check — skip if already archived.
    if (await this.s3.exists(manifestKey)) {
      this.logger.log(`[archival] ${date}: manifest already exists — skipping`);
      return {
        date,
        skipped: true,
        intentRowsArchived: 0,
        auditRowsArchived: 0,
        intentRowsDeleted: 0,
        auditRowsDeleted: 0,
        durationMs: Date.now() - start,
        manifestKey,
      };
    }

    this.logger.log(`[archival] ${date}: starting export`);

    const { from, to } = this.dateBounds(date);
    const terminalStates = ["filled", "cancelled", "expired", "slashed"];

    // ── Step 1: cursor-based export of intents ───────────────────────────────
    const manifest: ArchivalManifest = {
      date,
      archivedAt: new Date().toISOString(),
      files: [],
      totalIntentRows: 0,
      totalAuditRows: 0,
    };

    const intentIds: string[] = [];
    const allIntentFiles: ManifestFile[] = [];

    let cursor: string | undefined;
    let fileIndex = 0;

    for (;;) {
      const batch = await this.prisma.withStatsTimeout((tx) =>
        (tx as unknown as typeof this.prisma).intent.findMany({
          where: {
            state: { in: terminalStates as never[] },
            createdAt: { gte: from, lt: to },
          },
          orderBy: { intentId: "asc" },
          take: this.archivalConfig.maxRowsPerFile,
          ...(cursor ? { cursor: { intentId: cursor }, skip: 1 } : {}),
          select: {
            intentId: true,
            user: true,
            srcChain: true,
            srcToken: true,
            srcAmount: true,
            dstToken: true,
            minDstAmount: true,
            quotedDstAmount: true,
            acceptedDstAmount: true,
            fillAmount: true,
            feeAmount: true,
            solver: true,
            state: true,
            createdAt: true,
            deadline: true,
            filledAt: true,
            slashedAt: true,
            slashReason: true,
            txHash: true,
            version: true,
            srcVerified: true,
            srcTokenId: true,
            dstTokenId: true,
            srcDecimals: true,
            dstDecimals: true,
          },
        }),
      );

      if (batch.length === 0) break;

      // Convert to Parquet-safe rows.
      const rows = batch.map((r) => ({
        intent_id: r.intentId,
        user: r.user,
        src_chain: String(r.srcChain),
        src_token: JSON.stringify(r.srcToken),
        src_amount: r.srcAmount,
        dst_token: JSON.stringify(r.dstToken),
        min_dst_amount: r.minDstAmount,
        quoted_dst_amount: r.quotedDstAmount ?? null,
        accepted_dst_amount: r.acceptedDstAmount ?? null,
        fill_amount: r.fillAmount ?? null,
        fee_amount: r.feeAmount ?? null,
        solver: r.solver ?? null,
        state: String(r.state),
        created_at: BigInt(r.createdAt),
        deadline: BigInt(r.deadline),
        filled_at: r.filledAt != null ? BigInt(r.filledAt) : null,
        slashed_at: r.slashedAt != null ? BigInt(r.slashedAt) : null,
        slash_reason: r.slashReason ?? null,
        tx_hash: r.txHash ?? null,
        version: r.version,
        src_verified: r.srcVerified,
        src_token_id: r.srcTokenId ?? null,
        dst_token_id: r.dstTokenId ?? null,
        src_decimals: r.srcDecimals ?? null,
        dst_decimals: r.dstDecimals ?? null,
      }));

      const parquetBuffer = await writeIntentsParquet(rows);
      const key = this.intentFileKey(date, fileIndex++);
      const checksum = sha256Hex(parquetBuffer);

      await this.s3.put(key, parquetBuffer, "application/octet-stream");

      allIntentFiles.push({
        key,
        type: "intents",
        rowCount: batch.length,
        sha256: checksum,
        sizeBytes: parquetBuffer.length,
      });
      intentIds.push(...batch.map((r) => r.intentId));
      manifest.totalIntentRows += batch.length;

      cursor = batch[batch.length - 1].intentId;
      if (batch.length < this.archivalConfig.maxRowsPerFile) break;
    }

    // ── Step 2: export corresponding audit entries ────────────────────────────
    const auditFiles: ManifestFile[] = [];
    if (intentIds.length > 0) {
      const batchSize = 5000;
      let auditCursor = 0n;
      let auditFileIndex = 0;

      for (;;) {
        const auditBatch = await this.prisma.withStatsTimeout((tx) =>
          (tx as unknown as typeof this.prisma).intentAuditLog.findMany({
            where: {
              intentId: { in: intentIds },
              id: { gt: auditCursor },
            },
            orderBy: { id: "asc" },
            take: batchSize,
          }),
        );

        if (auditBatch.length === 0) break;

        const auditRows = auditBatch.map((a) => ({
          id: a.id,
          intent_id: a.intentId,
          timestamp: a.timestamp.toISOString(),
          to_state: a.toState,
          actor: a.actor,
          reason: a.reason,
          metadata: a.metadata ? JSON.stringify(a.metadata) : null,
        }));

        const buf = await writeAuditParquet(auditRows);
        const key = this.auditFileKey(date, auditFileIndex++);
        const checksum = sha256Hex(buf);

        await this.s3.put(key, buf, "application/octet-stream");
        auditFiles.push({ key, type: "audit", rowCount: auditBatch.length, sha256: checksum, sizeBytes: buf.length });
        manifest.totalAuditRows += auditBatch.length;

        auditCursor = auditBatch[auditBatch.length - 1].id;
        if (auditBatch.length < batchSize) break;
      }
    }

    manifest.files = [...allIntentFiles, ...auditFiles];

    // ── Step 3: write manifest ────────────────────────────────────────────────
    const manifestBuffer = Buffer.from(JSON.stringify(manifest, null, 2), "utf-8");
    await this.s3.put(manifestKey, manifestBuffer, "application/json");

    // ── Step 4: verify uploads before any deletion ────────────────────────────
    await verifyManifest(manifest, (key) => this.s3.get(key));
    this.logger.log(`[archival] ${date}: verification passed (${manifest.totalIntentRows} intents, ${manifest.totalAuditRows} audit entries)`);

    // ── Step 5: delete from Postgres ──────────────────────────────────────────
    // Audit entries are deleted first (FK to intents); then intents.
    let auditDeleted = 0;
    let intentDeleted = 0;

    if (intentIds.length > 0) {
      // Batch deletes to avoid massive single transactions.
      const batchSize = 1000;
      for (let i = 0; i < intentIds.length; i += batchSize) {
        const batch = intentIds.slice(i, i + batchSize);
        const { count: ac } = await this.prisma.intentAuditLog.deleteMany({
          where: { intentId: { in: batch } },
        });
        const { count: ic } = await this.prisma.intent.deleteMany({
          where: { intentId: { in: batch } },
        });
        auditDeleted += ac;
        intentDeleted += ic;
      }
    }

    const durationMs = Date.now() - start;
    this.logger.log(
      `[archival] ${date}: done — archived=${intentDeleted} intents, ` +
      `audit=${auditDeleted}, duration=${durationMs}ms`,
    );

    return {
      date,
      skipped: false,
      intentRowsArchived: manifest.totalIntentRows,
      auditRowsArchived: manifest.totalAuditRows,
      intentRowsDeleted: intentDeleted,
      auditRowsDeleted: auditDeleted,
      durationMs,
      manifestKey,
    };
  }

  /**
   * Re-verify a rolling sample of previously archived date partitions (#697).
   *
   * Once `archiveDate()` deletes the Postgres rows, the Parquet files in S3
   * are the only copy of that data.  This method periodically re-runs
   * `verifyManifest()` against a rotating window of archived partitions so
   * silent bit rot / accidental overwrites / lifecycle-policy mistakes are
   * detected long before a restore is attempted.
   *
   * Rotation: archived partitions are discovered by listing manifest keys
   * under `partitionPrefix` (no side-table).  A cursor (the last verified
   * date) is persisted in S3 so each run verifies the next
   * `verifySampleSize` partitions after it, wrapping around at the end.
   *
   * Detection only — mismatches are logged and counted, never repaired.
   */
  async verifyArchivedPartitions(): Promise<ArchivalVerifyResult> {
    const start = Date.now();
    const failures: ArchivalVerifyResult["failures"] = [];

    // ── Step 1: discover archived partitions via manifest keys ───────────────
    const manifestKeys = await this.s3.listKeys(this.archivalConfig.partitionPrefix);
    const dates = manifestKeys
      .filter((key) => key.endsWith("/manifest.json"))
      .map((key) => this.dateFromManifestKey(key))
      .filter((d): d is string => d !== null)
      .sort();

    if (dates.length === 0) {
      this.logger.log("[archival-verify] no archived partitions found — nothing to verify");
      return {
        partitionsChecked: 0,
        partitionsOk: 0,
        partitionsFailed: 0,
        failures,
        cursor: null,
        durationMs: Date.now() - start,
      };
    }

    // ── Step 2: read the persisted rotation cursor ───────────────────────────
    const cursorKey = this.verifyCursorKey();
    let cursor: string | null = null;
    try {
      const raw = await this.s3.get(cursorKey);
      const parsed = JSON.parse(raw.toString("utf-8")) as { cursor?: string };
      cursor = typeof parsed.cursor === "string" ? parsed.cursor : null;
    } catch {
      // No cursor yet (first run) or unreadable — start from the beginning.
      cursor = null;
    }

    // ── Step 3: select the next window, wrapping around ──────────────────────
    const sampleSize = Math.max(1, this.archivalConfig.verifySampleSize);
    let startIndex = 0;
    if (cursor !== null) {
      const idx = dates.findIndex((d) => d > cursor);
      startIndex = idx === -1 ? 0 : idx;
    }
    const selected: string[] = [];
    for (let i = 0; i < Math.min(sampleSize, dates.length); i++) {
      selected.push(dates[(startIndex + i) % dates.length]);
    }

    this.logger.log(
      `[archival-verify] verifying ${selected.length}/${dates.length} partitions ` +
      `(cursor=${cursor ?? "none"})`,
    );

    // ── Step 4: verify each selected partition ───────────────────────────────
    let partitionsOk = 0;
    let partitionsFailed = 0;

    for (const date of selected) {
      const partitionFailures = await this.verifyPartition(date);
      if (partitionFailures.length === 0) {
        partitionsOk++;
      } else {
        partitionsFailed++;
        failures.push(...partitionFailures);
      }
    }

    // ── Step 5: persist the new cursor (last verified date) ──────────────────
    const newCursor = selected[selected.length - 1] ?? cursor;
    if (newCursor !== null) {
      try {
        await this.s3.put(
          cursorKey,
          Buffer.from(JSON.stringify({ cursor: newCursor, updatedAt: new Date().toISOString() }), "utf-8"),
          "application/json",
        );
      } catch (err) {
        this.logger.warn(
          `[archival-verify] failed to persist rotation cursor: ${(err as Error).message}`,
        );
      }
    }

    const durationMs = Date.now() - start;
    this.logger.log(
      `[archival-verify] done — checked=${selected.length} ok=${partitionsOk} ` +
      `failed=${partitionsFailed} duration=${durationMs}ms`,
    );

    return {
      partitionsChecked: selected.length,
      partitionsOk,
      partitionsFailed,
      failures,
      cursor: newCursor,
      durationMs,
    };
  }

  /**
   * Verify a single archived partition's manifest and files.
   * Returns the list of failures (empty when everything matches).
   */
  private async verifyPartition(
    date: string,
  ): Promise<ArchivalVerifyResult["failures"]> {
    const failures: ArchivalVerifyResult["failures"] = [];
    const manifestKey = this.manifestKey(date);

    let manifest: ArchivalManifest;
    try {
      const raw = await this.s3.get(manifestKey);
      manifest = JSON.parse(raw.toString("utf-8")) as ArchivalManifest;
    } catch (err) {
      const reason = `manifest unreadable: ${(err as Error).message}`;
      this.recordMismatch(date, manifestKey, reason);
      failures.push({ date, key: manifestKey, reason });
      return failures;
    }

    try {
      await verifyManifest(manifest, (key) => this.s3.get(key));
    } catch (err) {
      // verifyManifest throws on the first mismatch; surface it structurally.
      const message = (err as Error).message;
      const reason = message || "checksum/size mismatch";
      this.recordMismatch(date, manifestKey, reason);
      failures.push({ date, key: manifestKey, reason });
    }

    return failures;
  }

  /** Emit a structured error log and increment the integrity-failure metric. */
  private recordMismatch(date: string, key: string, reason: string): void {
    this.logger.error({
      event: "archival.integrity.mismatch",
      date,
      key,
      reason,
    });
    try {
      this.metrics.incrementArchivalIntegrityFailures({ date });
    } catch {
      // Metrics must never break verification.
    }
  }

  /** Extract the YYYY-MM-DD date from a manifest key, or null if malformed. */
  private dateFromManifestKey(key: string): string | null {
    const prefix = this.archivalConfig.partitionPrefix;
    if (!key.startsWith(prefix)) return null;
    const rest = key.slice(prefix.length);
    const slash = rest.indexOf("/");
    if (slash <= 0) return null;
    const date = rest.slice(0, slash);
    return /^\d{4}-\d{2}-\d{2}$/.test(date) ? date : null;
  }

  /** S3 key of the persisted rotation cursor. */
  private verifyCursorKey(): string {
    return `${this.archivalConfig.partitionPrefix}_verify-cursor.json`;
  }

  /**
   * Compute the cutoff date: today minus `retentionDays`.
   * Returns the date string in YYYY-MM-DD format.
   */
  cutoffDate(): string {
    const d = new Date();
    d.setUTCDate(d.getUTCDate() - this.archivalConfig.retentionDays);
    return d.toISOString().slice(0, 10);
  }

  // ── Key builders ──────────────────────────────────────────────────────────

  private manifestKey(date: string): string {
    return `${this.archivalConfig.partitionPrefix}${date}/manifest.json`;
  }

  private intentFileKey(date: string, index: number): string {
    return `${this.archivalConfig.partitionPrefix}${date}/intents-${String(index).padStart(4, "0")}.parquet`;
  }

  private auditFileKey(date: string, index: number): string {
    return `${this.archivalConfig.partitionPrefix}${date}/audit-${String(index).padStart(4, "0")}.parquet`;
  }

  /** Unix epoch bounds [from, to) for a YYYY-MM-DD date string. */
  private dateBounds(date: string): { from: number; to: number } {
    const d = new Date(date + "T00:00:00Z");
    const next = new Date(date + "T00:00:00Z");
    next.setUTCDate(next.getUTCDate() + 1);
    return {
      from: Math.floor(d.getTime() / 1000),
      to: Math.floor(next.getTime() / 1000),
    };
  }
}
