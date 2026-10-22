import { ConfigService } from "@nestjs/config";
import { ArchivalService } from "./archival.service";
import { PrismaService } from "../prisma/prisma.service";
import { sha256Hex } from "./archival-manifest";
import type { AppConfig } from "../config/configuration";

/**
 * Unit tests for ArchivalService (#413).
 *
 * S3 and database calls are fully mocked so the tests run without MinIO or Postgres.
 */

// ── helpers ──────────────────────────────────────────────────────────────────

const DATE = "2026-09-01";
const FROM_TS = Math.floor(new Date("2026-09-01T00:00:00Z").getTime() / 1000);

function makeConfig(overrides: Partial<AppConfig["archival"]> = {}): ConfigService<AppConfig, true> {
  const archival: AppConfig["archival"] = {
    enabled: true,
    bucketName: "test-bucket",
    endpoint: "http://localhost:9000",
    region: "us-east-1",
    accessKeyId: "minioadmin",
    secretAccessKey: "minioadmin",
    retentionDays: 30,
    partitionPrefix: "date=",
    maxRowsPerFile: 100,
    ...overrides,
  };
  return {
    get: (key: keyof AppConfig) => (key === "archival" ? archival : undefined),
  } as unknown as ConfigService<AppConfig, true>;
}

function makeIntent(id: string, createdAt = FROM_TS + 100) {
  return {
    intentId: id,
    user: "GUSER1",
    srcChain: "ethereum",
    srcToken: { address: "0xabc", symbol: "USDC", name: "USD Coin", decimals: 6 },
    srcAmount: "1000000",
    dstToken: { contract: "CTEST", symbol: "USDC", decimals: 7 },
    minDstAmount: "990000",
    quotedDstAmount: null,
    acceptedDstAmount: null,
    fillAmount: "995000",
    feeAmount: null,
    solver: "GSOLVER",
    state: "filled",
    createdAt,
    deadline: createdAt + 3600,
    filledAt: createdAt + 300,
    slashedAt: null,
    slashReason: null,
    txHash: "abc123",
    version: 1,
    srcVerified: true,
    srcTokenId: null,
    dstTokenId: null,
    srcDecimals: null,
    dstDecimals: null,
  };
}

// ── tests ─────────────────────────────────────────────────────────────────────

describe("ArchivalService (#413)", () => {
  let service: ArchivalService;
  let s3Puts: Map<string, Buffer>;
  let s3Store: Map<string, Buffer>;
  let prismaFindIntents: jest.Mock;
  let prismaFindAudit: jest.Mock;
  let prismaDeleteAudit: jest.Mock;
  let prismaDeleteIntents: jest.Mock;
  let prismaWithStatsTimeout: jest.Mock;

  beforeEach(() => {
    s3Puts = new Map();
    s3Store = new Map();

    prismaFindIntents = jest.fn();
    prismaFindAudit = jest.fn().mockResolvedValue([]);
    prismaDeleteAudit = jest.fn().mockResolvedValue({ count: 0 });
    prismaDeleteIntents = jest.fn().mockResolvedValue({ count: 0 });

    // Mock prisma.withStatsTimeout to just call the callback with a mock tx.
    prismaWithStatsTimeout = jest.fn().mockImplementation(async (cb: (tx: unknown) => Promise<unknown>) => {
      const mockTx = {
        intent: {
          findMany: prismaFindIntents,
        },
        intentAuditLog: {
          findMany: prismaFindAudit,
        },
      };
      return cb(mockTx);
    });

    const mockPrisma = {
      withStatsTimeout: prismaWithStatsTimeout,
      intentAuditLog: { deleteMany: prismaDeleteAudit },
      intent: { deleteMany: prismaDeleteIntents },
    } as unknown as PrismaService;

    service = new ArchivalService(mockPrisma, makeConfig());

    // Patch the internal S3 client.
    const s3Client = (service as unknown as { s3: { put: jest.Mock; exists: jest.Mock; get: jest.Mock } }).s3;
    s3Client.put = jest.fn().mockImplementation(async (key: string, buf: Buffer) => {
      s3Store.set(key, buf);
      s3Puts.set(key, buf);
    });
    s3Client.exists = jest.fn().mockResolvedValue(false);
    s3Client.get = jest.fn().mockImplementation(async (key: string) => {
      const data = s3Store.get(key);
      if (!data) throw new Error(`S3 key not found: ${key}`);
      return data;
    });
  });

  it("archives eligible intents and produces a manifest", async () => {
    const intents = [makeIntent("id-1"), makeIntent("id-2")];
    prismaFindIntents.mockResolvedValueOnce(intents).mockResolvedValueOnce([]);

    const result = await service.archiveDate(DATE);

    expect(result.skipped).toBe(false);
    expect(result.intentRowsArchived).toBe(2);
    expect(result.manifestKey).toMatch(/manifest\.json/);
    expect(s3Puts.size).toBeGreaterThan(0);

    // Manifest must be valid JSON with correct date.
    const manifestBuf = s3Store.get(`date=${DATE}/manifest.json`);
    expect(manifestBuf).toBeDefined();
    const manifest = JSON.parse(manifestBuf!.toString("utf-8"));
    expect(manifest.date).toBe(DATE);
    expect(manifest.totalIntentRows).toBe(2);
  });

  it("skips when manifest already exists (idempotency)", async () => {
    const s3Client = (service as unknown as { s3: { exists: jest.Mock } }).s3;
    s3Client.exists = jest.fn().mockResolvedValue(true);

    const result = await service.archiveDate(DATE);

    expect(result.skipped).toBe(true);
    expect(prismaFindIntents).not.toHaveBeenCalled();
  });

  it("never deletes rows when S3 put throws (upload failure)", async () => {
    const intents = [makeIntent("id-fail")];
    prismaFindIntents.mockResolvedValueOnce(intents).mockResolvedValueOnce([]);

    const s3Client = (service as unknown as { s3: { put: jest.Mock } }).s3;
    s3Client.put = jest.fn().mockRejectedValue(new Error("S3 unavailable"));

    await expect(service.archiveDate(DATE)).rejects.toThrow("S3 unavailable");
    expect(prismaDeleteIntents).not.toHaveBeenCalled();
    expect(prismaDeleteAudit).not.toHaveBeenCalled();
  });

  it("throws on checksum mismatch and does not delete rows", async () => {
    const intents = [makeIntent("id-checksum")];
    prismaFindIntents.mockResolvedValueOnce(intents).mockResolvedValueOnce([]);

    // Corrupt the data returned by s3.get so the checksum fails.
    const s3Client = (service as unknown as { s3: { put: jest.Mock; get: jest.Mock } }).s3;
    s3Client.put = jest.fn().mockImplementation(async (key: string, buf: Buffer) => {
      s3Store.set(key, buf);
    });
    s3Client.get = jest.fn().mockImplementation(async (key: string) => {
      if (key.endsWith(".parquet")) {
        return Buffer.from("corrupted data");
      }
      return s3Store.get(key)!;
    });

    await expect(service.archiveDate(DATE)).rejects.toThrow(/checksum mismatch/);
    expect(prismaDeleteIntents).not.toHaveBeenCalled();
  });

  it("archives zero rows gracefully (empty partition)", async () => {
    prismaFindIntents.mockResolvedValue([]);

    const result = await service.archiveDate(DATE);
    expect(result.intentRowsArchived).toBe(0);
    expect(result.intentRowsDeleted).toBe(0);
    expect(result.skipped).toBe(false);
  });

  it("sha256Hex computes stable digests", () => {
    const buf = Buffer.from("hello world", "utf-8");
    const digest = sha256Hex(buf);
    // SHA-256 produces a 64-character hex string.
    expect(digest).toHaveLength(64);
    expect(digest).toMatch(/^[0-9a-f]{64}$/);
    // Deterministic — same input always gives the same output.
    expect(sha256Hex(buf)).toBe(digest);
    // Different inputs produce different digests.
    expect(sha256Hex(Buffer.from("different", "utf-8"))).not.toBe(digest);
  });
});

// ── integrity re-verification (#697) ─────────────────────────────────────────

describe("ArchivalService.verifyArchivedPartitions (#697)", () => {
  let service: ArchivalService;
  let s3Store: Map<string, Buffer>;
  let s3List: jest.Mock;
  let metrics: { increment: jest.Mock };
  let loggerError: jest.SpyInstance;

  const DATES = ["2026-01-01", "2026-01-02", "2026-01-03", "2026-01-04", "2026-01-05"];

  function makeManifest(date: string, files: Array<{ key: string; body: Buffer }>) {
    return {
      date,
      createdAt: new Date().toISOString(),
      totalIntentRows: files.length,
      totalAuditRows: 0,
      files: files.map((f) => ({
        key: f.key,
        rows: 1,
        bytes: f.body.length,
        sha256: sha256Hex(f.body),
      })),
    };
  }

  function seedPartition(date: string, body = Buffer.from(`parquet-${date}`)) {
    const key = `date=${date}/part-0000.parquet`;
    s3Store.set(key, body);
    const manifest = makeManifest(date, [{ key, body }]);
    s3Store.set(`date=${date}/manifest.json`, Buffer.from(JSON.stringify(manifest)));
    return { key, manifest };
  }

  beforeEach(() => {
    s3Store = new Map();
    s3List = jest.fn().mockImplementation(async (prefix: string) => {
      // Return every manifest key under the prefix, mirroring ListObjectsV2.
      return Array.from(s3Store.keys())
        .filter((k) => k.startsWith(prefix) && k.endsWith("manifest.json"))
        .sort();
    });

    metrics = { increment: jest.fn() };

    const mockPrisma = {} as unknown as PrismaService;
    service = new ArchivalService(mockPrisma, makeConfig(), metrics as never);

    const s3Client = (service as unknown as {
      s3: { get: jest.Mock; list: jest.Mock };
    }).s3;
    s3Client.get = jest.fn().mockImplementation(async (key: string) => {
      const data = s3Store.get(key);
      if (!data) {
        const err = new Error(`NoSuchKey: ${key}`);
        (err as Error & { name: string }).name = "NoSuchKey";
        throw err;
      }
      return data;
    });
    s3Client.list = s3List;

    loggerError = jest.spyOn(
      (service as unknown as { logger: { error: (...a: unknown[]) => void } }).logger,
      "error",
    ).mockImplementation(() => undefined);
  });

  afterEach(() => {
    jest.restoreAllMocks();
  });

  it("happy path: verifies every manifest and reports no failures", async () => {
    for (const d of DATES) seedPartition(d);

    const result = await service.verifyArchivedPartitions({ limit: 10 });

    expect(result.checked).toBe(DATES.length);
    expect(result.failures).toEqual([]);
    expect(metrics.increment).not.toHaveBeenCalledWith(
      expect.stringMatching(/integrity/i),
      expect.anything(),
    );
    expect(loggerError).not.toHaveBeenCalled();
  });

  it("detects a corrupted file (checksum mismatch) and increments a metric", async () => {
    seedPartition("2026-01-01");
    const { key } = seedPartition("2026-01-02");
    // Overwrite the parquet body so the recorded SHA-256 no longer matches.
    s3Store.set(key, Buffer.from("bit-rot"));

    const result = await service.verifyArchivedPartitions({ limit: 10 });

    expect(result.checked).toBe(2);
    expect(result.failures).toHaveLength(1);
    expect(result.failures[0]).toMatchObject({
      date: "2026-01-02",
      key,
      reason: "checksum_mismatch",
    });
    expect(metrics.increment).toHaveBeenCalledWith(
      "archival_integrity_failure_total",
      expect.objectContaining({ reason: "checksum_mismatch" }),
    );
    expect(loggerError).toHaveBeenCalled();
  });

  it("detects a missing file (S3 get throws) and increments a metric", async () => {
    seedPartition("2026-01-01");
    const { key } = seedPartition("2026-01-02");
    s3Store.delete(key);

    const result = await service.verifyArchivedPartitions({ limit: 10 });

    expect(result.checked).toBe(2);
    expect(result.failures).toHaveLength(1);
    expect(result.failures[0]).toMatchObject({
      date: "2026-01-02",
      key,
      reason: "missing_file",
    });
    expect(metrics.increment).toHaveBeenCalledWith(
      "archival_integrity_failure_total",
      expect.objectContaining({ reason: "missing_file" }),
    );
    expect(loggerError).toHaveBeenCalled();
  });

  it("detects a size mismatch even when the digest happens to match", async () => {
    const { key, manifest } = seedPartition("2026-01-01");
    manifest.files[0].bytes = manifest.files[0].bytes + 1;
    s3Store.set("date=2026-01-01/manifest.json", Buffer.from(JSON.stringify(manifest)));

    const result = await service.verifyArchivedPartitions({ limit: 10 });

    expect(result.failures).toHaveLength(1);
    expect(result.failures[0]).toMatchObject({ key, reason: "size_mismatch" });
  });

  it("only verifies `limit` partitions per run and advances the rotation cursor", async () => {
    for (const d of DATES) seedPartition(d);

    const first = await service.verifyArchivedPartitions({ limit: 2 });
    expect(first.checked).toBe(2);
    expect(first.checkedDates).toEqual(["2026-01-01", "2026-01-02"]);

    const second = await service.verifyArchivedPartitions({ limit: 2 });
    expect(second.checked).toBe(2);
    expect(second.checkedDates).toEqual(["2026-01-03", "2026-01-04"]);

    const third = await service.verifyArchivedPartitions({ limit: 2 });
    expect(third.checked).toBe(1);
    expect(third.checkedDates).toEqual(["2026-01-05"]);

    // Wraps back around to the oldest partition.
    const fourth = await service.verifyArchivedPartitions({ limit: 2 });
    expect(fourth.checkedDates).toEqual(["2026-01-01", "2026-01-02"]);
  });

  it("returns an empty result when no partitions have been archived", async () => {
    const result = await service.verifyArchivedPartitions({ limit: 5 });
    expect(result.checked).toBe(0);
    expect(result.failures).toEqual([]);
    expect(s3List).toHaveBeenCalled();
  });
});
