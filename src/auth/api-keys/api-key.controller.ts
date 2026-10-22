import {
  Body,
  Controller,
  Get,
  Headers,
  Param,
  Post,
  Query,
  UnauthorizedException,
  UseGuards,
} from "@nestjs/common";
import {
  ApiCreatedResponse,
  ApiHeader,
  ApiOkResponse,
  ApiOperation,
  ApiQuery,
  ApiTags,
  ApiUnauthorizedResponse,
} from "@nestjs/swagger";
import { AdminGuard, RequireAdminRole } from "../../admin/admin.guard";
import { extractApiKey } from "../rate-limit/tiered-throttle.guard";
import { ApiKeyService } from "./api-key.service";
import { CreateApiKeyDto } from "./dto/create-api-key.dto";
import { RotateApiKeyDto } from "./dto/rotate-api-key.dto";

/**
 * Admin endpoints for API key lifecycle (issue #441).
 *
 * Key creation and rotation are privileged operations — they require an admin
 * key (`x-admin-key`). The plaintext secret is returned exactly once at
 * creation/rotation and is never persisted or logged.
 */
@ApiTags("admin")
@Controller({ path: "admin/api-keys", version: "1" })
@UseGuards(AdminGuard)
@RequireAdminRole("admin")
export class ApiKeyController {
  constructor(private readonly apiKeys: ApiKeyService) {}

  @Post()
  @ApiOperation({
    summary: "Create a new API key",
    description:
      "Creates an API key with the given tier. The plaintext secret is returned " +
      "ONCE in `plaintext` — it cannot be recovered later because only its " +
      "SHA-256 hash is stored.",
  })
  @ApiCreatedResponse({ description: "Key created (plaintext shown once)" })
  @ApiUnauthorizedResponse({ description: "Missing or invalid admin key" })
  async create(@Body() dto: CreateApiKeyDto) {
    const { record, plaintext } = await this.apiKeys.createKey({
      tier: dto.tier,
      owner: dto.owner,
      scopes: dto.scopes,
      expiresAt: dto.expiresAt ?? null,
    });
    return {
      id: record.id,
      keyPrefix: record.keyPrefix,
      tier: record.tier,
      owner: record.owner,
      scopes: record.scopes,
      createdAt: record.createdAt,
      expiresAt: record.expiresAt,
      plaintext,
    };
  }

  @Post(":id/rotate")
  @ApiOperation({
    summary: "Rotate an API key",
    description:
      "Revokes the key with the given id and issues a replacement. The new " +
      "plaintext secret is returned ONCE. The old key stops working immediately.",
  })
  @ApiOkResponse({ description: "Key rotated (new plaintext shown once)" })
  @ApiUnauthorizedResponse({ description: "Missing or invalid admin key" })
  async rotate(@Param("id") id: string, @Body() dto: RotateApiKeyDto) {
    const current = await this.apiKeys.getKey(id);
    if (!current) {
      await this.apiKeys.revokeKey(id);
      throw new Error(`API key not found: ${id}`);
    }
    await this.apiKeys.revokeKey(id);
    const { record, plaintext } = await this.apiKeys.createKey({
      tier: current.tier,
      owner: current.owner,
      scopes: dto.scopes ?? current.scopes ?? [],
      expiresAt: current.expiresAt ?? null,
    });
    return {
      id: record.id,
      keyPrefix: record.keyPrefix,
      tier: record.tier,
      owner: record.owner,
      scopes: record.scopes,
      createdAt: record.createdAt,
      expiresAt: record.expiresAt,
      plaintext,
    };
  }

  @Post(":id/revoke")
  @ApiOperation({
    summary: "Revoke an API key",
    description: "Instantly revokes the key. Revoked keys cannot authenticate.",
  })
  @ApiOkResponse({ description: "Key revoked" })
  @ApiUnauthorizedResponse({ description: "Missing or invalid admin key" })
  async revoke(@Param("id") id: string) {
    await this.apiKeys.revokeKey(id);
    return { revoked: true, id };
  }

  @Get()
  @ApiOperation({
    summary: "List API keys",
    description:
      "Lists key metadata (never plaintext). Filter by owner with ?owner=.",
  })
  @ApiQuery({ name: "owner", required: false })
  @ApiOkResponse({ description: "Key metadata list" })
  @ApiUnauthorizedResponse({ description: "Missing or invalid admin key" })
  async list(@Query("owner") owner?: string) {
    const keys = owner
      ? await this.apiKeys.listKeys(owner)
      : await this.apiKeys.listKeys("");
    return {
      keys: keys.map((k) => ({
        id: k.id,
        keyPrefix: k.keyPrefix,
        tier: k.tier,
        owner: k.owner,
        scopes: k.scopes,
        createdAt: k.createdAt,
        revokedAt: k.revokedAt,
        expiresAt: k.expiresAt,
        lastUsedAt: k.lastUsedAt,
      })),
      count: keys.length,
    };
  }
}

/**
 * Self-service API key introspection (issue #700).
 *
 * Authenticated by the key itself (`Authorization: Bearer` or `x-api-key`),
 * using the same header extraction as `TieredThrottleGuard`. Deliberately NOT
 * behind `AdminGuard`: a key's owner may inspect their own key's metadata.
 *
 * Missing, revoked, expired and nonexistent keys all collapse to a 401 so the
 * endpoint never discloses whether a presented prefix exists.
 */
@ApiTags("api-keys")
@Controller({ path: "api-keys", version: "1" })
export class ApiKeySelfController {
  constructor(private readonly apiKeys: ApiKeyService) {}

  @Get("me")
  @ApiOperation({
    summary: "Introspect the calling API key",
    description:
      "Returns metadata for the API key used to authenticate this request: " +
      "tier, scopes, creation/expiry timestamps, last-used timestamp and key " +
      "prefix. Never returns the plaintext secret or its hash. Revoked, " +
      "expired and unknown keys all yield 401.",
  })
  @ApiHeader({
    name: "Authorization",
    required: false,
    description: "Bearer <api key> (alternative to x-api-key)",
  })
  @ApiHeader({
    name: "x-api-key",
    required: false,
    description: "API key (alternative to Authorization: Bearer)",
  })
  @ApiOkResponse({ description: "Metadata for the calling key" })
  @ApiUnauthorizedResponse({
    description: "Missing, invalid, revoked or expired API key",
  })
  async me(
    @Headers("authorization") authorization?: string,
    @Headers("x-api-key") xApiKey?: string,
  ) {
    const presented = extractApiKey(authorization, xApiKey);
    if (!presented) {
      throw new UnauthorizedException("API key required");
    }
    const record = await this.apiKeys.resolveKey(presented);
    if (!record) {
      throw new UnauthorizedException("Invalid API key");
    }
    return {
      tier: record.tier,
      scopes: record.scopes,
      createdAt: record.createdAt,
      expiresAt: record.expiresAt,
      lastUsedAt: record.lastUsedAt,
      keyPrefix: record.keyPrefix,
    };
  }
}
