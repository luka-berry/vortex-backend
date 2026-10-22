import { Controller, Get, UseGuards } from "@nestjs/common";
import { ApiOperation, ApiTags } from "@nestjs/swagger";
import { DatasetsService } from "./datasets.service";
import { ApiKeyScopeGuard } from "../auth/api-keys/api-key-scope.guard";
import { RequireApiKeyScope } from "../auth/api-keys/require-api-key-scope.decorator";

@ApiTags("datasets")
@Controller({ path: "datasets", version: "1" })
@UseGuards(ApiKeyScopeGuard)
export class DatasetsController {
  constructor(private readonly datasetsService: DatasetsService) {}

  @Get()
  @RequireApiKeyScope("datasets:read")
  @ApiOperation({ summary: "List published dataset dates, revisions, and schemas" })
  list() {
    return this.datasetsService.listDatasets();
  }

  @Get("schemas")
  @RequireApiKeyScope("datasets:read")
  @ApiOperation({ summary: "List the versioned schemas for every dataset kind" })
  schemas() {
    return this.datasetsService.listSchemas();
  }
}