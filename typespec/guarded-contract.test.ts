import { expect, test } from "bun:test";
import { readFileSync } from "node:fs";
import { parse } from "yaml";

type SchemaNode = {
  $ref?: string;
  type?: string;
  enum?: string[];
  nullable?: boolean;
  required?: string[];
  properties?: Record<string, SchemaNode>;
  allOf?: { $ref: string }[];
  oneOf?: { $ref: string }[];
  items?: SchemaNode;
};

type Parameter = { $ref?: string; name?: string; required?: boolean; schema?: SchemaNode };
type Response = {
  headers?: Record<string, { required?: boolean; schema?: SchemaNode }>;
  content?: { "application/json"?: { schema?: SchemaNode } };
};
type Operation = {
  operationId: string;
  parameters?: Parameter[];
  responses: Record<string, Response>;
};
type Contract = {
  paths: Record<string, Record<string, Operation>>;
  components: { parameters: Record<string, Parameter>; schemas: Record<string, SchemaNode> };
};

// Generated emitter output has this fixed OpenAPI 3.0 shape.
const spec = parse(readFileSync(new URL("../openapi/teldrive.openapi.yaml", import.meta.url), "utf8")) as Contract;
const schemaText = readFileSync(new URL("../ui/src/api/schema.ts", import.meta.url), "utf8");

const operation = (path: string, method: string): Operation => spec.paths[path][method];
const parameter = (ref: string): Parameter => spec.components.parameters[ref.split("/").pop()!];
const schema = (name: string): SchemaNode => spec.components.schemas[name];
const headerParameterNames = (op: Operation): string[] =>
  (op.parameters ?? []).filter((p) => p.$ref).map((p) => parameter(p.$ref!).name!);

test("b3_guard_wire_routes_and_headers", () => {
  const discovery = operation("/v1/uploads/guarded", "get");
  const create = operation("/v1/uploads/guarded", "post");
  const status = operation("/v1/uploads/guarded/{uploadId}", "get");
  const part = operation("/v1/uploads/guarded/{uploadId}/parts/{partNo}", "put");
  const complete = operation("/v1/uploads/guarded/{uploadId}/complete", "post");

  expect(discovery.operationId).toBe("discoverGuardedUpload");
  expect(create.operationId).toBe("createGuardedUpload");
  expect(status.operationId).toBe("getGuardedUpload");
  expect(part.operationId).toBe("putGuardedUploadPart");
  expect(complete.operationId).toBe("completeGuardedUpload");

  expect(parameter("IdempotencyHeader").required).toBe(true);
  expect(parameter("RecoveryIncarnationHeader").name).toBe("X-TelDrive-Recovery-Incarnation");
  expect(parameter("RecoveryIncarnationHeader").required).toBe(true);

  for (const op of [create, status, part, complete]) {
    expect(headerParameterNames(op)).toContain("X-TelDrive-Recovery-Incarnation");
  }
  for (const op of [create, complete]) {
    expect(headerParameterNames(op)).toContain("Idempotency-Key");
  }

  const discoveryResponse = discovery.responses["200"];
  expect(discoveryResponse.headers?.["X-TelDrive-Recovery-Incarnation"].required).toBe(true);
  expect(discoveryResponse.headers?.["X-TelDrive-Upload-Guard"]).toBeUndefined();

  const statusResponse = status.responses["200"];
  expect(statusResponse.headers?.["X-TelDrive-Recovery-Incarnation"].required).toBe(true);
  expect(statusResponse.headers?.["X-TelDrive-Upload-Guard"].required).toBe(true);
  expect(statusResponse.headers?.["X-TelDrive-Upload-Guard"].schema?.enum).toEqual(["1"]);
});

test("b3_guard_wire_create_destination", () => {
  const body = schema("GuardedUploadCreateRequest");
  expect(body.required).toEqual(["destination", "name", "size", "modTime"]);
  expect(body.properties?.parentId).toBeUndefined();
  expect(body.properties?.conflictPolicy).toBeUndefined();
  expect(body.properties?.destination.$ref).toBe("#/components/schemas/GuardedUploadDestination");

  const destination = schema("GuardedUploadDestination");
  expect(destination.required).toEqual(["mountRoot", "parentId", "expected"]);
  expect(destination.properties?.mountRoot.nullable).toBe(true);
  expect(destination.properties?.parentId.nullable).toBe(true);

  const expected = schema("GuardedSlotExpectation");
  expect(expected.oneOf?.map((ref) => ref.$ref)).toEqual([
    "#/components/schemas/GuardedSlotAbsent",
    "#/components/schemas/GuardedSlotVersion",
  ]);
  expect(schema("GuardedSlotAbsent").properties?.kind.enum).toEqual(["absent"]);
  const version = schema("GuardedSlotVersion");
  expect(version.required).toEqual(["kind", "fileId", "generation"]);
  expect(version.properties?.kind.enum).toEqual(["version"]);
});

test("b3_guard_wire_complete_requires_proof", () => {
  const complete = operation("/v1/uploads/guarded/{uploadId}/complete", "post");
  const body = schema("GuardedUploadCompleteRequest");
  expect(body.required).toEqual(["size", "hash"]);
  expect(body.properties?.hash.$ref).toBe("#/components/schemas/FileHash");

  for (const code of ["200", "201"]) {
    expect(complete.responses[code].headers?.Etag).toBeDefined();
    expect(complete.responses[code].headers?.Location).toBeDefined();
    expect(complete.responses[code].content?.["application/json"]?.schema?.$ref).toBe(
      "#/components/schemas/GuardedFileEntry",
    );
  }

  const entry = schema("GuardedFileEntry");
  expect(entry.allOf).toEqual([{ $ref: "#/components/schemas/FileEntry" }]);
  expect(entry.properties?.parentSegments.type).toBe("array");
  expect(entry.required).toBeUndefined();
});

test("b3_guard_wire_status_receipt", () => {
  const status = operation("/v1/uploads/guarded/{uploadId}", "get");
  expect(status.responses["200"].content?.["application/json"]?.schema?.$ref).toBe(
    "#/components/schemas/GuardedUploadSession",
  );
  expect(status.responses["412"].content?.["application/json"]?.schema?.$ref).toBe(
    "#/components/schemas/ErrorEnvelope",
  );

  const session = schema("GuardedUploadSession");
  expect(session.allOf).toEqual([{ $ref: "#/components/schemas/UploadSession" }]);
  expect(session.required).toBeUndefined();

  const receipt = schema("GuardedCompletedReceipt");
  expect(receipt.required).toEqual(["admissionKey", "uploadId", "size", "hash", "result"]);
  expect(receipt.properties?.result.$ref).toBe("#/components/schemas/GuardedFileEntry");
});

test("b3_guard_skew_legacy_upload_shape_unchanged", () => {
  expect(schema("UploadCreateRequest").properties?.destination).toBeUndefined();
  expect(schema("UploadCreateRequest").properties?.parentId).toBeDefined();
  expect(schema("UploadCreateRequest").required).toEqual(["name", "size", "modTime"]);

  expect(schema("UploadCompleteRequest").required).toEqual(["size"]);
  expect(schema("UploadCompleteRequest").properties?.hash).toBeDefined();

  const legacyCreate = operation("/v1/uploads", "post");
  const legacyComplete = operation("/v1/uploads/{uploadId}/complete", "post");
  expect(legacyCreate.operationId).toBe("createUpload");
  expect(legacyComplete.operationId).toBe("completeUpload");
  expect(headerParameterNames(legacyCreate)).toEqual(["Idempotency-Key"]);
  expect(legacyCreate.responses["201"].content?.["application/json"]?.schema?.$ref).toBe(
    "#/components/schemas/UploadSession",
  );
  expect(legacyComplete.responses["200"].content?.["application/json"]?.schema?.$ref).toBe(
    "#/components/schemas/FileEntry",
  );
  expect(legacyComplete.responses["200"].headers?.["X-TelDrive-Upload-Guard"]).toBeUndefined();
});

test("b3_guard_skew_generated_client_has_guarded_routes", () => {
  for (const path of [
    "/v1/uploads/guarded",
    "/v1/uploads/guarded/{uploadId}",
    "/v1/uploads/guarded/{uploadId}/parts/{partNo}",
    "/v1/uploads/guarded/{uploadId}/complete",
  ]) {
    expect(schemaText).toContain(`"${path}"`);
  }
  expect(schemaText).toContain("GuardedUploadCreateRequest");
  expect(schemaText).toContain("GuardedCompletedReceipt");
  expect(schemaText).toContain('"/v1/uploads/{uploadId}"');
});
