import { expect, type Page, test } from "@playwright/test";
import type { components } from "../src/api/schema";
import type * as UploadStoreModule from "../src/features/uploads/store";
import { installPreferencesApi } from "./fixtures/preferences";

const block = 16 * 1024 * 1024;
const uploadId = "66666666-6666-4666-8666-666666666666";
const fileId = "22222222-2222-4222-8222-222222222222";
const now = "2026-11-16T12:00:00Z";
const result = {
  id: fileId,
  name: "current.bin",
  kind: "file",
  size: 7,
  encryption: false,
  status: "active",
  generation: 1,
  modTime: now,
  createdAt: now,
  updatedAt: now,
};
const session: components["schemas"]["UploadSession"] = {
  id: uploadId,
  name: "current.bin",
  expectedSize: 7,
  encryption: false,
  conflictPolicy: "rename",
  partSize: block,
  state: "open",
  requiresCompletionManifest: true,
  modTime: now,
  expiresAt: now,
  createdAt: now,
};

type FixtureOptions = {
  session?: Record<string, unknown>;
  conflict?: boolean;
  stored?: boolean;
  refreshFailure?: boolean;
};

async function fixture(page: Page, options: FixtureOptions = {}) {
  const puts: Buffer[] = [];
  const manifests: unknown[] = [];
  const events: string[] = [];
  const headers: Record<string, string>[] = [];
  await page.route("**/api/v1/**", async (route) => {
    const request = route.request();
    const path = new URL(request.url()).pathname;
    const method = request.method();
    if (path.includes("/public/shares/fixture-token") && !path.includes("/uploads")) {
      if (request.headers()["x-share-password"] !== "fixture-password") {
        return route.fulfill({
          status: 401,
          json: { error: { code: "unauthorized", message: "Password required" } },
        });
      }
      if (path.endsWith("/files")) {
        if (options.refreshFailure && manifests.length > 0)
          return route.fulfill({
            status: 500,
            json: { error: { code: "internal_error", message: "Listing refresh failed" } },
          });
        return route.fulfill({ json: { items: [] } });
      }
      if (path.endsWith("/access")) return route.fulfill({ status: 204 });
      return route.fulfill({
        json: {
          token: "fixture-token",
          permission: "edit",
          passwordProtected: true,
          file: { ...result, kind: "folder", name: "Shared folder" },
        },
      });
    }
    if (path.includes("/uploads")) {
      headers.push(request.headers());
      events.push(`${method} ${path}`);
      if (method === "DELETE") return route.fulfill({ status: 204 });
      if (method === "PUT") {
        expect(request.headers()["content-type"]).toBe("application/octet-stream");
        puts.push(request.postDataBuffer() ?? Buffer.alloc(0));
        if (options.conflict)
          return route.fulfill({
            status: 409,
            json: { error: { code: "upload_part_conflict", message: "Current bytes differ" } },
          });
        return route.fulfill({
          json: {
            uploadId,
            partNo: Number(path.split("/").at(-1)),
            state: "stored",
            plainSize: puts.at(-1)?.length,
            createdAt: now,
            updatedAt: now,
          },
        });
      }
      if (path.endsWith("/complete")) {
        expect(request.headers()["content-type"]).toContain("application/json");
        manifests.push(request.postDataJSON());
        return route.fulfill({ status: 201, json: result });
      }
      if (path.endsWith("/parts"))
        return route.fulfill({
          json: {
            items: options.stored
              ? [
                  {
                    uploadId,
                    partNo: 1,
                    state: "stored",
                    plainSize: Math.min(block, Number(options.session?.expectedSize ?? 7)),
                    checksum: "a".repeat(64),
                  },
                ]
              : [],
          },
        });
      if (method === "POST" && path === "/api/v1/uploads")
        expect(request.postDataJSON().encryption).toBe(true);
      return route.fulfill({
        status: 201,
        json: {
          ...session,
          ...(method === "POST" ? { expectedSize: request.postDataJSON().size } : {}),
          ...options.session,
        },
      });
    }
    return route.fulfill({ json: { items: [] } });
  });
  await installPreferencesApi(page, { uploads: { encryption: true } });
  // Public route avoids authenticated layout while the owner tests drive the real queue module.
  await page.goto("/share/fixture-token");
  return { puts, manifests, events, headers };
}

async function enqueue(page: Page, size: number, resume = false, parentId?: string) {
  await page.evaluate(
    async ({ size, resume, uploadId, parentId }) => {
      const modulePath = "/src/features/uploads/store.ts";
      // Static imports run in Node; load this module inside the browser's Vite runtime.
      const { useUploadStore } = (await import(modulePath)) as typeof UploadStoreModule;
      const bytes = new Uint8Array(size);
      for (let i = 0; i < bytes.length; i++) bytes[i] = i % 251;
      useUploadStore.getState().enqueue(
        [new File([bytes], "current.bin", { lastModified: 1 })], parentId,
      );
      if (resume) {
        const task = useUploadStore.getState().tasks.at(-1)!;
        useUploadStore.getState().patchTask(task.id, { uploadId });
      }
    },
    { size, resume, uploadId, parentId },
  );
}

async function ownerStatus(page: Page) {
  return page.evaluate(async () => {
    const modulePath = "/src/features/uploads/store.ts";
    // Static imports run in Node; load this module inside the browser's Vite runtime.
    const { useUploadStore } = (await import(modulePath)) as typeof UploadStoreModule;
    const task = useUploadStore.getState().tasks.at(-1);
    return { status: task?.status, progress: task?.progress, uploadedBytes: task?.uploadedBytes };
  });
}

for (const state of ["open", "completed"] as const) {
  test(`B1 upload owner replay current bodies from ${state} session`, async ({ page }) => {
    const proof = await fixture(page, {
      stored: true,
      session: { state, fileId, expectedSize: block + 3 },
    });
    await enqueue(page, block + 3, true);
    await expect.poll(async () => (await ownerStatus(page)).status).toBe("completed");
    expect(proof.puts.map((body) => body.length)).toEqual([block, 3]);
    const expected = Buffer.alloc(block + 3);
    for (let i = 0; i < expected.length; i++) expected[i] = i % 251;
    expect(Buffer.concat(proof.puts)).toEqual(expected);
    expect(proof.manifests).toEqual([{ size: block + 3 }]);
    expect(await ownerStatus(page)).toEqual({
      status: "completed",
      progress: 100,
      uploadedBytes: block + 3,
    });
    expect(proof.events.findIndex((event) => event.includes("/complete"))).toBeGreaterThan(
      proof.events.findIndex((event) => event.startsWith("PUT")),
    );
  });
}

test("B1 upload owner replay conflict never completes or deletes shared attempt", async ({
  page,
}) => {
  const proof = await fixture(page, { stored: true, conflict: true });
  await enqueue(page, 7, true);
  await expect.poll(async () => (await ownerStatus(page)).status).toBe("failed");
  expect(proof.puts).toEqual([Buffer.from([0, 1, 2, 3, 4, 5, 6])]);
  expect(proof.manifests).toEqual([]);
  expect(proof.events.some((event) => event.startsWith("DELETE"))).toBe(false);
});

const invalidSessions = [
  { name: "missing capability", requiresCompletionManifest: undefined },
  { name: "false capability", requiresCompletionManifest: false },
  { name: "unaligned", partSize: block + 1 },
  { name: "zero", partSize: 0 },
  { name: "negative", partSize: -block },
  { name: "fractional", partSize: block + 0.5 },
  { name: "unsafe", partSize: Number.MAX_SAFE_INTEGER + 1 },
];
for (const invalid of invalidSessions) {
  for (const publicUpload of [false, true]) {
    test(`B1 upload rejects incompatible session ${invalid.name} ${publicUpload ? "public" : "owner"}`, async ({
      page,
    }) => {
      const proof = await fixture(page, { session: invalid });
      if (publicUpload) {
        await unlock(page);
        await page.locator('input[type="file"]').setInputFiles({
          name: "current.bin",
          mimeType: "application/octet-stream",
          buffer: Buffer.from("current"),
        });
        await expect(page.getByText("Upload failed", { exact: true })).toBeVisible();
      } else {
        await enqueue(page, 7);
        await expect.poll(async () => (await ownerStatus(page)).status).toBe("failed");
      }
      expect(proof.puts).toEqual([]);
      expect(proof.manifests).toEqual([]);
    });
  }
}

for (const publicUpload of [false, true]) {
  test(`B1 upload rejects unsafe File size ${publicUpload ? "public" : "owner"}`, async ({
    page,
  }) => {
    const proof = await fixture(page);
    if (publicUpload) await unlock(page);
    await page.evaluate(() => {
      Object.defineProperty(File.prototype, "size", {
        configurable: true,
        get: () => Number.MAX_SAFE_INTEGER + 1,
      });
    });
    if (publicUpload) {
      await page.locator('input[type="file"]').setInputFiles({
        name: "current.bin",
        mimeType: "application/octet-stream",
        buffer: Buffer.from("current"),
      });
      await expect(page.getByText("Upload failed", { exact: true })).toBeVisible();
    } else {
      await enqueue(page, 7);
      await expect.poll(async () => (await ownerStatus(page)).status).toBe("failed");
    }
    expect(proof.events).toEqual([]);
    expect(proof.puts).toEqual([]);
    expect(proof.manifests).toEqual([]);
  });
}

test("B1 upload zero owner sends explicit size without parts", async ({ page }) => {
  const proof = await fixture(page);
  await enqueue(page, 0);
  await expect.poll(async () => (await ownerStatus(page)).status).toBe("completed");
  expect(proof.puts).toEqual([]);
  expect(proof.manifests).toEqual([{ size: 0 }]);
});

async function unlock(page: Page) {
  await page.locator('input[type="password"]').fill("fixture-password");
  await page.locator('button[type="submit"]').click();
  await expect(page.getByRole("button", { name: "Upload file", exact: true })).toBeVisible();
}

for (const size of [0, block + 3]) {
  test(`B1 upload public manifest preserves password and actual bytes size ${size}`, async ({
    page,
  }) => {
    const proof = await fixture(page);
    await unlock(page);
    const bytes = Buffer.alloc(size);
    for (let i = 0; i < size; i++) bytes[i] = i % 251;
    await page
      .locator('input[type="file"]')
      .setInputFiles({ name: "current.bin", mimeType: "application/octet-stream", buffer: bytes });
    await expect.poll(() => proof.manifests.length).toBe(1);
    expect(Buffer.concat(proof.puts)).toEqual(bytes);
    expect(proof.puts.map((body) => body.length)).toEqual(size ? [block, 3] : []);
    expect(proof.manifests).toEqual([{ size }]);
    expect(
      proof.headers.every((headers) => headers["x-share-password"] === "fixture-password"),
    ).toBe(true);
  });
}

test("B1 upload public manifest part conflict never completes", async ({ page }) => {
  const proof = await fixture(page, { conflict: true });
  await unlock(page);
  await page.locator('input[type="file"]').setInputFiles({
    name: "current.bin",
    mimeType: "application/octet-stream",
    buffer: Buffer.from("current"),
  });
  await expect(page.getByText("Upload failed", { exact: true })).toBeVisible();
  expect(proof.puts).toEqual([Buffer.from("current")]);
  expect(proof.manifests).toEqual([]);
});

test("B2 upload public completion survives listing refresh failure", async ({ page }) => {
  const proof = await fixture(page, { refreshFailure: true });
  await unlock(page);
  await page.locator('input[type="file"]').setInputFiles({
    name: "current.bin",
    mimeType: "application/octet-stream",
    buffer: Buffer.from("current"),
  });
  await expect(page.getByText("current.bin uploaded", { exact: true })).toBeVisible();
  await expect(page.locator("main .text-danger")).toBeVisible();
  await expect(page.getByRole("button", { name: "Upload file", exact: true })).toBeEnabled();
  expect(proof.manifests).toEqual([{ size: 7 }]);
  expect(Buffer.concat(proof.puts)).toEqual(Buffer.from("current"));
  expect(proof.events.filter((event) => event.startsWith("DELETE "))).toEqual([]);
  await expect(page.getByText("Upload failed", { exact: true })).toHaveCount(0);
});

type LifecycleProof = {
  terminal: boolean;
  lostAck: boolean;
  denied: boolean;
  creates: { key: string; body: string }[];
  folders: number;
  lookups: number;
  lookupStatus: number;
  admittedKeys: Set<string>;
  completes: unknown[];
};

async function lifecycleFixture(page: Page) {
  const options: FixtureOptions = {};
  const proof = await fixture(page, options);
  const lifecycle: LifecycleProof = {
    terminal: false,
    lostAck: false,
    denied: false,
    creates: [],
    folders: 0,
    lookups: 0,
    lookupStatus: 409,
    admittedKeys: new Set(),
    completes: [],
  };
  await page.route("**/api/v1/**", async (route) => {
    const request = route.request();
    const path = new URL(request.url()).pathname;
    if (path.endsWith("/folders")) lifecycle.folders++;
    if (!path.includes("/uploads")) return route.fallback();
    if (lifecycle.denied && path.includes("/public/"))
      return route.fulfill({
        status: 403, json: { error: { code: "forbidden", message: "Access denied" } },
      });
    const create = request.method() === "POST" && path.endsWith("/uploads");
    if (create) {
      lifecycle.creates.push({
        key: request.headers()["idempotency-key"], body: request.postData()!,
      });
      lifecycle.admittedKeys.add(request.headers()["idempotency-key"]);
      if (lifecycle.lostAck) {
        lifecycle.lostAck = false;
        lifecycle.terminal = true;
        return route.abort("failed");
      }
    }
    if (request.method() === "GET") lifecycle.lookups++;
    if (lifecycle.terminal) {
      return route.fulfill({
        status: request.method() === "GET" ? lifecycle.lookupStatus : 409,
        json: { error: { code: "upload_parent_unavailable", message: "Upload destination is unavailable" } },
      });
    }
    return route.fallback();
  });
  return { ...proof, lifecycle, options };
}

async function ownerTask(page: Page, action?: "retry" | "pause") {
  return page.evaluate(async (action) => {
    const modulePath = "/src/features/uploads/store.ts";
    // Static imports execute in Node, not the browser's Vite module realm.
    const { useUploadStore } = (await import(modulePath)) as typeof UploadStoreModule;
    const store = useUploadStore.getState();
    const task = store.tasks.at(-1)!;
    if (action) store[action](task.id);
    return task;
  }, action);
}

for (const unknown of [false, true]) {
  test(`B2 upload owner ${unknown ? "legacy unknown" : "parent failure"}`, async ({ page }) => {
    const proof = await lifecycleFixture(page);
    // First attempt sends actual File slices, then loses its destination.
    proof.lifecycle.terminal = false;
    await page.route("**/api/v1/uploads/*/complete", async (route) => {
      expect(route.request().headers()["content-type"]).toContain("application/json");
      proof.lifecycle.completes.push(route.request().postDataJSON());
      proof.lifecycle.terminal = true;
      return route.fulfill({
        status: 409,
        json: { error: { code: "upload_parent_unavailable", message: "Upload destination is unavailable" } },
      });
    });
    await enqueue(page, 7, false, unknown ? undefined : fileId);
    await expect.poll(async () => (await ownerStatus(page)).status).toBe("failed");
    const selected = await ownerTask(page);
    const creates = [...proof.lifecycle.creates];
    expect(JSON.parse(creates[0].body).parentId).toBe(unknown ? undefined : fileId);
    expect(proof.lifecycle.completes).toEqual([{ size: 7 }]);
    await ownerTask(page, "retry");
    await expect.poll(() => proof.lifecycle.lookups).toBeGreaterThan(0);
    await expect.poll(async () => (await ownerStatus(page)).status).toBe("failed");
    expect((await ownerTask(page)).uploadId).toBe(selected.uploadId);
    expect((await ownerTask(page)).error).not.toContain("original file is no longer available");
    // Missing/expired selected sessions also refuse fresh creation after restore.
    for (const status of [404, 410]) {
      const lookups = proof.lifecycle.lookups;
      proof.lifecycle.lookupStatus = status;
      await ownerTask(page, "retry");
      await expect.poll(() => proof.lifecycle.lookups).toBe(lookups + 1);
      await expect.poll(async () => (await ownerStatus(page)).status).toBe("failed");
      expect(proof.lifecycle.creates).toEqual(creates);
    }
    proof.lifecycle.lookupStatus = 409;
    expect(proof.lifecycle.creates).toEqual(creates);
    expect(proof.lifecycle.folders).toBe(0);
    expect(proof.manifests).toEqual([]);
    expect(proof.events.some((event) => event.startsWith("DELETE"))).toBe(false);
    expect(proof.lifecycle.completes).toEqual([{ size: 7 }]);
    expect(Buffer.concat(proof.puts)).toEqual(Buffer.from([0, 1, 2, 3, 4, 5, 6]));

    // Restore cannot reopen the selected loser. A lost create ACK keeps its exact admission.
    proof.lifecycle.terminal = false;
    proof.lifecycle.lostAck = true;
    await enqueue(page, 7, false, unknown ? undefined : fileId);
    await expect.poll(async () => (await ownerStatus(page)).status).toBe("failed");
    const frozen = await ownerTask(page);
    const admitted = proof.lifecycle.creates.at(-1)!;
    await ownerTask(page, "retry");
    await expect.poll(() => proof.lifecycle.creates.length).toBe(creates.length + 2);
    await expect.poll(async () => (await ownerStatus(page)).status).toBe("failed");
    expect(proof.lifecycle.creates.at(-1)).toEqual(admitted);
    expect((await ownerTask(page)).createKey).toBe(frozen.createKey);
    expect((await ownerTask(page)).createPayload).toBe(frozen.createPayload);
    expect(JSON.parse(frozen.createPayload!).parentId).toBe(unknown ? undefined : fileId);
    expect(proof.lifecycle.admittedKeys.size).toBe(2);
    expect(proof.lifecycle.folders).toBe(0);
    expect(proof.manifests).toEqual([]);

    // Cancel an actual body request without dropping the File or selected admission.
    proof.lifecycle.terminal = false;
    const partPath = "**/api/v1/uploads/*/parts/1";
    await page.route(partPath, async (route) => {
      expect(route.request().postDataBuffer()).toEqual(Buffer.from([0, 1, 2, 3, 4, 5, 6]));
      await ownerTask(page, "pause");
      await route.abort("aborted");
    });
    await enqueue(page, 7, false, unknown ? undefined : fileId);
    await expect.poll(async () => (await ownerStatus(page)).status).toBe("paused");
    const paused = await ownerTask(page);
    const beforeResume = proof.lifecycle.creates.length;
    await page.unroute(partPath);
    proof.lifecycle.terminal = true;
    await ownerTask(page, "retry");
    await expect.poll(async () => (await ownerStatus(page)).status).toBe("failed");
    expect((await ownerTask(page)).uploadId).toBe(paused.uploadId);
    expect((await ownerTask(page)).createPayload).toBe(paused.createPayload);
    expect(proof.lifecycle.creates).toHaveLength(beforeResume);
    expect(proof.lifecycle.completes).toEqual([{ size: 7 }]);

    // Independently requested root is new intent, not retry of either failed task.
    proof.lifecycle.terminal = false;
    await page.unroute("**/api/v1/uploads/*/complete");
    await enqueue(page, 0);
    await expect.poll(async () => (await ownerStatus(page)).status).toBe("completed");
    expect(proof.lifecycle.creates.at(-1)?.key).not.toBe(admitted.key);
    expect(proof.manifests).toEqual([{ size: 0 }]);
    // Existing completed-result replay still sends actual bytes and explicit manifest.
    proof.options.session = { state: "completed", fileId };
    await enqueue(page, 7, true);
    await expect.poll(async () => (await ownerStatus(page)).status).toBe("completed");
    expect(proof.manifests).toEqual([{ size: 0 }, { size: 7 }]);
  });

  test(`B2 upload public ${unknown ? "legacy unknown" : "parent failure"}`, async ({ page }) => {
    const proof = await lifecycleFixture(page);
    await unlock(page);
    const input = page.locator('input[type="file"]');
    const source = { name: "current.bin", mimeType: "application/octet-stream", buffer: Buffer.from("current") };
    await page.route("**/api/v1/public/shares/*/uploads/*/complete", async (route) => {
      expect(route.request().headers()["content-type"]).toContain("application/json");
      expect(route.request().headers()["x-share-password"]).toBe("fixture-password");
      proof.lifecycle.completes.push(route.request().postDataJSON());
      proof.lifecycle.terminal = true;
      return route.fulfill({
        status: 409,
        json: { error: { code: "upload_parent_unavailable", message: "Upload destination is unavailable" } },
      });
    });
    await input.setInputFiles(source);
    await expect(page.getByText("Upload failed", { exact: true })).toBeVisible();
    expect(await input.evaluate(async (element: HTMLInputElement) => {
      const file = element.files?.[0];
      if (!file) throw new Error("Public input did not retain the selected File");
      return Array.from(new Uint8Array(await file.arrayBuffer()));
    })).toEqual([...source.buffer]);
    expect(JSON.parse(proof.lifecycle.creates[0].body)).toMatchObject({
      parentId: fileId, name: source.name, size: source.buffer.length, conflictPolicy: "rename",
    });
    expect(proof.manifests).toEqual([]);
    expect(proof.lifecycle.creates).toHaveLength(1);
    expect(proof.lifecycle.completes).toEqual([{ size: 7 }]);
    expect(proof.lifecycle.folders).toBe(0);
    expect(Buffer.concat(proof.puts)).toEqual(source.buffer);
    expect(proof.headers.every((header) => header["x-share-password"] === "fixture-password")).toBe(true);
    // A later action is distinct intent; current denial never falls back to owner/root.
    proof.lifecycle.denied = true;
    const creates = proof.lifecycle.creates.length;
    await input.setInputFiles(source);
    await expect(
      page.getByText("You do not have permission to perform this action.", { exact: true }),
    ).toBeVisible();
    expect(proof.lifecycle.creates).toHaveLength(creates);
    proof.lifecycle.denied = false;
    // Lost create ACK never triggers an automatic fresh request or owner fallback.
    proof.lifecycle.terminal = false;
    proof.lifecycle.lostAck = true;
    const beforeLostAck = proof.lifecycle.creates.length;
    await input.setInputFiles(source);
    await expect.poll(() => proof.lifecycle.creates.length).toBe(beforeLostAck + 1);
    await expect(page.getByRole("button", { name: "Upload file", exact: true })).toBeEnabled();
    const lostAdmission = proof.lifecycle.creates.at(-1)!;
    expect(lostAdmission.key).not.toBe(proof.lifecycle.creates[0].key);
    expect(proof.manifests).toEqual([]);
    expect(proof.lifecycle.creates).toHaveLength(beforeLostAck + 1);
    expect(proof.lifecycle.folders).toBe(0);
    expect(proof.lifecycle.completes).toEqual([{ size: 7 }]);
    proof.lifecycle.terminal = false;
    await page.unroute("**/api/v1/public/shares/*/uploads/*/complete");
    await input.setInputFiles({ ...source, buffer: Buffer.alloc(0) });
    await expect.poll(() => proof.manifests.length).toBe(1);
    expect(proof.lifecycle.creates.at(-1)?.key).not.toBe(proof.lifecycle.creates[0].key);
    expect(proof.manifests).toEqual([{ size: 0 }]);
  });
}
