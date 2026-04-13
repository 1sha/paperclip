import { randomUUID } from "node:crypto";
import { eq } from "drizzle-orm";
import { afterAll, afterEach, beforeAll, describe, expect, it, vi } from "vitest";
import {
  agents,
  agentRuntimeState,
  agentTaskSessions,
  agentWakeupRequests,
  companies,
  createDb,
  heartbeatRunEvents,
  heartbeatRuns,
  issueComments,
  issues,
} from "@paperclipai/db";
import {
  getEmbeddedPostgresTestSupport,
  startEmbeddedPostgresTestDatabase,
} from "./helpers/embedded-postgres.js";

const adapterExecute = vi.hoisted(() => vi.fn());
const runningProcesses = vi.hoisted(() => new Map());
const mockTelemetryClient = vi.hoisted(() => ({ track: vi.fn() }));

vi.mock("../adapters/index.js", () => ({
  runningProcesses,
  getServerAdapter: () => ({
    execute: adapterExecute,
    supportsLocalAgentJwt: false,
    sessionCodec: null,
  }),
}));

vi.mock("../telemetry.js", () => ({
  getTelemetryClient: () => mockTelemetryClient,
}));

import { heartbeatService } from "../services/heartbeat.ts";

const embeddedPostgresSupport = await getEmbeddedPostgresTestSupport();
const describeEmbeddedPostgres = embeddedPostgresSupport.supported ? describe : describe.skip;

if (!embeddedPostgresSupport.supported) {
  console.warn(
    `Skipping embedded Postgres gateway retry tests on this host: ${embeddedPostgresSupport.reason ?? "unsupported environment"}`,
  );
}

describeEmbeddedPostgres("heartbeat gateway transport retry", () => {
  let db!: ReturnType<typeof createDb>;
  let tempDb: Awaited<ReturnType<typeof startEmbeddedPostgresTestDatabase>> | null = null;

  beforeAll(async () => {
    tempDb = await startEmbeddedPostgresTestDatabase("paperclip-heartbeat-gateway-retry-");
    db = createDb(tempDb.connectionString);
  }, 20_000);

  afterEach(async () => {
    vi.clearAllMocks();
    runningProcesses.clear();
    await db.delete(issueComments);
    await db.delete(issues);
    await db.delete(heartbeatRunEvents);
    await db.delete(agentTaskSessions);
    await db.delete(heartbeatRuns);
    await db.delete(agentWakeupRequests);
    await db.delete(agentRuntimeState);
    await db.delete(agents);
  });

  afterAll(async () => {
    await tempDb?.cleanup();
  });

  async function seedFixture() {
    const companyId = randomUUID();
    const agentId = randomUUID();
    const issueId = randomUUID();
    const issuePrefix = `T${companyId.replace(/-/g, "").slice(0, 6).toUpperCase()}`;
    const now = new Date("2026-03-19T00:00:00.000Z");

    await db.insert(companies).values({
      id: companyId,
      name: "Paperclip",
      issuePrefix,
      requireBoardApprovalForNewAgents: false,
    });

    await db.insert(agents).values({
      id: agentId,
      companyId,
      name: "Gateway Retry Agent",
      role: "engineer",
      adapterType: "hermes_local",
      status: "idle",
      adapterConfig: {
        transport: "gateway_api",
        profile: "gateway-retry-agent",
        apiServerUrl: "http://127.0.0.1:8715/v1",
      },
      runtimeConfig: {
        heartbeat: {
          enabled: false,
          wakeOnDemand: true,
          maxConcurrentRuns: 1,
        },
      },
      createdAt: now,
      updatedAt: now,
    });

    await db.insert(issues).values({
      id: issueId,
      companyId,
      title: "Retry gateway transport failure",
      status: "todo",
      priority: "high",
      assigneeAgentId: agentId,
      issueNumber: 1,
      identifier: `${issuePrefix}-1`,
      createdAt: now,
      updatedAt: now,
    });

    return { companyId, agentId, issueId };
  }

  async function waitFor<T>(fn: () => Promise<T>, predicate: (value: T) => boolean, timeoutMs = 5_000) {
    const deadline = Date.now() + timeoutMs;
    while (Date.now() < deadline) {
      const value = await fn();
      if (predicate(value)) return value;
      await new Promise((resolve) => setTimeout(resolve, 25));
    }
    return fn();
  }

  it("queues and starts one retry when gateway_api transport fails during execution", async () => {
    const { agentId, issueId } = await seedFixture();
    const heartbeat = heartbeatService(db);

    let resolveSecondCall: (() => void) | null = null;
    const secondCallGate = new Promise<void>((resolve) => {
      resolveSecondCall = resolve;
    });

    adapterExecute
      .mockImplementationOnce(async () => ({
        exitCode: 1,
        signal: null,
        timedOut: false,
        errorCode: "hermes_gateway_transport_error",
        errorMessage: "Hermes gateway API transport error: fetch failed",
        resultJson: {
          transport: "gateway_api",
          endpoint: "http://127.0.0.1:8715/v1",
        },
      }))
      .mockImplementationOnce(async () => {
        await secondCallGate;
        return {
          exitCode: 0,
          signal: null,
          timedOut: false,
          summary: "retry succeeded",
          resultJson: {
            transport: "gateway_api",
            endpoint: "http://127.0.0.1:8715/v1",
            responseId: "resp_retry_ok",
          },
          sessionParams: {
            transport: "gateway_api",
            responseId: "resp_retry_ok",
          },
          sessionDisplayId: "resp_retry_ok",
        };
      });

    const initialRun = await heartbeat.wakeup(agentId, {
      source: "assignment",
      triggerDetail: "system",
      reason: "issue_assigned",
      requestedByActorType: "system",
      requestedByActorId: null,
      contextSnapshot: { issueId },
    });

    expect(initialRun).not.toBeNull();

    const runsWhileRetryActive = await waitFor(
      async () => db.select().from(heartbeatRuns).orderBy(heartbeatRuns.createdAt),
      (runs) => runs.length === 2 && runs.some((run) => run.status === "running" && run.retryOfRunId != null),
    );

    const failedRun = runsWhileRetryActive.find((run) => run.retryOfRunId == null);
    const retryRun = runsWhileRetryActive.find((run) => run.retryOfRunId != null);

    expect(failedRun?.status).toBe("failed");
    expect(failedRun?.errorCode).toBe("hermes_gateway_transport_error");
    expect(retryRun?.status).toBe("running");
    expect(retryRun?.retryOfRunId).toBe(failedRun?.id ?? null);
    expect(retryRun?.processLossRetryCount).toBe(1);
    expect(retryRun?.contextSnapshot).toMatchObject({
      issueId,
      retryReason: "gateway_transport",
      wakeReason: "gateway_transport_retry",
    });

    const issueWhileRetryActive = await db
      .select()
      .from(issues)
      .where(eq(issues.id, issueId))
      .then((rows) => rows[0] ?? null);
    expect(issueWhileRetryActive?.executionRunId).toBe(retryRun?.id ?? null);

    const runningAgent = await db
      .select()
      .from(agents)
      .where(eq(agents.id, agentId))
      .then((rows) => rows[0] ?? null);
    expect(runningAgent?.status).toBe("running");

    resolveSecondCall?.();

    await waitFor(
      async () => db.select().from(agents).where(eq(agents.id, agentId)).then((rows) => rows[0] ?? null),
      (agent) => agent?.status === "idle",
    );

    const finalRuns = await db.select().from(heartbeatRuns).orderBy(heartbeatRuns.createdAt);
    expect(finalRuns).toHaveLength(2);
    expect(finalRuns[1]?.status).toBe("succeeded");

    const finalIssue = await db
      .select()
      .from(issues)
      .where(eq(issues.id, issueId))
      .then((rows) => rows[0] ?? null);
    expect(finalIssue?.executionRunId).toBeNull();

    expect(adapterExecute).toHaveBeenCalledTimes(2);
  });
});