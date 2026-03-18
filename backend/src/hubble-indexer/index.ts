/**
 * HubbleIndexer — Soroban contract event backfill + live sync worker.
 *
 * Architecture
 * ────────────
 * 1. On startup, reads per-contract ledger cursors from the `indexer_cursors`
 *    Postgres table.
 * 2. Runs a backfill pass: pages through `getEvents` from the earliest known
 *    cursor up to the chain tip, writing deduped rows into event tables.
 * 3. After backfill completes, switches to live-polling mode (every
 *    LIVE_POLL_INTERVAL_MS), continuing from the latest ledger.
 * 4. Cursor state is flushed to Postgres after every batch so the worker can
 *    resume safely after a restart.
 *
 * Deduplication
 * ─────────────
 * Every event write uses `createMany({ skipDuplicates: true })` which relies
 * on the `@@unique([txHash, eventIndex])` constraints in schema.prisma.
 *
 * Relation to the legacy EventListenerService
 * ────────────────────────────────────────────
 * The legacy service (`event-listener/index.ts`) continues to run alongside
 * this indexer for business-logic side-effects (event bus, withdrawal queue).
 * This indexer exclusively owns the analytics event tables.
 */

import { PrismaClient } from "@prisma/client";
import { config } from "../config/index.js";
import { parseEvent, persistEvents } from "./processor.js";
import type { GetEventsResponse, RawSorobanEvent } from "./types.js";

// ─── Constants ───────────────────────────────────────────────────────────────

/** Maximum number of events the RPC returns per page. */
const PAGE_LIMIT = 200;

/** Milliseconds between live-poll ticks. */
const LIVE_POLL_INTERVAL_MS = 10_000;

/** Milliseconds to wait before retrying after an RPC/network error. */
const RETRY_DELAY_BASE_MS = 2_000;

/** Maximum number of consecutive errors before the indexer backs off further. */
const MAX_CONSECUTIVE_ERRORS = 5;

/** All contract IDs monitored by this indexer. */
const CONTRACT_IDS = [
  config.contracts.stakingContractId,
  config.contracts.lendingContractId,
  config.contracts.lpPoolContractId,
  config.contracts.governanceContractId,
  config.contracts.sxlmTokenContractId,
].filter(Boolean);

// ─── HubbleIndexer ───────────────────────────────────────────────────────────

export class HubbleIndexer {
  private prisma: PrismaClient;
  private pollTimer: ReturnType<typeof setTimeout> | null = null;
  private running = false;

  /** Highest ledger observed across all contracts during the current run. */
  private globalLastLedger = 0;

  /** Per-contract ledger cursor (contract ID → last fully-processed ledger). */
  private cursors: Map<string, number> = new Map();

  private consecutiveErrors = 0;

  constructor(prisma: PrismaClient) {
    this.prisma = prisma;
  }

  // ─── Lifecycle ──────────────────────────────────────────────────────────────

  async initialize(): Promise<void> {
    console.log("[HubbleIndexer] Initializing...");
    await this.loadCursors();
    this.running = true;

    // Run backfill asynchronously so initialize() returns promptly.
    this.runBackfillThenLive().catch((err) => {
      console.error("[HubbleIndexer] Fatal error in indexer loop:", err);
    });

    console.log("[HubbleIndexer] Started (backfill running in background)");
  }

  async shutdown(): Promise<void> {
    this.running = false;
    if (this.pollTimer) {
      clearTimeout(this.pollTimer);
      this.pollTimer = null;
    }
    console.log("[HubbleIndexer] Shut down");
  }

  // ─── Cursor management ──────────────────────────────────────────────────────

  private async loadCursors(): Promise<void> {
    const rows = await this.prisma.indexerCursor.findMany();
    for (const row of rows) {
      this.cursors.set(row.contractId, row.lastLedger);
    }

    this.globalLastLedger = rows.reduce(
      (max, r) => Math.max(max, r.lastLedger),
      0
    );

    console.log(
      `[HubbleIndexer] Loaded ${rows.length} cursors, ` +
        `globalLastLedger=${this.globalLastLedger}`
    );
  }

  /**
   * Persist the current per-contract cursor values to Postgres.
   * Uses upsert so the first run also creates the rows.
   */
  private async flushCursors(): Promise<void> {
    const ops = Array.from(this.cursors.entries()).map(
      ([contractId, lastLedger]) =>
        this.prisma.indexerCursor.upsert({
          where: { contractId },
          create: { contractId, lastLedger },
          update: { lastLedger },
        })
    );
    await Promise.all(ops);
  }

  // ─── Core indexing logic ────────────────────────────────────────────────────

  private async runBackfillThenLive(): Promise<void> {
    // ── Backfill ──────────────────────────────────────────────────────────────
    console.log(
      `[HubbleIndexer] Starting backfill from ledger ${this.globalLastLedger}`
    );
    await this.indexFromLedger(this.globalLastLedger || undefined);
    console.log("[HubbleIndexer] Backfill complete, switching to live mode");

    // ── Live polling ──────────────────────────────────────────────────────────
    if (this.running) {
      this.scheduleLivePoll();
    }
  }

  private scheduleLivePoll(): void {
    if (!this.running) return;
    this.pollTimer = setTimeout(async () => {
      try {
        await this.indexFromLedger(this.globalLastLedger || undefined);
        this.consecutiveErrors = 0;
      } catch (err) {
        this.consecutiveErrors++;
        const delay =
          RETRY_DELAY_BASE_MS *
          Math.min(Math.pow(2, this.consecutiveErrors - 1), 32);
        console.error(
          `[HubbleIndexer] Live poll error (attempt ${this.consecutiveErrors}), ` +
            `retrying in ${delay}ms:`,
          err
        );
        await sleep(delay);
      } finally {
        if (this.running) {
          // Re-schedule with the normal interval after any live tick.
          this.pollTimer = setTimeout(
            () => this.scheduleLivePoll(),
            LIVE_POLL_INTERVAL_MS
          );
        }
      }
    }, LIVE_POLL_INTERVAL_MS);
  }

  /**
   * Fetch and persist all events starting from `startLedger` (inclusive),
   * paginating until the chain tip.
   */
  private async indexFromLedger(startLedger?: number): Promise<void> {
    let cursor: string | undefined;

    // The RPC requires startLedger OR cursor — not both.
    // Use startLedger for the first page; use cursor for subsequent pages.
    let isFirstPage = true;

    while (this.running) {
      const response = await this.fetchEventPage(
        isFirstPage ? startLedger : undefined,
        isFirstPage ? undefined : cursor
      );

      if (!response.result) {
        if (response.error) {
          throw new Error(
            `[HubbleIndexer] RPC error ${response.error.code}: ${response.error.message}`
          );
        }
        break;
      }

      const { events, latestLedger, cursor: nextCursor } = response.result;

      if (events.length > 0) {
        await this.processBatch(events);
      }

      // Advance global cursor to the chain tip even if there were no events.
      if (latestLedger > this.globalLastLedger) {
        this.globalLastLedger = latestLedger;
      }

      // No more pages or we've caught up to the tip.
      if (!nextCursor || events.length < PAGE_LIMIT) {
        break;
      }

      cursor = nextCursor;
      isFirstPage = false;
    }

    // Ensure contracts with no events yet still have a cursor row.
    for (const id of CONTRACT_IDS) {
      if (!this.cursors.has(id)) {
        this.cursors.set(id, this.globalLastLedger);
      }
    }

    await this.flushCursors();
  }

  // ─── Batch processing ───────────────────────────────────────────────────────

  private async processBatch(events: RawSorobanEvent[]): Promise<void> {
    const parsed = events
      .filter((e) => e.inSuccessfulContractCall !== false)
      .map(parseEvent)
      .filter((e): e is NonNullable<typeof e> => e !== null);

    if (parsed.length > 0) {
      await persistEvents(this.prisma, parsed);

      console.log(
        `[HubbleIndexer] Persisted ${parsed.length}/${events.length} events ` +
          `(ledgers ${events[0]?.ledger}–${events[events.length - 1]?.ledger})`
      );
    }

    // Update per-contract cursors.
    for (const event of events) {
      const prev = this.cursors.get(event.contractId) ?? 0;
      if (event.ledger > prev) {
        this.cursors.set(event.contractId, event.ledger);
      }
      if (event.ledger > this.globalLastLedger) {
        this.globalLastLedger = event.ledger;
      }
    }
  }

  // ─── RPC fetch ───────────────────────────────────────────────────────────────

  /**
   * Fetch a page of Soroban contract events from the RPC.
   *
   * @param startLedger - First ledger to include (use for initial/backfill requests).
   * @param cursor      - Pagination cursor from the previous response.
   */
  private async fetchEventPage(
    startLedger?: number,
    cursor?: string
  ): Promise<GetEventsResponse> {
    const pagination: Record<string, unknown> = { limit: PAGE_LIMIT };
    if (cursor) {
      pagination["cursor"] = cursor;
    }

    const params: Record<string, unknown> = {
      filters: [
        {
          type: "contract",
          contractIds: CONTRACT_IDS,
        },
      ],
      pagination,
    };

    // startLedger and cursor are mutually exclusive in the Soroban RPC spec.
    if (startLedger !== undefined && !cursor) {
      params["startLedger"] = startLedger;
    }

    const response = await fetch(config.stellar.rpcUrl, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        jsonrpc: "2.0",
        id: 1,
        method: "getEvents",
        params,
      }),
    });

    if (!response.ok) {
      throw new Error(
        `[HubbleIndexer] HTTP ${response.status} from RPC endpoint`
      );
    }

    return response.json() as Promise<GetEventsResponse>;
  }
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
