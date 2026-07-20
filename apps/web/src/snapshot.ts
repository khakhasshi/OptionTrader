/**
 * MarketSnapshot contract + strict runtime parser for the Cockpit.
 *
 * Mirrors market_snapshot.json. The snapshot is produced by Rust Market Core
 * and proxied by the Python BFF; the Cockpit only ever renders what the BFF
 * returns (no fixture constants live here). If the payload is missing a
 * required field, has a wrong type, or reports data_health != HEALTHY, the
 * snapshot is treated as unavailable/STALE and not shown as live.
 */
import type { DataHealth } from "./health";

const DATA_HEALTH: readonly DataHealth[] = [
  "HEALTHY",
  "DEGRADED",
  "STALE",
  "DISCONNECTED",
  "RECONCILING",
];

export interface MarketSnapshot {
  schema_version: string;
  snapshot_id: string;
  occurred_at_utc: string;
  symbol: string;
  price: string;
  open: string;
  vwap: string;
  sequence_number: number;
  data_health: DataHealth;
}

function isRecord(v: unknown): v is Record<string, unknown> {
  return typeof v === "object" && v !== null && !Array.isArray(v);
}

/**
 * Strictly parse an untrusted /market/snapshot body. Returns null on any
 * missing required field, wrong type, or unknown data_health enum. A STALE (or
 * otherwise non-HEALTHY) but well-formed snapshot parses successfully — the
 * caller decides how to render its data_health; the fail-closed
 * SnapshotUnavailable body from the BFF simply lacks required fields and yields
 * null here.
 */
export function parseMarketSnapshot(raw: unknown): MarketSnapshot | null {
  if (!isRecord(raw)) return null;
  const {
    snapshot_id,
    occurred_at_utc,
    symbol,
    price,
    open,
    vwap,
    sequence_number,
    data_health,
  } = raw;
  if (typeof snapshot_id !== "string") return null;
  if (typeof occurred_at_utc !== "string") return null;
  if (typeof symbol !== "string") return null;
  if (typeof price !== "string") return null;
  if (typeof open !== "string") return null;
  if (typeof vwap !== "string") return null;
  if (typeof sequence_number !== "number" || !Number.isFinite(sequence_number)) return null;
  if (typeof data_health !== "string" || !DATA_HEALTH.includes(data_health as DataHealth))
    return null;
  return {
    schema_version: typeof raw.schema_version === "string" ? raw.schema_version : "1.0",
    snapshot_id,
    occurred_at_utc,
    symbol,
    price,
    open,
    vwap,
    sequence_number,
    data_health: data_health as DataHealth,
  };
}

/** A snapshot is live/usable only when well-formed AND data_health is HEALTHY. */
export function isSnapshotLive(snapshot: MarketSnapshot | null): boolean {
  return snapshot !== null && snapshot.data_health === "HEALTHY";
}
