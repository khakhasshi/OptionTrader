/**
 * CockpitState contract + strict runtime parser for the real-time cockpit.
 *
 * Mirrors packages/contracts/jsonschema/cockpit_state.json. Frames arrive over
 * the WebSocket stream from the Application Service, one per Rust MarketTick.
 * The Cockpit renders frames verbatim; it does NOT re-derive trading permission.
 *
 * Fail closed: any missing required field, wrong type, or unknown enum makes
 * parseCockpitState return null -> the caller treats it as no trustworthy state
 * and shows No Trade. new_position_allowed here is the DATA-dimension gate; the
 * Cockpit still ANDs it with the broker-dimension /core/health gate.
 */
import type { DataHealth } from "./health";
import { parseMarketSnapshot, type MarketSnapshot } from "./snapshot";

export type Connection = "LIVE" | "STALE" | "DISCONNECTED";
const CONNECTION: readonly Connection[] = ["LIVE", "STALE", "DISCONNECTED"];

export type RegimeKind = "Trend" | "Range" | "Event" | "Chaos" | "NoTrade";
const REGIME: readonly RegimeKind[] = ["Trend", "Range", "Event", "Chaos", "NoTrade"];

export type StrategyKind = "LongGamma" | "ShortPremium" | "EventVolCrush" | "NoTrade";
const STRATEGY: readonly StrategyKind[] = ["LongGamma", "ShortPremium", "EventVolCrush", "NoTrade"];

export interface RegimeView {
  regime: RegimeKind;
  trend_score: number;
  range_score: number;
}

export interface VolView {
  iv_hv_state: string;
  interpretation: string;
  realized_move: number;
}

export interface SignalView {
  strategy: StrategyKind;
  regime: RegimeKind;
  initial_risk_status: string;
  reason: string[];
  rule_version: string;
}

export interface CockpitState {
  schema_version: string;
  seq: number;
  session_id: string;
  server_time_utc: string;
  connection: Connection;
  new_position_allowed: boolean;
  snapshot: MarketSnapshot | null;
  regime: RegimeView | null;
  vol: VolView | null;
  signal: SignalView | null;
  risk_flags: string[];
}

function isRecord(v: unknown): v is Record<string, unknown> {
  return typeof v === "object" && v !== null && !Array.isArray(v);
}

function isStringArray(v: unknown): v is string[] {
  return Array.isArray(v) && v.every((x) => typeof x === "string");
}

function parseRegime(raw: unknown): RegimeView | null {
  if (raw === null || raw === undefined) return null;
  if (!isRecord(raw)) return null;
  const { regime, trend_score, range_score } = raw;
  if (typeof regime !== "string" || !REGIME.includes(regime as RegimeKind)) return null;
  if (typeof trend_score !== "number" || typeof range_score !== "number") return null;
  return { regime: regime as RegimeKind, trend_score, range_score };
}

function parseVol(raw: unknown): VolView | null {
  if (raw === null || raw === undefined) return null;
  if (!isRecord(raw)) return null;
  const { iv_hv_state, interpretation, realized_move } = raw;
  if (typeof iv_hv_state !== "string" || typeof interpretation !== "string") return null;
  if (typeof realized_move !== "number") return null;
  return { iv_hv_state, interpretation, realized_move };
}

function parseSignal(raw: unknown): SignalView | null {
  if (raw === null || raw === undefined) return null;
  if (!isRecord(raw)) return null;
  const { strategy, regime, initial_risk_status, reason, rule_version } = raw;
  if (typeof strategy !== "string" || !STRATEGY.includes(strategy as StrategyKind)) return null;
  if (typeof regime !== "string" || !REGIME.includes(regime as RegimeKind)) return null;
  if (typeof initial_risk_status !== "string") return null;
  if (!isStringArray(reason)) return null;
  if (typeof rule_version !== "string") return null;
  return {
    strategy: strategy as StrategyKind,
    regime: regime as RegimeKind,
    initial_risk_status,
    reason,
    rule_version,
  };
}

/**
 * Strictly parse an untrusted WebSocket frame into CockpitState. Returns null on
 * any missing/invalid required field. Optional derivations (snapshot/regime/vol/
 * signal) parse to null individually without failing the whole frame — a
 * fail-closed DISCONNECTED frame legitimately carries all-null derivations.
 */
export function parseCockpitState(raw: unknown): CockpitState | null {
  if (!isRecord(raw)) return null;
  if (raw.schema_version !== "1.0") return null;
  const { seq, session_id, server_time_utc, connection, new_position_allowed, risk_flags } = raw;
  if (typeof seq !== "number" || !Number.isInteger(seq) || seq < 0) return null;
  if (typeof session_id !== "string") return null;
  if (typeof server_time_utc !== "string") return null;
  if (typeof connection !== "string" || !CONNECTION.includes(connection as Connection)) return null;
  if (typeof new_position_allowed !== "boolean") return null;
  if (!isStringArray(risk_flags)) return null;
  return {
    schema_version: "1.0",
    seq,
    session_id,
    server_time_utc,
    connection: connection as Connection,
    new_position_allowed,
    snapshot: raw.snapshot == null ? null : parseMarketSnapshot(raw.snapshot),
    regime: parseRegime(raw.regime),
    vol: parseVol(raw.vol),
    signal: parseSignal(raw.signal),
    risk_flags,
  };
}

export interface CockpitGateInput {
  /** Latest parsed frame, or null (never received / unparseable). */
  frame: CockpitState | null;
  /** Broker-dimension gate from /core/health (canOpenNewPosition). */
  brokerAllowed: boolean;
}

/**
 * Final fail-closed trading gate for the real-time cockpit. Requires BOTH the
 * data-dimension frame (LIVE + new_position_allowed) AND the broker-dimension
 * /core/health gate. Any missing/degraded/contradictory signal -> No Trade.
 */
export function cockpitCanTrade({ frame, brokerAllowed }: CockpitGateInput): boolean {
  if (!frame) return false;
  return (
    frame.connection === "LIVE" &&
    frame.new_position_allowed === true &&
    (frame.snapshot?.data_health ?? "STALE") === "HEALTHY" &&
    brokerAllowed
  );
}

/** DataHealth of the frame's snapshot, or STALE when absent. */
export function frameDataHealth(frame: CockpitState | null): DataHealth {
  return frame?.snapshot?.data_health ?? "STALE";
}
