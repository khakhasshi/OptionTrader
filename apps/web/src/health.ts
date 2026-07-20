/**
 * Shared health contract + trading gate for the Cockpit.
 *
 * Mirrors health.json#/$defs/ServiceHealth. The gate is fail-closed: a new
 * position is permitted ONLY when the core is reachable AND status is "ok" AND
 * data_health is HEALTHY AND broker_health is HEALTHY AND the book is
 * reconciled AND the Rust gateway itself set new_position_allowed=true.
 *
 * new_position_allowed is the Rust Risk & Execution Gateway's final authority.
 * The UI obeys it: we never open a position the gateway forbids, even if every
 * other field looks healthy. We ALSO recompute the surrounding conjunction so a
 * malformed or contradictory payload (e.g. new_position_allowed=true but broker
 * DISCONNECTED) can never flip the UI to ALLOWED. Any missing field, wrong
 * type, or bad enum makes parseServiceHealth return null -> No Trade.
 */
export type DataHealth = "HEALTHY" | "DEGRADED" | "STALE" | "DISCONNECTED" | "RECONCILING";
export type BrokerHealth = "HEALTHY" | "DEGRADED" | "DISCONNECTED" | "RECONCILING";
export type ServiceStatus = "ok" | "unreachable";

const DATA_HEALTH: readonly DataHealth[] = [
  "HEALTHY",
  "DEGRADED",
  "STALE",
  "DISCONNECTED",
  "RECONCILING",
];
const BROKER_HEALTH: readonly BrokerHealth[] = ["HEALTHY", "DEGRADED", "DISCONNECTED", "RECONCILING"];
const SERVICE_STATUS: readonly ServiceStatus[] = ["ok", "unreachable"];

export interface ServiceHealth {
  schema_version: string;
  status: ServiceStatus;
  service: string;
  environment?: string;
  data_health: DataHealth;
  broker_health: BrokerHealth;
  reconciled: boolean;
  /** Rust gateway's final authority. Required — never optional. */
  new_position_allowed: boolean;
}

function isRecord(v: unknown): v is Record<string, unknown> {
  return typeof v === "object" && v !== null && !Array.isArray(v);
}

/**
 * Strictly parse an untrusted /health body into ServiceHealth. Returns null on
 * any missing field, wrong type, or unknown enum value — the caller treats null
 * as "not reachable / not tradable". This is the runtime guard TypeScript's
 * compile-time types cannot provide for network data.
 */
export function parseServiceHealth(raw: unknown): ServiceHealth | null {
  if (!isRecord(raw)) return null;
  const { status, service, data_health, broker_health, reconciled, new_position_allowed } = raw;
  if (typeof status !== "string" || !SERVICE_STATUS.includes(status as ServiceStatus)) return null;
  if (typeof service !== "string") return null;
  if (typeof data_health !== "string" || !DATA_HEALTH.includes(data_health as DataHealth))
    return null;
  if (typeof broker_health !== "string" || !BROKER_HEALTH.includes(broker_health as BrokerHealth))
    return null;
  if (typeof reconciled !== "boolean") return null;
  if (typeof new_position_allowed !== "boolean") return null;
  return {
    schema_version: typeof raw.schema_version === "string" ? raw.schema_version : "1.0",
    status: status as ServiceStatus,
    service,
    environment: typeof raw.environment === "string" ? raw.environment : undefined,
    data_health: data_health as DataHealth,
    broker_health: broker_health as BrokerHealth,
    reconciled,
    new_position_allowed,
  };
}

export interface GateInput {
  /** Core reachable and returned a body that parsed into ServiceHealth. */
  reachable: boolean;
  health: ServiceHealth | null;
}

/**
 * Returns true only when a NEW position may be opened. Fail closed on any
 * unreachable/missing/degraded/contradictory signal. Obeys the Rust gateway's
 * new_position_allowed AND re-checks every precondition locally.
 */
export function canOpenNewPosition({ reachable, health }: GateInput): boolean {
  if (!reachable || !health) return false;
  return (
    health.status === "ok" &&
    health.data_health === "HEALTHY" &&
    health.broker_health === "HEALTHY" &&
    health.reconciled === true &&
    health.new_position_allowed === true
  );
}
