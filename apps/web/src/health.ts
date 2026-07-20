/**
 * Shared health contract + trading gate for the Cockpit.
 *
 * Mirrors health.json#/$defs/ServiceHealth. The gate is fail-closed: a new
 * position is permitted ONLY when the API/core is reachable AND data_health is
 * HEALTHY AND broker_health is HEALTHY AND the book is reconciled. Every other
 * combination — including a failed fetch or any missing field — is No Trade.
 *
 * This is the single source of truth the Rust `new_position_allowed` gate and
 * the Python BFF fallback are kept consistent with; keep the conjunction here
 * identical to risk-gateway::new_position_allowed.
 */
export type DataHealth = "HEALTHY" | "DEGRADED" | "STALE" | "DISCONNECTED" | "RECONCILING";
export type BrokerHealth = "HEALTHY" | "DEGRADED" | "DISCONNECTED";

export interface ServiceHealth {
  status: string;
  service: string;
  data_health: DataHealth;
  broker_health: BrokerHealth;
  reconciled: boolean;
  new_position_allowed?: boolean;
}

export interface GateInput {
  /** API/core reachable and returned a parseable body. */
  reachable: boolean;
  health: ServiceHealth | null;
}

/**
 * Returns true only when a NEW position may be opened. Fail closed on any
 * unreachable/missing/degraded signal. We compute the conjunction locally
 * rather than trusting the server's `new_position_allowed` alone, so a
 * malformed or partial payload can never flip the UI to ALLOWED.
 */
export function canOpenNewPosition({ reachable, health }: GateInput): boolean {
  if (!reachable || !health) return false;
  return (
    health.data_health === "HEALTHY" &&
    health.broker_health === "HEALTHY" &&
    health.reconciled === true
  );
}
