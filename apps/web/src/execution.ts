export type ExecutionOrderState =
  | "AWAITING_CONFIRMATION"
  | "RISK_REJECTED"
  | "APPROVED"
  | "SUBMITTING"
  | "WORKING"
  | "PARTIAL_FILL"
  | "FILLED"
  | "CANCEL_PENDING"
  | "CANCELLED"
  | "REJECTED"
  | "EXPIRED"
  | "RECONCILE_PENDING"
  | "SHADOWED";

export interface CandidateLeg {
  side: "BUY" | "SELL";
  type: "CALL" | "PUT";
  contract_id: string;
  expiry: string;
  strike: string;
  quantity: number;
}

export interface CandidateTradePlan {
  schema_version: "1.0";
  plan_id: string;
  plan_hash: string;
  idempotency_key: string;
  session_id: string;
  signal_id: string;
  broker_id: "longbridge" | "ibkr";
  strategy: "LongGamma" | "ShortPremium" | "EventVolCrush";
  execution_mode: "REPLAY" | "SHADOW" | "PAPER" | "MANUAL_CONFIRM" | "CONTROLLED_AUTO";
  created_at_utc: string;
  legs: CandidateLeg[];
  limit_price: string;
  max_loss: string;
  expires_at_utc: string;
  rule_version: string;
  data_snapshot_ids: string[];
  manual_confirmation_required: true;
}

export interface ExecutionOrder {
  schema_version: "1.0";
  order_id: string;
  plan_id: string;
  plan_hash: string;
  idempotency_key: string;
  session_id: string;
  broker_id: "longbridge" | "ibkr";
  execution_mode: CandidateTradePlan["execution_mode"];
  state: ExecutionOrderState;
  total_quantity: number;
  filled_quantity: number;
  broker_order_id: string | null;
  expires_at_utc: string;
  updated_at_utc: string;
  state_version: number;
  risk_reason_codes: string[];
}

export interface ExecutionTicket {
  plan: CandidateTradePlan;
  order: ExecutionOrder;
}

const STATES: readonly ExecutionOrderState[] = [
  "AWAITING_CONFIRMATION",
  "RISK_REJECTED",
  "APPROVED",
  "SUBMITTING",
  "WORKING",
  "PARTIAL_FILL",
  "FILLED",
  "CANCEL_PENDING",
  "CANCELLED",
  "REJECTED",
  "EXPIRED",
  "RECONCILE_PENDING",
  "SHADOWED",
];
const MODES: readonly CandidateTradePlan["execution_mode"][] = [
  "REPLAY",
  "SHADOW",
  "PAPER",
  "MANUAL_CONFIRM",
  "CONTROLLED_AUTO",
];

function record(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function utc(value: unknown): value is string {
  return typeof value === "string" && value.endsWith("Z") && !Number.isNaN(Date.parse(value));
}

function decimal(value: unknown): value is string {
  return typeof value === "string" && /^-?[0-9]+(\.[0-9]+)?$/.test(value);
}

function hash(value: unknown): value is string {
  return typeof value === "string" && /^[a-f0-9]{64}$/.test(value);
}

function parseLeg(value: unknown): CandidateLeg | null {
  if (!record(value)) return null;
  if (value.side !== "BUY" && value.side !== "SELL") return null;
  if (value.type !== "CALL" && value.type !== "PUT") return null;
  if (
    typeof value.contract_id !== "string" ||
    typeof value.expiry !== "string" ||
    !decimal(value.strike) ||
    !Number.isInteger(value.quantity) ||
    Number(value.quantity) < 1
  )
    return null;
  return {
    side: value.side,
    type: value.type,
    contract_id: value.contract_id,
    expiry: value.expiry,
    strike: value.strike,
    quantity: Number(value.quantity),
  };
}

function parsePlan(value: unknown): CandidateTradePlan | null {
  if (!record(value) || value.schema_version !== "1.0") return null;
  const legs = Array.isArray(value.legs) ? value.legs.map(parseLeg) : [];
  if (
    typeof value.plan_id !== "string" ||
    !hash(value.plan_hash) ||
    typeof value.idempotency_key !== "string" ||
    typeof value.session_id !== "string" ||
    typeof value.signal_id !== "string" ||
    (value.broker_id !== "longbridge" && value.broker_id !== "ibkr") ||
    !["LongGamma", "ShortPremium", "EventVolCrush"].includes(String(value.strategy)) ||
    !MODES.includes(value.execution_mode as CandidateTradePlan["execution_mode"]) ||
    !utc(value.created_at_utc) ||
    legs.length < 1 ||
    legs.some((leg) => leg === null) ||
    new Set(legs.map((leg) => leg?.quantity)).size !== 1 ||
    !decimal(value.limit_price) ||
    !decimal(value.max_loss) ||
    !utc(value.expires_at_utc) ||
    typeof value.rule_version !== "string" ||
    !Array.isArray(value.data_snapshot_ids) ||
    !value.data_snapshot_ids.every((item) => typeof item === "string") ||
    value.manual_confirmation_required !== true
  )
    return null;
  return {
    schema_version: "1.0",
    plan_id: value.plan_id,
    plan_hash: value.plan_hash,
    idempotency_key: value.idempotency_key,
    session_id: value.session_id,
    signal_id: value.signal_id,
    broker_id: value.broker_id,
    strategy: value.strategy as CandidateTradePlan["strategy"],
    execution_mode: value.execution_mode as CandidateTradePlan["execution_mode"],
    created_at_utc: value.created_at_utc,
    legs: legs as CandidateLeg[],
    limit_price: value.limit_price,
    max_loss: value.max_loss,
    expires_at_utc: value.expires_at_utc,
    rule_version: value.rule_version,
    data_snapshot_ids: value.data_snapshot_ids as string[],
    manual_confirmation_required: true,
  };
}

function parseOrder(value: unknown): ExecutionOrder | null {
  if (!record(value) || value.schema_version !== "1.0") return null;
  if (
    typeof value.order_id !== "string" ||
    typeof value.plan_id !== "string" ||
    !hash(value.plan_hash) ||
    typeof value.idempotency_key !== "string" ||
    typeof value.session_id !== "string" ||
    (value.broker_id !== "longbridge" && value.broker_id !== "ibkr") ||
    !MODES.includes(value.execution_mode as CandidateTradePlan["execution_mode"]) ||
    !STATES.includes(value.state as ExecutionOrderState) ||
    !Number.isInteger(value.total_quantity) ||
    Number(value.total_quantity) < 1 ||
    !Number.isInteger(value.filled_quantity) ||
    Number(value.filled_quantity) < 0 ||
    Number(value.filled_quantity) > Number(value.total_quantity) ||
    (value.broker_order_id !== null && typeof value.broker_order_id !== "string") ||
    !utc(value.expires_at_utc) ||
    !utc(value.updated_at_utc) ||
    !Number.isInteger(value.state_version) ||
    Number(value.state_version) < 1 ||
    !Array.isArray(value.risk_reason_codes) ||
    !value.risk_reason_codes.every((item) => typeof item === "string")
  )
    return null;
  return value as unknown as ExecutionOrder;
}

export function parseExecutionTicket(value: unknown): ExecutionTicket | null {
  if (!record(value)) return null;
  const plan = parsePlan(value.plan);
  const order = parseOrder(value.order);
  if (!plan || !order) return null;
  if (
    plan.plan_id !== order.plan_id ||
    plan.plan_hash !== order.plan_hash ||
    plan.idempotency_key !== order.idempotency_key ||
    plan.session_id !== order.session_id ||
    plan.broker_id !== order.broker_id ||
    plan.execution_mode !== order.execution_mode
  )
    return null;
  return { plan, order };
}

/** Accept only a monotonic projection for one order. A different order must
 * have a strictly later authoritative timestamp. Equal versions may refresh
 * the timestamp but cannot change state or reduce filled quantity. */
export function isNewerExecutionOrder(current: ExecutionOrder, incoming: ExecutionOrder): boolean {
  if (incoming.order_id !== current.order_id) {
    return Date.parse(incoming.updated_at_utc) > Date.parse(current.updated_at_utc);
  }
  if (incoming.state_version !== current.state_version) {
    return incoming.state_version > current.state_version;
  }
  return (
    incoming.state === current.state &&
    incoming.filled_quantity >= current.filled_quantity &&
    Date.parse(incoming.updated_at_utc) >= Date.parse(current.updated_at_utc)
  );
}
