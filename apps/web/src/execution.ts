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
  quote: OptionQuoteProof;
  broker_contract_id: string;
  symbol: string;
  exchange?: string;
}

export interface OptionQuoteProof {
  bid: string;
  ask: string;
  bid_size: number;
  ask_size: number;
  occurred_at_utc: string;
  delta: string;
  gamma: string;
  theta: string;
  vega: string;
  chain_snapshot_id: string;
  provider: "THETADATA";
}

export interface AdaptiveLimitPolicy {
  initial_aggressiveness_bps: number;
  max_attempts: number;
  max_quote_age_ms: number;
  max_spread_bps: number;
}

export interface CandidateTradePlan {
  schema_version: "1.3";
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
  order_side: "BUY" | "SELL";
  order_type: "MARKET" | "LIMIT" | "ADAPTIVE_LIMIT";
  adaptive_limit?: AdaptiveLimitPolicy;
  market_data_provider: "THETADATA";
  position_effect: "OPEN" | "CLOSE";
}

export interface ExecutionOrder {
  schema_version: "1.1";
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
  broker_child_order_ids: string[];
  broker_child_orders: ExecutionChildOrder[];
  residual_exposure: boolean;
  risk_reason_codes: string[];
}

export interface ExecutionChildOrder {
  broker_order_id: string;
  leg_index: number;
  contract_id: string;
  side: "BUY" | "SELL";
  quantity: number;
  filled_quantity: number;
  state: "WORKING" | "PARTIAL_FILL" | "FILLED" | "CANCELLED" | "REJECTED" | "RECONCILE_PENDING";
  submitted_price: string | null;
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

function parseQuote(value: unknown): OptionQuoteProof | null {
  if (!record(value)) return null;
  if (
    !decimal(value.bid) ||
    !decimal(value.ask) ||
    !Number.isInteger(value.bid_size) ||
    Number(value.bid_size) < 1 ||
    !Number.isInteger(value.ask_size) ||
    Number(value.ask_size) < 1 ||
    !utc(value.occurred_at_utc) ||
    !decimal(value.delta) ||
    !decimal(value.gamma) ||
    !decimal(value.theta) ||
    !decimal(value.vega) ||
    typeof value.chain_snapshot_id !== "string" ||
    value.chain_snapshot_id.length === 0 ||
    value.provider !== "THETADATA"
  ) return null;
  return value as unknown as OptionQuoteProof;
}

function parseAdaptiveLimit(value: unknown): AdaptiveLimitPolicy | null {
  if (!record(value)) return null;
  const fields = [
    value.initial_aggressiveness_bps,
    value.max_attempts,
    value.max_quote_age_ms,
    value.max_spread_bps,
  ];
  if (!fields.every(Number.isInteger)) return null;
  const policy = value as unknown as AdaptiveLimitPolicy;
  if (
    policy.initial_aggressiveness_bps < 0 || policy.initial_aggressiveness_bps > 10_000 ||
    policy.max_attempts < 1 || policy.max_attempts > 10 ||
    policy.max_quote_age_ms < 1 || policy.max_quote_age_ms > 5_000 ||
    policy.max_spread_bps < 1 || policy.max_spread_bps > 10_000
  ) return null;
  return policy;
}

function parseLeg(value: unknown): CandidateLeg | null {
  if (!record(value)) return null;
  if (value.side !== "BUY" && value.side !== "SELL") return null;
  if (value.type !== "CALL" && value.type !== "PUT") return null;
  const quote = parseQuote(value.quote);
  if (
    typeof value.contract_id !== "string" ||
    typeof value.expiry !== "string" ||
    !decimal(value.strike) ||
    !Number.isInteger(value.quantity) ||
    Number(value.quantity) < 1 ||
    !quote ||
    typeof value.symbol !== "string" ||
    value.symbol.length === 0 ||
    typeof value.broker_contract_id !== "string" || value.broker_contract_id.length === 0 ||
    (value.exchange !== undefined && typeof value.exchange !== "string")
  )
    return null;
  return {
    side: value.side,
    type: value.type,
    contract_id: value.contract_id,
    expiry: value.expiry,
    strike: value.strike,
    quantity: Number(value.quantity),
    quote,
    broker_contract_id: value.broker_contract_id,
    symbol: value.symbol,
    ...(typeof value.exchange === "string" ? { exchange: value.exchange } : {}),
  };
}

function parsePlan(value: unknown): CandidateTradePlan | null {
  if (!record(value) || value.schema_version !== "1.3") return null;
  const legs = Array.isArray(value.legs) ? value.legs.map(parseLeg) : [];
  const adaptive = value.adaptive_limit === undefined ? null : parseAdaptiveLimit(value.adaptive_limit);
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
    value.manual_confirmation_required !== true ||
    (value.order_side !== "BUY" && value.order_side !== "SELL") ||
    !["MARKET", "LIMIT", "ADAPTIVE_LIMIT"].includes(String(value.order_type)) ||
    (value.order_type === "ADAPTIVE_LIMIT" && !adaptive) ||
    (value.order_type !== "ADAPTIVE_LIMIT" && value.adaptive_limit !== undefined)
    || value.market_data_provider !== "THETADATA" ||
    (value.position_effect !== "OPEN" && value.position_effect !== "CLOSE") ||
    (value.position_effect === "OPEN" && Number(value.max_loss) <= 0) ||
    (value.position_effect === "CLOSE" && Number(value.max_loss) !== 0) ||
    (value.order_type === "MARKET" && (value.position_effect !== "CLOSE" || legs.length !== 1))
  )
    return null;
  return {
    schema_version: "1.3",
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
    order_side: value.order_side as CandidateTradePlan["order_side"],
    order_type: value.order_type as CandidateTradePlan["order_type"],
    market_data_provider: "THETADATA",
    position_effect: value.position_effect,
    ...(adaptive
      ? { adaptive_limit: adaptive }
      : {}),
  };
}

function parseChildOrder(value: unknown): ExecutionChildOrder | null {
  if (!record(value)) return null;
  if (
    typeof value.broker_order_id !== "string" || value.broker_order_id.length === 0 ||
    !Number.isInteger(value.leg_index) || Number(value.leg_index) < 0 ||
    typeof value.contract_id !== "string" || value.contract_id.length === 0 ||
    (value.side !== "BUY" && value.side !== "SELL") ||
    !Number.isInteger(value.quantity) || Number(value.quantity) < 1 ||
    !Number.isInteger(value.filled_quantity) || Number(value.filled_quantity) < 0 ||
    Number(value.filled_quantity) > Number(value.quantity) ||
    !["WORKING", "PARTIAL_FILL", "FILLED", "CANCELLED", "REJECTED", "RECONCILE_PENDING"].includes(String(value.state)) ||
    (value.submitted_price !== null && (!decimal(value.submitted_price) || Number(value.submitted_price) <= 0))
  ) return null;
  if (value.state === "FILLED" && value.filled_quantity !== value.quantity) return null;
  if (value.state === "PARTIAL_FILL" && !(Number(value.filled_quantity) > 0 && Number(value.filled_quantity) < Number(value.quantity))) return null;
  if (value.state === "REJECTED" && value.filled_quantity !== 0) return null;
  return value as unknown as ExecutionChildOrder;
}

function parseOrder(value: unknown): ExecutionOrder | null {
  if (!record(value) || value.schema_version !== "1.1") return null;
  const childOrders = Array.isArray(value.broker_child_orders)
    ? value.broker_child_orders.map(parseChildOrder)
    : [];
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
    !Array.isArray(value.broker_child_order_ids) ||
    !value.broker_child_order_ids.every((item) => typeof item === "string" && item.length > 0) ||
    new Set(value.broker_child_order_ids).size !== value.broker_child_order_ids.length ||
    !Array.isArray(value.broker_child_orders) ||
    childOrders.some((child) => child === null) ||
    typeof value.residual_exposure !== "boolean" ||
    !Array.isArray(value.risk_reason_codes) ||
    !value.risk_reason_codes.every((item) => typeof item === "string")
  )
    return null;
  const children = childOrders as ExecutionChildOrder[];
  const childIds = children.map((child) => child.broker_order_id);
  if (
    new Set(childIds).size !== childIds.length ||
    new Set(children.map((child) => child.leg_index)).size !== children.length ||
    childIds.join("\u0000") !== value.broker_child_order_ids.join("\u0000")
  ) return null;
  if (children.length > 0) {
    const allFilled = children.every((child) => child.state === "FILLED" && child.filled_quantity === child.quantity);
    const possibleFill = children.some((child) => child.state === "WORKING" || child.state === "RECONCILE_PENDING");
    const incompleteFill = children.some((child) => child.filled_quantity > 0) && !allFilled;
    if (value.residual_exposure !== (possibleFill || incompleteFill)) return null;
  }
  if (value.residual_exposure && !["WORKING", "PARTIAL_FILL", "RECONCILE_PENDING", "CANCEL_PENDING", "CANCELLED"].includes(String(value.state))) return null;
  if (value.state === "PARTIAL_FILL" && !value.residual_exposure) return null;
  if (children.length > 0 && (value.state === "WORKING" || value.state === "RECONCILE_PENDING") && !value.residual_exposure) return null;
  return { ...(value as unknown as ExecutionOrder), broker_child_orders: children };
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
    plan.execution_mode !== order.execution_mode ||
    plan.legs[0]?.quantity !== order.total_quantity
  )
    return null;
  if (order.broker_child_orders.some((child) => {
    const leg = plan.legs[child.leg_index];
    return !leg || leg.contract_id !== child.contract_id || leg.side !== child.side || leg.quantity !== child.quantity;
  })) return null;
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
    return (
      incoming.state_version > current.state_version &&
      incoming.filled_quantity >= current.filled_quantity
    );
  }
  return (
    incoming.state === current.state &&
    incoming.filled_quantity >= current.filled_quantity &&
    incoming.residual_exposure === current.residual_exposure &&
    incoming.broker_child_order_ids.join("\u0000") === current.broker_child_order_ids.join("\u0000") &&
    JSON.stringify(incoming.broker_child_orders) === JSON.stringify(current.broker_child_orders) &&
    Date.parse(incoming.updated_at_utc) >= Date.parse(current.updated_at_utc)
  );
}

export type ProjectionRelation = "NEWER" | "DUPLICATE" | "STALE" | "CONFLICT";

/** Classify an action response so same-version conflicts cannot be silently ignored. */
export function classifyExecutionProjection(
  current: ExecutionOrder,
  incoming: ExecutionOrder,
): ProjectionRelation {
  if (incoming.order_id !== current.order_id) return "CONFLICT";
  if (incoming.state_version < current.state_version) return "STALE";
  if (incoming.state_version > current.state_version) {
    return isNewerExecutionOrder(current, incoming) ? "NEWER" : "CONFLICT";
  }
  const sameContent =
    incoming.state === current.state &&
    incoming.filled_quantity === current.filled_quantity &&
    incoming.broker_order_id === current.broker_order_id &&
    incoming.residual_exposure === current.residual_exposure &&
    incoming.broker_child_order_ids.join("\u0000") === current.broker_child_order_ids.join("\u0000") &&
    JSON.stringify(incoming.broker_child_orders) === JSON.stringify(current.broker_child_orders) &&
    incoming.risk_reason_codes.join("\u0000") === current.risk_reason_codes.join("\u0000");
  return sameContent ? "DUPLICATE" : "CONFLICT";
}
