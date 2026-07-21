export type ReviewStatus = "COMPLETED" | "UNAVAILABLE" | "INVALID";
export type SopAlignment = "Aligned" | "Conflict" | "Unknown";

export interface LossAttribution {
  kind: "DIRECTION" | "IV" | "THETA" | "SLIPPAGE" | "EXECUTION_ERROR" | "OTHER";
  explanation: string;
  evidence_ids: string[];
}

export interface DailyReviewDetail {
  best_trade: string | null;
  worst_trade: string | null;
  good_losses: string[];
  bad_losses: string[];
  sop_violations: string[];
  loss_attribution: LossAttribution[];
  one_change_tomorrow: string;
}

export interface ProviderMetadata {
  provider: string;
  model: string;
  provider_request_id: string | null;
  prompt_version: string;
  input_hash: string;
  latency_ms: number;
  attempts: number;
  cache_hit: boolean;
  input_tokens: number;
  output_tokens: number;
  estimated_cost_usd: string;
}

export interface LLMReview {
  schema_version: "1.0";
  review_id: string;
  request_id: string;
  session_id: string;
  occurred_at_utc: string;
  received_at_utc: string;
  rule_version: string;
  stage: "POST_MARKET" | "PRE_MARKET" | "INTRADAY" | "PRE_EXECUTION" | "RULE_HYPOTHESIS";
  trading_date: string | null;
  review_status: ReviewStatus;
  summary: string;
  decision_support: string;
  sop_alignment: SopAlignment;
  risk_notes: string[];
  invalidations: string[];
  recommended_action: "Proceed" | "Wait" | "Cancel" | "Reduce Risk" | "Review Only";
  confidence: number;
  rule_references: string[];
  daily_review: DailyReviewDetail | null;
  rule_hypotheses: RuleHypothesis[];
  unavailable_reason_code: string | null;
  provider: ProviderMetadata;
}

export interface RuleHypothesis {
  title: string;
  rationale: string;
  validation_plan: string;
  evidence_ids: string[];
  status: "RESEARCH_ONLY";
  activation_allowed: false;
}

export interface RuleHypothesisRecord {
  hypothesis_id: string;
  review_id: string;
  session_id: string | null;
  trading_date: string | null;
  status: "PENDING_RESEARCH" | "VALIDATING" | "REJECTED" | "APPROVED_FOR_SHADOW";
  activation_allowed: false;
  payload: {
    title: string;
    rationale: string;
    validation_plan: string;
    evidence_ids: string[];
    status: "RESEARCH_ONLY";
    activation_allowed: false;
  };
}

export interface LLMServiceStatus {
  configured: boolean;
  provider: string;
  model: string;
  trading_authority: "NONE";
}

export function parseLLMReview(value: unknown): LLMReview | null {
  if (!isRecord(value)) return null;
  if (
    value.schema_version !== "1.0" ||
    !isString(value.review_id) ||
    !isString(value.request_id) ||
    !isString(value.session_id) ||
    !isUtc(value.occurred_at_utc) ||
    !isUtc(value.received_at_utc) ||
    !isString(value.rule_version) ||
    !isOneOf(value.stage, ["POST_MARKET", "PRE_MARKET", "INTRADAY", "PRE_EXECUTION", "RULE_HYPOTHESIS"]) ||
    !(value.trading_date === null || isDate(value.trading_date)) ||
    !isOneOf(value.review_status, ["COMPLETED", "UNAVAILABLE", "INVALID"]) ||
    typeof value.summary !== "string" ||
    typeof value.decision_support !== "string" ||
    !isOneOf(value.sop_alignment, ["Aligned", "Conflict", "Unknown"]) ||
    !isStringArray(value.risk_notes) ||
    !isStringArray(value.invalidations) ||
    !isOneOf(value.recommended_action, ["Proceed", "Wait", "Cancel", "Reduce Risk", "Review Only"]) ||
    typeof value.confidence !== "number" ||
    !Number.isFinite(value.confidence) ||
    value.confidence < 0 ||
    value.confidence > 1 ||
    !isStringArray(value.rule_references) ||
    !(value.unavailable_reason_code === null || isString(value.unavailable_reason_code))
  ) return null;
  const provider = parseProvider(value.provider);
  if (!provider) return null;
  const daily = value.daily_review === null ? null : parseDailyReview(value.daily_review);
  if (value.daily_review !== null && !daily) return null;
  const hypotheses = parseEmbeddedHypotheses(value.rule_hypotheses);
  if (!hypotheses) return null;
  if (value.review_status === "COMPLETED" && value.stage === "POST_MARKET") {
    if (!daily || value.trading_date === null) return null;
  } else if (daily !== null) return null;
  if (value.review_status !== "COMPLETED") {
    if (
      value.recommended_action !== "Review Only" ||
      value.confidence !== 0 ||
      value.unavailable_reason_code === null ||
      hypotheses.length !== 0
    ) return null;
  } else if (value.unavailable_reason_code !== null) return null;
  if (value.stage === "RULE_HYPOTHESIS" && value.review_status === "COMPLETED") {
    if (hypotheses.length === 0) return null;
  } else if (value.stage !== "POST_MARKET" && hypotheses.length !== 0) return null;
  if (value.recommended_action === "Proceed" && value.stage !== "PRE_EXECUTION") {
    return null;
  }
  return {
    schema_version: "1.0",
    review_id: value.review_id,
    request_id: value.request_id,
    session_id: value.session_id,
    occurred_at_utc: value.occurred_at_utc,
    received_at_utc: value.received_at_utc,
    rule_version: value.rule_version,
    stage: value.stage,
    trading_date: value.trading_date,
    review_status: value.review_status,
    summary: value.summary,
    decision_support: value.decision_support,
    sop_alignment: value.sop_alignment,
    risk_notes: value.risk_notes,
    invalidations: value.invalidations,
    recommended_action: value.recommended_action,
    confidence: value.confidence,
    rule_references: value.rule_references,
    daily_review: daily,
    rule_hypotheses: hypotheses,
    unavailable_reason_code: value.unavailable_reason_code,
    provider,
  };
}

export function parseRuleHypotheses(value: unknown): RuleHypothesisRecord[] | null {
  if (!Array.isArray(value)) return null;
  const records: RuleHypothesisRecord[] = [];
  for (const item of value) {
    if (!isRecord(item) || !isRecord(item.payload)) return null;
    if (
      !isString(item.hypothesis_id) ||
      !isString(item.review_id) ||
      !(item.session_id === null || isString(item.session_id)) ||
      !(item.trading_date === null || isDate(item.trading_date)) ||
      !isOneOf(item.status, ["PENDING_RESEARCH", "VALIDATING", "REJECTED", "APPROVED_FOR_SHADOW"]) ||
      item.activation_allowed !== false ||
      !isString(item.payload.title) ||
      !isString(item.payload.rationale) ||
      !isString(item.payload.validation_plan) ||
      !isStringArray(item.payload.evidence_ids) ||
      item.payload.status !== "RESEARCH_ONLY" ||
      item.payload.activation_allowed !== false
    ) return null;
    records.push(item as unknown as RuleHypothesisRecord);
  }
  return records;
}

export function parseLLMServiceStatus(value: unknown): LLMServiceStatus | null {
  if (!isRecord(value)) return null;
  if (
    typeof value.configured !== "boolean" ||
    !isString(value.provider) ||
    !isString(value.model) ||
    value.trading_authority !== "NONE"
  ) return null;
  return value as unknown as LLMServiceStatus;
}

function parseProvider(value: unknown): ProviderMetadata | null {
  if (!isRecord(value)) return null;
  if (
    !isString(value.provider) ||
    !isString(value.model) ||
    !(value.provider_request_id === null || isString(value.provider_request_id)) ||
    !isString(value.prompt_version) ||
    !isHash(value.input_hash) ||
    !isNonNegativeInteger(value.latency_ms) ||
    !isNonNegativeInteger(value.attempts) ||
    value.attempts > 4 ||
    typeof value.cache_hit !== "boolean" ||
    !isNonNegativeInteger(value.input_tokens) ||
    !isNonNegativeInteger(value.output_tokens) ||
    !isDecimal(value.estimated_cost_usd)
  ) return null;
  return value as unknown as ProviderMetadata;
}

function parseEmbeddedHypotheses(value: unknown): RuleHypothesis[] | null {
  if (!Array.isArray(value) || value.length > 5) return null;
  const hypotheses: RuleHypothesis[] = [];
  for (const item of value) {
    if (
      !isRecord(item) ||
      !isString(item.title) ||
      !isString(item.rationale) ||
      !isString(item.validation_plan) ||
      !isStringArray(item.evidence_ids) ||
      item.status !== "RESEARCH_ONLY" ||
      item.activation_allowed !== false
    ) return null;
    hypotheses.push(item as unknown as RuleHypothesis);
  }
  return hypotheses;
}

function parseDailyReview(value: unknown): DailyReviewDetail | null {
  if (!isRecord(value)) return null;
  if (
    !(value.best_trade === null || typeof value.best_trade === "string") ||
    !(value.worst_trade === null || typeof value.worst_trade === "string") ||
    !isStringArray(value.good_losses) ||
    !isStringArray(value.bad_losses) ||
    !isStringArray(value.sop_violations) ||
    typeof value.one_change_tomorrow !== "string" ||
    !Array.isArray(value.loss_attribution)
  ) return null;
  const attribution: LossAttribution[] = [];
  for (const item of value.loss_attribution) {
    if (
      !isRecord(item) ||
      !isOneOf(item.kind, ["DIRECTION", "IV", "THETA", "SLIPPAGE", "EXECUTION_ERROR", "OTHER"]) ||
      !isString(item.explanation) ||
      !isStringArray(item.evidence_ids)
    ) return null;
    attribution.push(item as unknown as LossAttribution);
  }
  return { ...value, loss_attribution: attribution } as unknown as DailyReviewDetail;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}
function isString(value: unknown): value is string { return typeof value === "string" && value.length > 0; }
function isStringArray(value: unknown): value is string[] { return Array.isArray(value) && value.every((item) => typeof item === "string" && item.length > 0); }
function isNonNegativeInteger(value: unknown): value is number { return typeof value === "number" && Number.isInteger(value) && value >= 0; }
function isDecimal(value: unknown): value is string { return typeof value === "string" && /^[0-9]+(?:\.[0-9]+)?$/.test(value); }
function isHash(value: unknown): value is string { return typeof value === "string" && /^[a-f0-9]{64}$/.test(value); }
function isUtc(value: unknown): value is string { return typeof value === "string" && value.endsWith("Z") && !Number.isNaN(Date.parse(value)); }
function isDate(value: unknown): value is string { return typeof value === "string" && /^\d{4}-\d{2}-\d{2}$/.test(value); }
function isOneOf<T extends string>(value: unknown, allowed: readonly T[]): value is T { return typeof value === "string" && allowed.includes(value as T); }
