import { act, cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ReviewPage } from "./ReviewPage";
import { parseLLMReview, parseRuleHypotheses } from "./review";


const STATUS = {
  configured: true,
  provider: "deepseek-openai",
  model: "deepseek-v4-flash",
  trading_authority: "NONE",
};

const REVIEW = {
  schema_version: "1.0",
  review_id: "review_daily_001",
  request_id: "request_daily_001",
  session_id: "session_2026-07-20",
  occurred_at_utc: "2026-07-20T20:01:00Z",
  received_at_utc: "2026-07-20T20:01:02Z",
  rule_version: "rules_p3_1.0.0",
  stage: "POST_MARKET",
  trading_date: "2026-07-20",
  review_status: "COMPLETED",
  summary: "今日执行遵守了确定性止损。",
  decision_support: "亏损来自方向变化，不是执行绕过。",
  sop_alignment: "Aligned",
  risk_notes: ["尾盘 Gamma 风险上升。"],
  invalidations: ["事件上下文变为不可用。"],
  recommended_action: "Review Only",
  confidence: 0.76,
  rule_references: ["SOP-EXIT-02"],
  rule_hypotheses: [],
  daily_review: {
    best_trade: "上午 Long Gamma 按计划止盈。",
    worst_trade: "午后 Long Gamma 触发止损。",
    good_losses: ["止损在规则窗口内执行。"],
    bad_losses: [],
    sop_violations: [],
    loss_attribution: [{ kind: "DIRECTION", explanation: "标的反转。", evidence_ids: [] }],
    one_change_tomorrow: "保持事件前禁开新仓。",
  },
  unavailable_reason_code: null,
  provider: {
    provider: "deepseek-openai",
    model: "deepseek-v4-flash",
    provider_request_id: "provider-review-page-001",
    prompt_version: "phase4-review-v3",
    input_hash: "b".repeat(64),
    latency_ms: 320,
    attempts: 1,
    cache_hit: false,
    input_tokens: 800,
    output_tokens: 220,
    estimated_cost_usd: "0.0001736",
  },
};

const HYPOTHESES = [{
  hypothesis_id: "hyp_001",
  review_id: "review_daily_001",
  session_id: "session_2026-07-20",
  trading_date: "2026-07-20",
  status: "PENDING_RESEARCH",
  activation_allowed: false,
  payload: {
    title: "验证更窄的入场窗口",
    rationale: "单日样本只构成研究问题。",
    validation_plan: "完成成本回测、walk-forward 与样本外验证。",
    evidence_ids: [],
    status: "RESEARCH_ONLY",
    activation_allowed: false,
  },
}];

function response(body: unknown, status = 200): Response {
  return { ok: status >= 200 && status < 300, status, json: async () => body } as Response;
}

function installFetch(reviewResponse: Response = response(REVIEW), hypotheses: unknown = HYPOTHESES) {
  vi.stubGlobal("fetch", vi.fn(async (url: string) => {
    if (url.includes("/status")) return response(STATUS);
    if (url.includes("/daily-reviews")) return reviewResponse;
    return response(hypotheses);
  }));
}

afterEach(() => {
  cleanup();
  vi.unstubAllGlobals();
});

describe("ReviewPage", () => {
  it("renders the daily review and keeps hypotheses research-only", async () => {
    installFetch();
    render(<ReviewPage />);
    expect(await screen.findByText("今日执行遵守了确定性止损。")).toBeInTheDocument();
    expect(screen.getByText("保持事件前禁开新仓。")).toBeInTheDocument();
    expect(screen.getByText("仅研究 · 不可激活")).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: /下单|应用规则|确认交易/ })).not.toBeInTheDocument();
  });

  it("shows an empty state for a valid 404 without inventing a review", async () => {
    installFetch(response({}, 404), []);
    render(<ReviewPage />);
    expect(await screen.findByText("尚无每日复盘")).toBeInTheDocument();
  });

  it("fails closed when a research hypothesis claims activation authority", async () => {
    installFetch(response(REVIEW), [{ ...HYPOTHESES[0], activation_allowed: true }]);
    render(<ReviewPage />);
    expect(await screen.findByText("审阅数据不可用")).toBeInTheDocument();
    expect(screen.queryByText("今日执行遵守了确定性止损。")).not.toBeInTheDocument();
  });

  it("refreshes all read-only views without mutating trading state", async () => {
    installFetch();
    render(<ReviewPage />);
    await screen.findByText("今日执行遵守了确定性止损。");
    const mockedFetch = vi.mocked(fetch);
    await act(async () => fireEvent.click(screen.getByRole("button", { name: "刷新" })));
    await waitFor(() => expect(mockedFetch).toHaveBeenCalledTimes(6));
  });
});

describe("review contract parser", () => {
  it("rejects non-completed advice that is not inert", () => {
    expect(parseLLMReview({
      ...REVIEW,
      review_status: "UNAVAILABLE",
      unavailable_reason_code: "TIMEOUT",
      confidence: 0.8,
      recommended_action: "Proceed",
      daily_review: null,
    })).toBeNull();
  });

  it("rejects activation authority at either research envelope", () => {
    expect(parseRuleHypotheses([{ ...HYPOTHESES[0], activation_allowed: true }])).toBeNull();
    expect(parseRuleHypotheses([{ ...HYPOTHESES[0], payload: { ...HYPOTHESES[0].payload, activation_allowed: true } }])).toBeNull();
  });

  it("rejects cross-stage artifacts and actions", () => {
    expect(parseLLMReview({ ...REVIEW, stage: "INTRADAY", daily_review: null, recommended_action: "Proceed" })).toBeNull();
    expect(parseLLMReview({ ...REVIEW, stage: "INTRADAY" })).toBeNull();
    expect(parseLLMReview({
      ...REVIEW,
      review_status: "UNAVAILABLE",
      unavailable_reason_code: "TIMEOUT",
      recommended_action: "Review Only",
      confidence: 0,
      daily_review: null,
      rule_hypotheses: [HYPOTHESES[0].payload],
    })).toBeNull();
  });
});
