import { useCallback, useEffect, useState, type ReactNode } from "react";
import {
  AlertTriangle,
  BadgeCheck,
  BookOpenCheck,
  Bot,
  BrainCircuit,
  CalendarDays,
  CircleOff,
  FlaskConical,
  RefreshCcw,
  Scale,
  ShieldCheck,
  Sparkles,
  Target,
} from "lucide-react";
import {
  parseLLMReview,
  parseLLMServiceStatus,
  parseRuleHypotheses,
  type LLMReview,
  type LLMServiceStatus,
  type RuleHypothesisRecord,
} from "./review";

type LoadState = "loading" | "ready" | "empty" | "unavailable";

export function ReviewPage() {
  const [state, setState] = useState<LoadState>("loading");
  const [review, setReview] = useState<LLMReview | null>(null);
  const [hypotheses, setHypotheses] = useState<RuleHypothesisRecord[]>([]);
  const [service, setService] = useState<LLMServiceStatus | null>(null);
  const [refreshKey, setRefreshKey] = useState(0);

  const load = useCallback(async (signal: AbortSignal) => {
    setState("loading");
    try {
      const [statusResponse, reviewResponse, hypothesisResponse] = await Promise.all([
        fetch("/api/v1/llm/status", { signal }),
        fetch("/api/v1/llm/daily-reviews/latest", { signal }),
        fetch("/api/v1/llm/rule-hypotheses?limit=20", { signal }),
      ]);
      if (!statusResponse.ok || !hypothesisResponse.ok) throw new Error("review service unavailable");
      const parsedStatus = parseLLMServiceStatus(await statusResponse.json());
      const parsedHypotheses = parseRuleHypotheses(await hypothesisResponse.json());
      if (!parsedStatus || !parsedHypotheses) throw new Error("review contract invalid");
      setService(parsedStatus);
      setHypotheses(parsedHypotheses);
      if (reviewResponse.status === 404) {
        setReview(null);
        setState("empty");
        return;
      }
      if (!reviewResponse.ok) throw new Error("daily review unavailable");
      const parsedReview = parseLLMReview(await reviewResponse.json());
      if (!parsedReview) throw new Error("daily review contract invalid");
      setReview(parsedReview);
      setState("ready");
    } catch (error) {
      if (error instanceof DOMException && error.name === "AbortError") return;
      setReview(null);
      setHypotheses([]);
      setService(null);
      setState("unavailable");
    }
  }, []);

  useEffect(() => {
    const controller = new AbortController();
    void load(controller.signal);
    return () => controller.abort();
  }, [load, refreshKey]);

  const daily = review?.daily_review ?? null;
  return (
    <main className="page review-page">
      <header className="page-header review-header">
        <div>
          <span className="page-eyebrow"><BookOpenCheck size={14} /> LLM 审阅台</span>
          <h1>每日复盘</h1>
          <p>以结构化审计记录解释交易，规则变更始终留在研究队列。</p>
        </div>
        <button className="secondary-command" type="button" onClick={() => setRefreshKey((key) => key + 1)} disabled={state === "loading"}>
          <RefreshCcw size={16} aria-hidden="true" /> 刷新
        </button>
      </header>

      <section className="review-authority" aria-label="LLM 权限边界">
        <ShieldCheck size={19} aria-hidden="true" />
        <div><strong>顾问权限：只读</strong><span>不能修改硬风控、下单、撤单或激活研究规则</span></div>
        <span className={service?.configured ? "review-service-on" : "review-service-off"}>
          {service?.configured ? `${service.provider} · ${service.model}` : "审阅服务未就绪"}
        </span>
      </section>

      {state === "loading" && <ReviewState icon={<BrainCircuit size={24} />} title="正在读取审阅记录" detail="等待 PostgreSQL 审计视图返回。" />}
      {state === "empty" && <ReviewState icon={<CalendarDays size={24} />} title="尚无每日复盘" detail="完成盘后结构化审阅后，此处会显示不可变记录。" />}
      {state === "unavailable" && <ReviewState icon={<CircleOff size={24} />} title="审阅数据不可用" detail="页面保持空白；核心交易与确定性风险流程不受影响。" danger />}

      {state === "ready" && review && daily && (
        <>
          <section className="review-summary-band">
            <div className="review-summary-copy">
              <span>{review.trading_date ?? "交易日未知"} · {localizeAlignment(review.sop_alignment)}</span>
              <h2>{review.summary}</h2>
              <p>{review.decision_support}</p>
            </div>
            <div className="review-score" aria-label={`Confidence: ${review.confidence}`}>
              <span>审阅置信度</span><strong>{Math.round(review.confidence * 100)}%</strong>
              <small>仅影响提示优先级</small>
            </div>
          </section>

          <div className="review-grid">
            <ReviewPanel icon={<Target size={17} />} title="交易回看" subtitle="最佳、最差与损失质量">
              <ReviewFact label="最佳交易" value={daily.best_trade ?? "无可评估交易"} tone="good" />
              <ReviewFact label="最差交易" value={daily.worst_trade ?? "无可评估交易"} tone="bad" />
              <ListBlock title="好亏损" items={daily.good_losses} empty="没有识别到好亏损。" />
              <ListBlock title="坏亏损" items={daily.bad_losses} empty="没有识别到坏亏损。" danger />
            </ReviewPanel>

            <ReviewPanel icon={<Scale size={17} />} title="SOP 一致性" subtitle="违规、风险与失效条件">
              <ListBlock title="SOP 违规" items={daily.sop_violations} empty="未识别到已证实的 SOP 违规。" danger />
              <ListBlock title="风险备注" items={review.risk_notes} empty="没有附加风险备注。" />
              <ListBlock title="失效条件" items={review.invalidations} empty="没有记录失效条件。" />
            </ReviewPanel>
          </div>

          <section className="tomorrow-focus">
            <span className="section-icon"><Sparkles size={18} /></span>
            <div><span>明天只改一件事</span><strong>{daily.one_change_tomorrow}</strong></div>
          </section>

          <div className="review-grid review-lower-grid">
            <ReviewPanel icon={<BadgeCheck size={17} />} title="损失归因" subtitle="方向、IV、Theta、滑点与执行">
              {daily.loss_attribution.length === 0 ? (
                <div className="review-empty-row">没有可归因的已实现损失。</div>
              ) : (
                <div className="attribution-table">
                  {daily.loss_attribution.map((item, index) => (
                    <div className="attribution-row" key={`${item.kind}-${index}`}>
                      <span>{localizeAttribution(item.kind)}</span><p>{item.explanation}</p>
                    </div>
                  ))}
                </div>
              )}
            </ReviewPanel>

            <ReviewPanel icon={<FlaskConical size={17} />} title="规则研究队列" subtitle={`${hypotheses.length} 项待验证假设`}>
              {hypotheses.length === 0 ? (
                <div className="review-empty-row">当前没有待验证规则假设。</div>
              ) : (
                <div className="hypothesis-list">
                  {hypotheses.map((hypothesis) => (
                    <article key={hypothesis.hypothesis_id}>
                      <header><strong>{hypothesis.payload.title}</strong><span>仅研究 · 不可激活</span></header>
                      <p>{hypothesis.payload.rationale}</p>
                      <div><FlaskConical size={14} /><span>{hypothesis.payload.validation_plan}</span></div>
                    </article>
                  ))}
                </div>
              )}
            </ReviewPanel>
          </div>

          <footer className="review-audit-footer">
            <Bot size={15} /><span>{review.provider.model}</span><i />
            <span>{review.provider.prompt_version}</span><i />
            <span>{review.provider.latency_ms} ms</span><i />
            <span>{review.provider.input_tokens + review.provider.output_tokens} tokens</span><i />
            <code>{review.review_id}</code>
          </footer>
        </>
      )}
    </main>
  );
}

function ReviewState({ icon, title, detail, danger = false }: { icon: ReactNode; title: string; detail: string; danger?: boolean }) {
  return <section className={`review-state${danger ? " danger" : ""}`}>{icon}<div><strong>{title}</strong><span>{detail}</span></div></section>;
}
function ReviewPanel({ icon, title, subtitle, children }: { icon: ReactNode; title: string; subtitle: string; children: ReactNode }) {
  return <section className="review-panel"><header><span className="panel-icon">{icon}</span><div><h2>{title}</h2><p>{subtitle}</p></div></header><div className="review-panel-body">{children}</div></section>;
}
function ReviewFact({ label, value, tone }: { label: string; value: string; tone: "good" | "bad" }) {
  return <div className={`review-fact ${tone}`}><span>{label}</span><strong>{value}</strong></div>;
}
function ListBlock({ title, items, empty, danger = false }: { title: string; items: string[]; empty: string; danger?: boolean }) {
  return <div className={`review-list-block${danger ? " danger" : ""}`}><span>{title}</span>{items.length === 0 ? <p>{empty}</p> : <ul>{items.map((item) => <li key={item}><AlertTriangle size={13} />{item}</li>)}</ul>}</div>;
}
function localizeAlignment(value: string): string { return ({ Aligned: "SOP 一致", Conflict: "存在冲突", Unknown: "证据不足" } as Record<string, string>)[value] ?? value; }
function localizeAttribution(value: string): string { return ({ DIRECTION: "方向", IV: "隐含波动率", THETA: "时间价值", SLIPPAGE: "滑点", EXECUTION_ERROR: "执行错误", OTHER: "其他" } as Record<string, string>)[value] ?? value; }
