import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { CheckCircle2, Clock3, RefreshCw, ShieldAlert, XCircle } from "lucide-react";
import {
  classifyExecutionProjection,
  isNewerExecutionOrder,
  parseExecutionTicket,
  type ExecutionOrder,
  type ExecutionTicket,
} from "./execution";

type LoadState = "LOADING" | "READY" | "EMPTY" | "UNAVAILABLE";

export function ExecutionPanel({ sessionId, canTrade }: { sessionId: string; canTrade: boolean }) {
  const [ticket, setTicket] = useState<ExecutionTicket | null>(null);
  const [loadState, setLoadState] = useState<LoadState>("LOADING");
  const [pending, setPending] = useState<"confirm" | "cancel" | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [reviewOpen, setReviewOpen] = useState(false);
  const [acknowledged, setAcknowledged] = useState(false);
  const [clock, setClock] = useState(Date.now());
  const requestGeneration = useRef(0);
  const ticketAnchor = useRef<ExecutionTicket | null>(null);

  const refresh = useCallback(async () => {
    const generation = ++requestGeneration.current;
    try {
      const response = await fetch(`/api/v1/trading/orders?session_id=${encodeURIComponent(sessionId)}`);
      if (generation !== requestGeneration.current) return;
      if (response.status === 404) {
        setLoadState(ticketAnchor.current ? "UNAVAILABLE" : "EMPTY");
        return;
      }
      if (!response.ok) throw new Error("unavailable");
      const parsed = parseExecutionTicket(await response.json());
      if (generation !== requestGeneration.current) return;
      if (!parsed) throw new Error("contract");
      const current = ticketAnchor.current;
      if (!current || isNewerExecutionOrder(current.order, parsed.order)) {
        ticketAnchor.current = parsed;
        setTicket(parsed);
      }
      setLoadState("READY");
      setError(null);
    } catch {
      if (generation !== requestGeneration.current) return;
      setLoadState("UNAVAILABLE");
    }
  }, [sessionId]);

  useEffect(() => {
    ticketAnchor.current = null;
    setTicket(null);
    setLoadState("LOADING");
    void refresh();
    const poll = window.setInterval(() => void refresh(), 2_000);
    return () => {
      requestGeneration.current += 1;
      window.clearInterval(poll);
    };
  }, [refresh]);

  useEffect(() => {
    const timer = window.setInterval(() => setClock(Date.now()), 1_000);
    return () => window.clearInterval(timer);
  }, []);

  const remaining = useMemo(() => {
    if (!ticket) return 0;
    return Math.max(0, Math.ceil((Date.parse(ticket.order.expires_at_utc) - clock) / 1000));
  }, [clock, ticket]);
  const confirmable =
    ticket?.order.state === "AWAITING_CONFIRMATION" &&
    (canTrade || ticket.plan.position_effect === "CLOSE") &&
    remaining > 0 &&
    pending === null;
  const cancellable =
    ticket !== null &&
    ["AWAITING_CONFIRMATION", "WORKING", "PARTIAL_FILL"].includes(ticket.order.state) &&
    pending === null;

  const updateActionOrder = (order: ExecutionOrder): boolean => {
    requestGeneration.current += 1;
    const current = ticketAnchor.current;
    if (!current) return false;
    const relation = classifyExecutionProjection(current.order, order);
    if (relation === "NEWER") {
      const next = { ...current, order };
      ticketAnchor.current = next;
      setTicket(next);
    }
    return relation === "NEWER" || relation === "DUPLICATE";
  };

  const confirm = async () => {
    if (!ticket || !confirmable || !acknowledged) return;
    setPending("confirm");
    setError(null);
    try {
      const response = await fetch(`/api/v1/trading/orders/${ticket.order.order_id}/confirm`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ plan_hash: ticket.plan.plan_hash }),
      });
      if (!response.ok) throw new Error(response.status === 409 ? "需要重新对账 · RECONCILIATION REQUIRED" : "确认失败 · CONFIRM FAILED");
      const parsed = parseExecutionTicket({ plan: ticket.plan, order: await response.json() });
      if (!parsed) throw new Error("网关响应无效 · INVALID GATEWAY RESPONSE");
      if (!updateActionOrder(parsed.order)) throw new Error("需要重新对账 · RECONCILIATION REQUIRED");
      setReviewOpen(false);
      setAcknowledged(false);
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : "确认失败 · CONFIRM FAILED");
    } finally {
      setPending(null);
    }
  };

  const cancel = async () => {
    if (!ticket || !cancellable) return;
    setPending("cancel");
    setError(null);
    try {
      const response = await fetch(`/api/v1/trading/orders/${ticket.order.order_id}/cancel`, {
        method: "POST",
      });
      if (!response.ok) throw new Error("撤单失败 · CANCEL FAILED");
      const parsed = parseExecutionTicket({ plan: ticket.plan, order: await response.json() });
      if (!parsed) throw new Error("网关响应无效 · INVALID GATEWAY RESPONSE");
      if (!updateActionOrder(parsed.order)) throw new Error("需要重新对账 · RECONCILIATION REQUIRED");
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : "撤单失败 · CANCEL FAILED");
    } finally {
      setPending(null);
    }
  };

  return (
    <section className="execution-band" aria-labelledby="execution-heading">
      <div className="section-heading-row">
        <div>
          <h2 id="execution-heading">执行控制</h2>
          <span className="section-kicker">Rust 权威控制 · 仅模拟与影子模式</span>
        </div>
        <button className="icon-button" title="刷新执行状态" onClick={() => void refresh()}>
          <RefreshCw size={17} aria-hidden="true" />
          <span className="sr-only">刷新执行状态</span>
        </button>
      </div>

      {loadState === "LOADING" && <p className="muted-line">正在加载订单状态…</p>}
      {loadState === "EMPTY" && <p className="muted-line">暂无待确认候选订单</p>}
      {loadState === "UNAVAILABLE" && (
        <p className="alert-line" role="status">
          <ShieldAlert size={16} aria-hidden="true" /> 执行审计不可用
        </p>
      )}
      {ticket && loadState === "READY" && (
        <>
          <div className="execution-summary">
            <div><span>状态</span><strong className={`state state-${ticket.order.state.toLowerCase()}`}>{ticket.order.state}</strong></div>
            <div><span>策略 / 开平</span><strong>{ticket.plan.strategy} · {ticket.plan.position_effect}</strong></div>
            <div><span>券商 / 模式</span><strong>{ticket.plan.broker_id.toUpperCase()} · {ticket.plan.execution_mode}</strong></div>
            <div><span>订单 / 保护价</span><strong>{ticket.plan.order_type} · {ticket.plan.limit_price}</strong></div>
            <div><span>已成交</span><strong>{ticket.order.filled_quantity} / {ticket.order.total_quantity}</strong></div>
            <div><span>剩余时间</span><strong className={remaining === 0 ? "danger-text" : ""}><Clock3 size={14} aria-hidden="true" /> {remaining}秒</strong></div>
          </div>

          <div className="legs-table" role="table" aria-label="候选期权组合腿">
            <div className="legs-head" role="row"><span>方向</span><span>合约</span><span>行权价</span><span>数量</span></div>
            {ticket.plan.legs.map((leg) => (
              <div className="legs-row" role="row" key={leg.contract_id}>
                <span className={leg.side === "BUY" ? "buy-text" : "sell-text"}>{leg.side}</span>
                <span>{leg.expiry} {leg.type}</span><span>{leg.strike}</span><span>{leg.quantity}</span>
              </div>
            ))}
          </div>

          <div className="execution-meta">
            <code title={ticket.plan.plan_hash}>计划 {ticket.plan.plan_hash.slice(0, 12)}</code>
            <code>规则 {ticket.plan.rule_version}</code>
            <code>数据源 {ticket.plan.market_data_provider}</code>
            {ticket.order.broker_order_id && <code>券商单号 {ticket.order.broker_order_id}</code>}
          </div>
          {ticket.order.broker_child_orders.length > 0 && (
            <div className="legs-table" role="table" aria-label="券商子订单">
              <div className="legs-head" role="row"><span>子单</span><span>方向</span><span>成交</span><span>状态</span></div>
              {ticket.order.broker_child_orders.map((child) => (
                <div className="legs-row" role="row" key={child.broker_order_id}>
                  <span title={child.broker_order_id}>{child.broker_order_id.slice(0, 12)}</span>
                  <span className={child.side === "BUY" ? "buy-text" : "sell-text"}>{child.side}</span>
                  <span>{child.filled_quantity} / {child.quantity}</span>
                  <span>{child.state}</span>
                </div>
              ))}
            </div>
          )}
          {ticket.order.residual_exposure && (
            <p className="alert-line" role="alert"><ShieldAlert size={16} aria-hidden="true" /> 存在残余敞口 · 已成交 {ticket.order.broker_child_orders.reduce((sum, child) => sum + child.filled_quantity, 0)} 张子单合约 · 必须重新对账</p>
          )}
          {ticket.order.risk_reason_codes.length > 0 && (
            <p className="alert-line"><ShieldAlert size={16} aria-hidden="true" /> {ticket.order.risk_reason_codes.join(" · ")}</p>
          )}
          {error && <p className="alert-line" role="alert"><ShieldAlert size={16} aria-hidden="true" /> {error}</p>}

          <div className="execution-actions">
            <button className="primary-command" disabled={!confirmable} onClick={() => setReviewOpen(true)}>
              <CheckCircle2 size={17} aria-hidden="true" /> 确认执行
            </button>
            <button className="secondary-command" disabled={!cancellable} onClick={() => void cancel()}>
              <XCircle size={17} aria-hidden="true" /> {pending === "cancel" ? "正在撤单" : "取消订单"}
            </button>
          </div>
        </>
      )}

      {reviewOpen && ticket && (
        <div className="modal-scrim" role="presentation">
          <div className="confirm-dialog" role="dialog" aria-modal="true" aria-labelledby="confirm-title">
            <div className="dialog-icon"><ShieldAlert size={20} aria-hidden="true" /></div>
            <h3 id="confirm-title">确认精确执行计划</h3>
            <dl>
              <div><dt>策略</dt><dd>{ticket.plan.strategy}</dd></div>
              <div><dt>开平标记</dt><dd>{ticket.plan.position_effect}</dd></div>
              <div><dt>订单</dt><dd>{ticket.plan.order_side} {ticket.plan.order_type}</dd></div>
              <div><dt>保护价格</dt><dd>{ticket.plan.limit_price}</dd></div>
              <div><dt>最大亏损</dt><dd>{ticket.plan.max_loss}</dd></div>
              <div><dt>执行模式</dt><dd>{ticket.plan.execution_mode}</dd></div>
            </dl>
            <label className="confirm-check">
              <input type="checkbox" checked={acknowledged} onChange={(event) => setAcknowledged(event.target.checked)} />
              <span>我已核对开平标记、合约、保护价格与最大亏损。</span>
            </label>
            <div className="dialog-actions">
              <button className="secondary-command" onClick={() => { setReviewOpen(false); setAcknowledged(false); }}>返回</button>
              <button className="primary-command" disabled={!acknowledged || pending !== null} onClick={() => void confirm()}>
                <CheckCircle2 size={17} aria-hidden="true" /> {pending === "confirm" ? "正在提交" : "提交精确哈希"}
              </button>
            </div>
          </div>
        </div>
      )}
    </section>
  );
}
