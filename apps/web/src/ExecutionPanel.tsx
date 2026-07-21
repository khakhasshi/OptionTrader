import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { CheckCircle2, Clock3, RefreshCw, ShieldAlert, XCircle } from "lucide-react";
import {
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
    ticket?.order.state === "AWAITING_CONFIRMATION" && canTrade && remaining > 0 && pending === null;
  const cancellable =
    ticket !== null &&
    ["AWAITING_CONFIRMATION", "WORKING", "PARTIAL_FILL"].includes(ticket.order.state) &&
    pending === null;

  const updateOrder = (order: ExecutionOrder) => {
    requestGeneration.current += 1;
    const current = ticketAnchor.current;
    if (current && isNewerExecutionOrder(current.order, order)) {
      const next = { ...current, order };
      ticketAnchor.current = next;
      setTicket(next);
    }
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
      if (!response.ok) throw new Error(response.status === 409 ? "RECONCILIATION REQUIRED" : "CONFIRM FAILED");
      const parsed = parseExecutionTicket({ plan: ticket.plan, order: await response.json() });
      if (!parsed) throw new Error("INVALID GATEWAY RESPONSE");
      updateOrder(parsed.order);
      setReviewOpen(false);
      setAcknowledged(false);
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : "CONFIRM FAILED");
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
      if (!response.ok) throw new Error("CANCEL FAILED");
      const parsed = parseExecutionTicket({ plan: ticket.plan, order: await response.json() });
      if (!parsed) throw new Error("INVALID GATEWAY RESPONSE");
      updateOrder(parsed.order);
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : "CANCEL FAILED");
    } finally {
      setPending(null);
    }
  };

  return (
    <section className="execution-band" aria-labelledby="execution-heading">
      <div className="section-heading-row">
        <div>
          <h2 id="execution-heading">Execution</h2>
          <span className="section-kicker">Rust authority · paper/shadow only</span>
        </div>
        <button className="icon-button" title="Refresh execution state" onClick={() => void refresh()}>
          <RefreshCw size={17} aria-hidden="true" />
          <span className="sr-only">Refresh execution state</span>
        </button>
      </div>

      {loadState === "LOADING" && <p className="muted-line">Loading order state…</p>}
      {loadState === "EMPTY" && <p className="muted-line">No staged candidate</p>}
      {loadState === "UNAVAILABLE" && (
        <p className="alert-line" role="status">
          <ShieldAlert size={16} aria-hidden="true" /> Execution audit unavailable
        </p>
      )}
      {ticket && loadState === "READY" && (
        <>
          <div className="execution-summary">
            <div><span>State</span><strong className={`state state-${ticket.order.state.toLowerCase()}`}>{ticket.order.state}</strong></div>
            <div><span>Strategy</span><strong>{ticket.plan.strategy}</strong></div>
            <div><span>Broker / mode</span><strong>{ticket.plan.broker_id.toUpperCase()} · {ticket.plan.execution_mode}</strong></div>
            <div><span>Limit / max loss</span><strong>{ticket.plan.limit_price} / {ticket.plan.max_loss}</strong></div>
            <div><span>Filled</span><strong>{ticket.order.filled_quantity} / {ticket.order.total_quantity}</strong></div>
            <div><span>TTL</span><strong className={remaining === 0 ? "danger-text" : ""}><Clock3 size={14} aria-hidden="true" /> {remaining}s</strong></div>
          </div>

          <div className="legs-table" role="table" aria-label="Candidate option legs">
            <div className="legs-head" role="row"><span>Side</span><span>Contract</span><span>Strike</span><span>Qty</span></div>
            {ticket.plan.legs.map((leg) => (
              <div className="legs-row" role="row" key={leg.contract_id}>
                <span className={leg.side === "BUY" ? "buy-text" : "sell-text"}>{leg.side}</span>
                <span>{leg.expiry} {leg.type}</span><span>{leg.strike}</span><span>{leg.quantity}</span>
              </div>
            ))}
          </div>

          <div className="execution-meta">
            <code title={ticket.plan.plan_hash}>Plan {ticket.plan.plan_hash.slice(0, 12)}</code>
            <code>Rule {ticket.plan.rule_version}</code>
            {ticket.order.broker_order_id && <code>Broker {ticket.order.broker_order_id}</code>}
          </div>
          {ticket.order.risk_reason_codes.length > 0 && (
            <p className="alert-line"><ShieldAlert size={16} aria-hidden="true" /> {ticket.order.risk_reason_codes.join(" · ")}</p>
          )}
          {error && <p className="alert-line" role="alert"><ShieldAlert size={16} aria-hidden="true" /> {error}</p>}

          <div className="execution-actions">
            <button className="primary-command" disabled={!confirmable} onClick={() => setReviewOpen(true)}>
              <CheckCircle2 size={17} aria-hidden="true" /> Confirm
            </button>
            <button className="secondary-command" disabled={!cancellable} onClick={() => void cancel()}>
              <XCircle size={17} aria-hidden="true" /> {pending === "cancel" ? "Cancelling" : "Cancel"}
            </button>
          </div>
        </>
      )}

      {reviewOpen && ticket && (
        <div className="modal-scrim" role="presentation">
          <div className="confirm-dialog" role="dialog" aria-modal="true" aria-labelledby="confirm-title">
            <div className="dialog-icon"><ShieldAlert size={20} aria-hidden="true" /></div>
            <h3 id="confirm-title">Confirm exact plan</h3>
            <dl>
              <div><dt>Strategy</dt><dd>{ticket.plan.strategy}</dd></div>
              <div><dt>Limit</dt><dd>{ticket.plan.limit_price}</dd></div>
              <div><dt>Max loss</dt><dd>{ticket.plan.max_loss}</dd></div>
              <div><dt>Mode</dt><dd>{ticket.plan.execution_mode}</dd></div>
            </dl>
            <label className="confirm-check">
              <input type="checkbox" checked={acknowledged} onChange={(event) => setAcknowledged(event.target.checked)} />
              <span>I verified contracts, limit and maximum loss.</span>
            </label>
            <div className="dialog-actions">
              <button className="secondary-command" onClick={() => { setReviewOpen(false); setAcknowledged(false); }}>Back</button>
              <button className="primary-command" disabled={!acknowledged || pending !== null} onClick={() => void confirm()}>
                <CheckCircle2 size={17} aria-hidden="true" /> {pending === "confirm" ? "Submitting" : "Confirm exact hash"}
              </button>
            </div>
          </div>
        </div>
      )}
    </section>
  );
}
