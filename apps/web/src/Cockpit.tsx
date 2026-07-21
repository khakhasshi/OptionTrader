import { useEffect, useRef, useState, type ReactNode } from "react";
import {
  Activity,
  AlertTriangle,
  CalendarClock,
  Database,
  Gauge,
  Radio,
  Server,
  ShieldCheck,
  ShieldX,
  Waypoints,
  Waves,
} from "lucide-react";
import {
  canOpenNewPosition,
  parseServiceHealth,
  type BrokerHealth,
  type DataHealth,
  type ServiceHealth,
} from "./health";
import {
  cockpitCanTrade,
  frameDataHealth,
  type CockpitState,
  type SignalView,
} from "./cockpitState";
import { useCockpitStream } from "./useCockpitStream";
import { ExecutionPanel } from "./ExecutionPanel";

const SESSION_ID = "live";
const MAX_SIGNAL_LOG = 12;

export function Cockpit() {
  const [health, setHealth] = useState<ServiceHealth | null>(null);
  const [reachable, setReachable] = useState(false);
  const { frame, link, reconnects } = useCockpitStream(SESSION_ID);
  const signalLog = useSignalLog(frame);

  useEffect(() => {
    let cancelled = false;
    const poll = async () => {
      try {
        const response = await fetch("/api/v1/core/health");
        if (!response.ok) throw new Error(String(response.status));
        const parsed = parseServiceHealth(await response.json());
        if (!cancelled) {
          setHealth(parsed);
          setReachable(Boolean(parsed && parsed.status === "ok"));
        }
      } catch {
        if (!cancelled) {
          setReachable(false);
          setHealth(null);
        }
      }
    };
    void poll();
    const id = window.setInterval(() => void poll(), 2_000);
    return () => {
      cancelled = true;
      window.clearInterval(id);
    };
  }, []);

  const online = reachable && health?.status === "ok";
  const brokerAllowed = canOpenNewPosition({ reachable, health });
  const brokerHealth: BrokerHealth = online && health ? health.broker_health : "DISCONNECTED";
  const reconciled = online && health ? health.reconciled === true : false;
  const dataHealth: DataHealth = link === "OPEN" ? frameDataHealth(frame) : "STALE";
  const streamLive = link === "OPEN" && frame?.connection === "LIVE";
  const canTrade = link === "OPEN" && cockpitCanTrade({ frame, brokerAllowed });
  const snapshot = frame?.snapshot ?? null;
  const blockers = tradingBlockers({ online, streamLive, dataHealth, frame, brokerHealth, reconciled });

  return (
    <main className="page cockpit-page">
      <header className="page-header cockpit-header">
        <div>
          <span className="page-eyebrow"><Activity size={14} /> QQQ.US · 日内波动率</span>
          <h1>交易驾驶舱</h1>
          <p>集中查看市场环境、决策权限与订单执行。</p>
        </div>
        <div className="session-meta">
          <span><i className={streamLive ? "live-dot" : "offline-dot"} /> 交易时段 {SESSION_ID}</span>
          <strong>{formatTimestamp(frame?.server_time_utc)}</strong>
        </div>
      </header>

      <section
        className={`authority-banner ${canTrade ? "authority-open" : "authority-closed"}`}
        role="status"
        aria-label={`Trading: ${canTrade ? "ALLOWED" : "No Trade"}`}
      >
        <span className="authority-icon">
          {canTrade ? <ShieldCheck size={24} aria-hidden="true" /> : <ShieldX size={24} aria-hidden="true" />}
        </span>
        <div className="authority-copy">
          <span>RUST 执行权限</span>
          <strong>{canTrade ? "允许开立新仓" : "已强制禁止交易"}</strong>
          <p>{canTrade ? "数据、事件与券商独立闸门全部一致。" : blockers[0] ?? "权威风控已拒绝开立新仓。"}</p>
        </div>
        <div className="authority-mode">
          <span>运行环境</span>
          <strong>{health?.environment?.toUpperCase() ?? "只读模式"}</strong>
        </div>
      </section>

      <section className="status-matrix" aria-label="系统闸门状态">
        <StatusTile icon={<Server size={17} />} label="Connection" displayLabel="核心连接" value={online ? "ONLINE" : "OFFLINE (read-only)"} displayValue={online ? "在线" : "离线（只读）"} ok={Boolean(online)} />
        <StatusTile icon={<Radio size={17} />} label="Stream" displayLabel="行情流" value={link === "OPEN" ? (streamLive ? "LIVE" : "OPEN (not live)") : link} displayValue={streamLive ? "实时" : localizeLink(link)} ok={Boolean(streamLive)} />
        <StatusTile icon={<Database size={17} />} label="Data Health" displayLabel="数据健康" value={dataHealth} displayValue={localizeHealth(dataHealth)} ok={dataHealth === "HEALTHY"} />
        <StatusTile icon={<CalendarClock size={17} />} label="Event Context" displayLabel="事件上下文" value={frame?.event_context?.available ? frame.event_context.event_day_type : "UNAVAILABLE"} displayValue={frame?.event_context?.available ? localizeEventDay(frame.event_context.event_day_type) : "不可用"} ok={frame?.event_context?.available === true} />
        <StatusTile icon={<Waypoints size={17} />} label="Broker Health" displayLabel="券商健康" value={brokerHealth} displayValue={localizeHealth(brokerHealth)} ok={brokerHealth === "HEALTHY"} />
        <StatusTile icon={<ShieldCheck size={17} />} label="Reconciliation" displayLabel="持仓对账" value={reconciled ? "RECONCILED" : "NOT RECONCILED"} displayValue={reconciled ? "已完成" : "未完成"} ok={reconciled} />
      </section>

      <div className="cockpit-primary-grid">
        <Panel icon={<Waves size={18} />} title="市场快照" kicker="ThetaData 权威行情" className="market-panel">
          {streamLive && snapshot ? (
            <div role="group" aria-label="Market Snapshot">
              <div className="instrument-row">
                <div><span>{snapshot.symbol}</span><strong className="market-price" aria-label={`Price: ${snapshot.price}`}>{snapshot.price}</strong></div>
                <span className={`health-chip ${snapshot.data_health === "HEALTHY" ? "ok" : "bad"}`} aria-label={`Snapshot Data Health: ${snapshot.data_health}`}>
                  {localizeHealth(snapshot.data_health)} · {snapshot.data_health}
                </span>
              </div>
              <div className="metric-grid metric-grid-market">
                <Metric label="Open" displayLabel="开盘价" value={snapshot.open} />
                <Metric label="VWAP" displayLabel="成交量加权均价" value={snapshot.vwap} />
                <Metric label="Sequence" displayLabel="序列号" value={String(snapshot.sequence_number)} />
              </div>
              <div className="snapshot-foot"><code>{snapshot.snapshot_id}</code><span>{formatTimestamp(snapshot.occurred_at_utc)}</span></div>
            </div>
          ) : (
            <div className="empty-state danger" role="status" aria-label="Market Snapshot: unavailable">
              <AlertTriangle size={21} aria-hidden="true" /><div><strong>市场快照不可用</strong><span>在收到可信实时帧之前，市场状态保持为陈旧。</span></div>
            </div>
          )}
        </Panel>

        <Panel icon={<Gauge size={18} />} title="交易决策" kicker="确定性策略引擎">
          <div className="decision-lead">
            <Metric label="Regime" displayLabel="市场状态" value={frame?.regime?.regime ?? "—"} displayValue={localizeRegime(frame?.regime?.regime)} emphasis />
            <Metric label="Strategy" displayLabel="策略" value={frame?.signal?.strategy ?? "—"} displayValue={localizeStrategy(frame?.signal?.strategy)} emphasis />
          </div>
          <div className="metric-grid">
            <Metric label="Vol State" displayLabel="波动率状态" value={frame?.vol?.iv_hv_state ?? "—"} />
            <Metric label="Vol Read" displayLabel="波动率判断" value={frame?.vol?.interpretation ?? "—"} />
            <Metric label="Initial Risk" displayLabel="初始风控" value={frame?.signal?.initial_risk_status ?? "NOT_EVALUATED"} displayValue={localizeRisk(frame?.signal?.initial_risk_status)} />
            <Metric label="Realized Move" displayLabel="实际波幅" value={frame?.vol ? formatPercent(frame.vol.realized_move) : "—"} />
          </div>
          <div className="decision-reason">
            <span>决策依据</span>
            <p>{frame?.signal?.reason[0] ?? "等待通过校验的信号帧。"}</p>
          </div>
        </Panel>

        <Panel icon={<CalendarClock size={18} />} title="事件上下文" kicker="宏观、财报与新闻风险">
          <div className="metric-grid">
            <Metric label="Event Day" displayLabel="事件日类型" value={frame?.event_context?.event_day_type ?? "—"} displayValue={frame?.event_context ? localizeEventDay(frame.event_context.event_day_type) : "—"} />
            <Metric label="Next Major Event" displayLabel="距重大事件" value={frame?.event_context?.minutes_to_major_event == null ? "UNKNOWN" : `${frame.event_context.minutes_to_major_event} min`} displayValue={frame?.event_context?.minutes_to_major_event == null ? "未知" : `${frame.event_context.minutes_to_major_event} 分钟`} />
            <Metric label="Weighted Risk" displayLabel="加权风险" value={frame?.event_context?.qqq_weighted_event_score ?? "—"} />
            <Metric label="Documents" displayLabel="来源文档" value={String(frame?.event_context?.source_documents.length ?? 0)} />
          </div>
          <div className="source-strip" aria-label="事件来源覆盖">
            {(["macro", "holdings", "earnings", "news"] as const).map((source) => {
              const available = frame?.event_context?.source_documents.some((document) => document.category === source) ?? false;
              return <span className={available ? "available" : "missing"} key={source}><i />{localizeSource(source)}</span>;
            })}
          </div>
        </Panel>

        <Panel icon={<ShieldCheck size={18} />} title="风险标记" kicker={`${frame?.risk_flags.length ?? 0} 项生效条件`}>
          {frame && frame.risk_flags.length > 0 ? (
            <ul className="risk-list" role="list" aria-label="Risk Flags">
              {frame.risk_flags.map((flag) => <li key={flag}><AlertTriangle size={15} aria-hidden="true" /><span>{flag}</span></li>)}
            </ul>
          ) : (
            <div className="empty-state" role="status" aria-label="Risk Flags: none">
              <ShieldCheck size={21} aria-hidden="true" /><div><strong>当前无帧级风险标记</strong><span>券商与数据闸门仍会独立执行。</span></div>
            </div>
          )}
        </Panel>
      </div>

      <ExecutionPanel sessionId={SESSION_ID} canTrade={canTrade} />

      <div className="cockpit-secondary-grid">
        <Panel icon={<Activity size={18} />} title="信号日志" kicker="最近的确定性决策记录">
          <SignalLog entries={signalLog} />
        </Panel>
        <Panel icon={<Server size={18} />} title="系统健康" kicker="传输链路与规则版本审计">
          <div className="metric-grid system-metrics">
            <Metric label="Link" displayLabel="链路" value={link} displayValue={localizeLink(link)} />
            <Metric label="Reconnects" displayLabel="重连次数" value={String(reconnects)} />
            <Metric label="Frame Seq" displayLabel="帧序列" value={frame ? String(frame.seq) : "—"} />
            <Metric label="Rule Version" displayLabel="规则版本" value={frame?.signal?.rule_version ?? "—"} />
          </div>
          <div className="transport-path" aria-label="Transport path">
            <span>ThetaData</span><i /><span>Rust 核心</span><i /><span>Python 服务</span><i /><span>WebSocket</span>
          </div>
        </Panel>
      </div>
    </main>
  );
}

function tradingBlockers({ online, streamLive, dataHealth, frame, brokerHealth, reconciled }: { online: boolean; streamLive: boolean; dataHealth: DataHealth; frame: CockpitState | null; brokerHealth: BrokerHealth; reconciled: boolean }): string[] {
  const blockers: string[] = [];
  if (!online) blockers.push("交易核心离线，或健康契约未通过校验。");
  if (!streamLive) blockers.push("行情流尚未进入实时状态。");
  if (dataHealth !== "HEALTHY") blockers.push(`市场数据状态：${localizeHealth(dataHealth)}。`);
  if (frame?.event_context?.available !== true) blockers.push("事件上下文不可用。");
  if (brokerHealth !== "HEALTHY") blockers.push(`券商连接状态：${localizeHealth(brokerHealth)}。`);
  if (!reconciled) blockers.push("券商持仓尚未完成对账。");
  if (frame?.new_position_allowed !== true) blockers.push("上游数据权限已拒绝开立新仓。");
  return blockers;
}

interface SignalLogEntry { seq: number; time: string; strategy: string; regime: string; risk: string; }

function useSignalLog(frame: CockpitState | null): SignalLogEntry[] {
  const [log, setLog] = useState<SignalLogEntry[]>([]);
  const lastSeq = useRef(-1);
  useEffect(() => {
    if (!frame || !frame.signal || frame.seq === lastSeq.current) return;
    lastSeq.current = frame.seq;
    const signal: SignalView = frame.signal;
    setLog((current) => [{ seq: frame.seq, time: frame.server_time_utc, strategy: signal.strategy, regime: signal.regime, risk: signal.initial_risk_status }, ...current].slice(0, MAX_SIGNAL_LOG));
  }, [frame]);
  return log;
}

function SignalLog({ entries }: { entries: SignalLogEntry[] }) {
  if (entries.length === 0) return <div className="empty-state" role="status" aria-label="Signal Log: empty"><Radio size={21} /><div><strong>暂无信号</strong><span>首个通过校验的信号将写入审计记录。</span></div></div>;
  return (
    <ul className="signal-list" role="list" aria-label="Signal Log">
      {entries.map((entry) => (
        <li key={entry.seq}>
          <span className="signal-seq">#{entry.seq}</span>
          <div><strong>{localizeStrategy(entry.strategy)}</strong><span>{localizeRegime(entry.regime)} · {formatTimestamp(entry.time)}</span></div>
          <span className={`risk-state ${entry.risk === "PASSED" ? "passed" : "blocked"}`}>{localizeRisk(entry.risk)}</span>
        </li>
      ))}
    </ul>
  );
}

function Panel({ icon, title, kicker, className = "", children }: { icon: ReactNode; title: string; kicker: string; className?: string; children: ReactNode }) {
  return <section className={`cockpit-panel ${className}`}><header className="panel-header"><span className="panel-icon">{icon}</span><div><h2>{title}</h2><p>{kicker}</p></div></header><div className="panel-body">{children}</div></section>;
}

function StatusTile({ icon, label, displayLabel, value, displayValue, ok }: { icon: ReactNode; label: string; displayLabel: string; value: string; displayValue: string; ok: boolean }) {
  return <div className={`status-tile ${ok ? "status-ok" : "status-bad"}`} role="status" aria-label={`${label}: ${value}`}><span className="status-icon">{icon}</span><div><span>{displayLabel}</span><strong>{displayValue}</strong></div><i className="status-indicator" /></div>;
}

function Metric({ label, displayLabel, value, displayValue, emphasis = false }: { label: string; displayLabel?: string; value: string; displayValue?: string; emphasis?: boolean }) {
  return <div className={`metric ${emphasis ? "metric-emphasis" : ""}`} aria-label={`${label}: ${value}`}><span>{displayLabel ?? label}</span><strong>{displayValue ?? value}</strong></div>;
}

function formatTimestamp(value: string | undefined): string {
  if (!value) return "等待数据帧";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return value;
  return new Intl.DateTimeFormat("zh-CN", { hour: "2-digit", minute: "2-digit", second: "2-digit", hour12: false, timeZone: "America/New_York" }).format(date) + " 美东";
}

function formatPercent(value: number): string {
  return `${(value * 100).toFixed(2)}%`;
}

function localizeHealth(value: string): string {
  return ({ HEALTHY: "健康", DEGRADED: "降级", STALE: "陈旧", DISCONNECTED: "已断开", RECONCILING: "对账中" } as Record<string, string>)[value] ?? value;
}

function localizeLink(value: string): string {
  return ({ OPEN: "已连接", CONNECTING: "连接中", DISCONNECTED: "已断开" } as Record<string, string>)[value] ?? value;
}

function localizeRegime(value: string | undefined): string {
  if (!value) return "—";
  const label = ({ Trend: "趋势", Range: "震荡", Event: "事件", Chaos: "混沌", NoTrade: "禁止交易" } as Record<string, string>)[value];
  return label ? `${label} · ${value}` : value;
}

function localizeStrategy(value: string | undefined): string {
  if (!value) return "—";
  const label = ({ LongGamma: "做多伽马", ShortPremium: "卖出波动率", EventVolCrush: "事件波动率回落", NoTrade: "禁止交易" } as Record<string, string>)[value];
  return label ? `${label} · ${value}` : value;
}

function localizeRisk(value: string | undefined): string {
  if (!value) return "未评估";
  return ({ PASSED: "通过", BLOCKED: "拦截", REJECTED: "拒绝", NOT_EVALUATED: "未评估" } as Record<string, string>)[value] ?? value;
}

function localizeEventDay(value: string): string {
  return ({ Normal: "普通交易日", MacroEvent: "宏观事件日", EarningsEvent: "财报事件日", FOMC: "美联储议息日", Mixed: "混合事件日", HighRisk: "高风险事件日" } as Record<string, string>)[value] ?? value;
}

function localizeSource(value: string): string {
  return ({ macro: "宏观", holdings: "成分股", earnings: "财报", news: "新闻" } as Record<string, string>)[value] ?? value;
}
