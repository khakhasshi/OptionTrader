import { useEffect, useRef, useState } from "react";
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

/**
 * Phase 2 real-time Cockpit. Two independent channels feed the fail-closed
 * trading gate:
 *   - WebSocket cockpit stream (data/decision dimension): the Rust-authoritative
 *     snapshot plus Python-derived Regime/Vol/Strategy/Signal, per MarketTick.
 *   - /core/health poll (broker dimension): BrokerHealth + reconciliation +
 *     the Rust gateway's new_position_allowed.
 *
 * A new position is permitted ONLY when the stream frame is LIVE, its snapshot
 * data_health is HEALTHY, the frame's new_position_allowed is true, AND the
 * broker-dimension gate (canOpenNewPosition) passes. Anything else — a degraded
 * broker, unreconciled book, stale data, a disconnected stream, a malformed
 * frame, or a failed health fetch — shows No Trade. Fail closed.
 */
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
        const r = await fetch("/api/v1/core/health");
        if (!r.ok) throw new Error(String(r.status));
        const parsed = parseServiceHealth(await r.json());
        if (!cancelled) {
          setHealth(parsed);
          setReachable(Boolean(parsed && parsed.status === "ok"));
        }
      } catch {
        // Fail closed: drop reachable AND clear the last body so no stale
        // HEALTHY health lingers behind an OFFLINE connection.
        if (!cancelled) {
          setReachable(false);
          setHealth(null);
        }
      }
    };
    poll();
    const id = setInterval(poll, 2000);
    return () => {
      cancelled = true;
      clearInterval(id);
    };
  }, []);

  const online = reachable && health?.status === "ok";
  const brokerAllowed = canOpenNewPosition({ reachable, health });
  const brokerHealth: BrokerHealth = online && health ? health.broker_health : "DISCONNECTED";
  const reconciled = online && health ? health.reconciled === true : false;
  // Data health comes from the stream frame's snapshot; STALE when absent.
  const dataHealth: DataHealth = link === "OPEN" ? frameDataHealth(frame) : "STALE";
  const streamLive = link === "OPEN" && frame?.connection === "LIVE";
  // Final fail-closed gate: the socket must be OPEN now (not CONNECTING/
  // DISCONNECTED) AND the frame+broker gates pass. Without the link check a
  // stale frame could show ALLOWED during a reconnect/malformed-frame window.
  const canTrade = link === "OPEN" && cockpitCanTrade({ frame, brokerAllowed });

  const snapshot = frame?.snapshot ?? null;

  return (
    <main style={{ fontFamily: "system-ui", padding: 24 }}>
      <h1>OptionTrader Cockpit</h1>
      <p style={{ color: "#5d6677" }}>Phase 2 — QQQ intraday volatility, real-time</p>

      <section style={{ display: "flex", gap: 16, marginTop: 16, flexWrap: "wrap" }}>
        <Badge label="Connection" value={online ? "ONLINE" : "OFFLINE (read-only)"} ok={Boolean(online)} />
        <Badge
          label="Stream"
          value={link === "OPEN" ? (streamLive ? "LIVE" : "OPEN (not live)") : link}
          ok={Boolean(streamLive)}
        />
        <Badge label="Data Health" value={dataHealth} ok={dataHealth === "HEALTHY"} />
        <Badge label="Broker Health" value={brokerHealth} ok={brokerHealth === "HEALTHY"} />
        <Badge
          label="Reconciliation"
          value={reconciled ? "RECONCILED" : "NOT RECONCILED"}
          ok={reconciled}
        />
        <Badge label="Trading" value={canTrade ? "ALLOWED" : "No Trade"} ok={canTrade} />
      </section>

      <h2 style={{ marginTop: 28, fontSize: 18 }}>Decision</h2>
      <section style={{ display: "flex", gap: 16, marginTop: 12, flexWrap: "wrap" }}>
        <Field label="Regime" value={frame?.regime?.regime ?? "—"} />
        <Field label="Strategy" value={frame?.signal?.strategy ?? "—"} />
        <Field label="Vol State" value={frame?.vol?.iv_hv_state ?? "—"} />
        <Field label="Vol Read" value={frame?.vol?.interpretation ?? "—"} />
        <Field
          label="Initial Risk"
          value={frame?.signal?.initial_risk_status ?? "NOT_EVALUATED"}
        />
      </section>

      <h2 style={{ marginTop: 28, fontSize: 18 }}>Market Snapshot</h2>
      {streamLive && snapshot ? (
        <section
          role="group"
          aria-label="Market Snapshot"
          style={{ display: "flex", gap: 16, marginTop: 12, flexWrap: "wrap" }}
        >
          <Field label="Snapshot ID" value={snapshot.snapshot_id} />
          <Field label="Symbol" value={snapshot.symbol} />
          <Field label="Price" value={snapshot.price} />
          <Field label="VWAP" value={snapshot.vwap} />
          <Field label="Snapshot Data Health" value={snapshot.data_health} />
        </section>
      ) : (
        <p
          role="status"
          aria-label="Market Snapshot: unavailable"
          style={{ color: "#b23", marginTop: 12 }}
        >
          Snapshot STALE / unavailable
        </p>
      )}

      <h2 style={{ marginTop: 28, fontSize: 18 }}>Risk Flags</h2>
      {frame && frame.risk_flags.length > 0 ? (
        <ul role="list" aria-label="Risk Flags" style={{ marginTop: 8, color: "#8a5a00" }}>
          {frame.risk_flags.map((flag, i) => (
            <li key={i}>{flag}</li>
          ))}
        </ul>
      ) : (
        <p role="status" aria-label="Risk Flags: none" style={{ color: "#5d6677", marginTop: 8 }}>
          None
        </p>
      )}

      <h2 style={{ marginTop: 28, fontSize: 18 }}>Signal Log</h2>
      <SignalLog entries={signalLog} />

      <h2 style={{ marginTop: 28, fontSize: 18 }}>System Health</h2>
      <section style={{ display: "flex", gap: 16, marginTop: 12, flexWrap: "wrap" }}>
        <Field label="Link" value={link} />
        <Field label="Reconnects" value={String(reconnects)} />
        <Field label="Frame Seq" value={frame ? String(frame.seq) : "—"} />
        <Field label="Rule Version" value={frame?.signal?.rule_version ?? "—"} />
      </section>
    </main>
  );
}

interface SignalLogEntry {
  seq: number;
  time: string;
  strategy: string;
  regime: string;
  risk: string;
}

/** Accumulate the most recent distinct signals for the operator's audit trail. */
function useSignalLog(frame: CockpitState | null): SignalLogEntry[] {
  const [log, setLog] = useState<SignalLogEntry[]>([]);
  const lastSeq = useRef<number>(-1);
  useEffect(() => {
    if (!frame || !frame.signal || frame.seq === lastSeq.current) return;
    lastSeq.current = frame.seq;
    const signal: SignalView = frame.signal;
    setLog((prev) =>
      [
        {
          seq: frame.seq,
          time: frame.server_time_utc,
          strategy: signal.strategy,
          regime: signal.regime,
          risk: signal.initial_risk_status,
        },
        ...prev,
      ].slice(0, MAX_SIGNAL_LOG),
    );
  }, [frame]);
  return log;
}

function SignalLog({ entries }: { entries: SignalLogEntry[] }) {
  if (entries.length === 0) {
    return (
      <p role="status" aria-label="Signal Log: empty" style={{ color: "#5d6677", marginTop: 8 }}>
        No signals yet
      </p>
    );
  }
  return (
    <ul role="list" aria-label="Signal Log" style={{ marginTop: 8, listStyle: "none", padding: 0 }}>
      {entries.map((e) => (
        <li
          key={e.seq}
          style={{ borderBottom: "1px solid #eef0f4", padding: "6px 0", fontVariantNumeric: "tabular-nums" }}
        >
          <span style={{ color: "#5d6677" }}>#{e.seq}</span> {e.strategy} · {e.regime} ·{" "}
          <span style={{ color: e.risk === "PASSED" ? "#2a7" : "#b23" }}>{e.risk}</span>
        </li>
      ))}
    </ul>
  );
}

function Badge({ label, value, ok }: { label: string; value: string; ok: boolean }) {
  return (
    <div
      role="status"
      aria-label={`${label}: ${value}`}
      style={{
        border: "1px solid #d9dee8",
        borderRadius: 8,
        padding: "10px 14px",
        background: ok ? "#eef7ef" : "#fff1ef",
        minWidth: 160,
      }}
    >
      <div style={{ fontSize: 12, color: "#5d6677" }}>{label}</div>
      <strong>{value}</strong>
    </div>
  );
}

function Field({ label, value }: { label: string; value: string }) {
  return (
    <div
      aria-label={`${label}: ${value}`}
      style={{
        border: "1px solid #d9dee8",
        borderRadius: 8,
        padding: "10px 14px",
        minWidth: 160,
      }}
    >
      <div style={{ fontSize: 12, color: "#5d6677" }}>{label}</div>
      <strong>{value}</strong>
    </div>
  );
}
